// ============================================================
//  ESP32 - BẢO TRÌ MÁY MÓC - FREERTOS
// ============================================================

#include <Arduino.h>
#include <Wire.h>
#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <LiquidCrystal_I2C.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include <Preferences.h>   

// ============================================================
//  THÔNG SỐ MẠNG
// ============================================================
const char* ssid        = "Nae";
const char* password    = "01112003";
const char* mqtt_server = "10.29.199.42";
const int   mqtt_port   = 1883;
const char* device_id   = "esp32-01";

// ============================================================
//  TOPICS MQTT
// ============================================================
#define TOPIC_TELEMETRY     "devices/esp32-01/telemetry"
#define TOPIC_WIFI_STATUS   "devices/esp32-01/wifi_status"
#define TOPIC_DEVICE_STATUS "devices/esp32-01/device_status"
#define TOPIC_CTRL_BUZZER   "devices/esp32-01/control/buzzer"
#define TOPIC_CTRL_RELAY    "devices/esp32-01/control/relay"
#define TOPIC_CONFIG_WIFI   "devices/esp32-01/config/wifi"

// ============================================================
//  CHÂN PHẦN CỨNG
// ============================================================
#define LCD_SDA         21
#define LCD_SCL         22
#define ADXL_SDA        19
#define ADXL_SCL        18
#define ADXL_ADDR       0x53
#define ONE_WIRE_PIN    4
#define ACS712_PIN      34
#define BUZZER_PIN      25
#define RELAY_PIN       23

// ============================================================
//  LED
// ============================================================
#define PIN_LED_NORMAL  14
#define PIN_LED_FAULT   27
#define PIN_LED_STATUS  2

// ============================================================
//  NGƯỠNG CẢNH BÁO
// ============================================================
#define TEMP_WARN       50.0f
#define TEMP_DANGER     70.0f
#define CURRENT_WARN     0.8f   // [FIX] cảnh báo sớm trước khi tới DANGER
#define CURRENT_DANGER   1.0f   // [FIX] khớp với ngưỡng 1A đặt trong pgAdmin
#define VIB_WARN     2.5f    // [FIX] thống nhất đơn vị g
#define VIB_DANGER   5.0f    // [FIX] thống nhất đơn vị g

// ============================================================
//  ACS712-05B - THÔNG SỐ DYNAMIC CALIBRATION
// ============================================================
#define ADC_VREF            3.3f
#define ADC_MAX             4095.0f
#define ACS_SENS_ADC        0.1310f
#define ACS_VZERO_THEORY    0.8333f
#define ACS_NO_LOAD_THR     0.030f
#define ACS_NO_LOAD_COUNT   5
#define ACS_EMA_ALPHA       0.05f
#define ACS_N_SAMPLES       1000

// ============================================================
//  WATCHDOG
// ============================================================
#define WDG_TIMEOUT_MS 15000

// ============================================================
//  [FIX 7] NVS KEY CHO V_ZERO
// ============================================================
#define NVS_NAMESPACE   "acs712"
#define NVS_KEY_VZERO   "vzero"

// ============================================================
//  BIẾN TOÀN CỤC ACS712 DYNAMIC CAL
// ============================================================
static float  g_acsVZero       = ACS_VZERO_THEORY;
static int    g_acsNoLoadCount = 0;
static bool   g_acsRecalFlag   = false;

Preferences   g_prefs;   // NVS

// ============================================================
//  ĐỊNH NGHĨA TRẠNG THÁI HỆ THỐNG
// ============================================================
typedef enum {
    STATE_NORMAL  = 0,
    STATE_WARNING = 1,
    STATE_DANGER  = 2
} SystemState_t;

// ============================================================
//  DỮ LIỆU DÙNG CHUNG
// ============================================================
struct SensorData_t {
    float temp_c;
    float current_a;
    float accel_x, accel_y, accel_z;
    float vibration_rms;
    SystemState_t state;
};

static SensorData_t      g_data  = {};
static SemaphoreHandle_t g_mutex;       // bảo vệ g_data

// [FIX 2] mutex riêng cho các cờ điều khiển từ MQTT callback
// (callback chạy từ task_mqtt, đọc từ task_buzzer_led/relay)
static SemaphoreHandle_t g_ctrlMutex;
static bool g_buzzerCmd = false;
static bool g_relayCmd  = true;  // [FIX] mặc định true = quạt chạy ngay khi boot, chưa cần lệnh dashboard

// Helper: đọc cờ điều khiển an toàn
static bool getCtrl(bool &buzzer, bool &relay) {
    if (xSemaphoreTake(g_ctrlMutex, pdMS_TO_TICKS(10)) == pdTRUE) {
        buzzer = g_buzzerCmd;
        relay  = g_relayCmd;
        xSemaphoreGive(g_ctrlMutex);
        return true;
    }
    return false;
}

static void setCtrl(bool *buzzer, bool *relay) {
    if (xSemaphoreTake(g_ctrlMutex, pdMS_TO_TICKS(10)) == pdTRUE) {
        if (buzzer) g_buzzerCmd = *buzzer;
        if (relay)  g_relayCmd  = *relay;
        xSemaphoreGive(g_ctrlMutex);
    }
}

// Watchdog timestamps
static volatile uint32_t wdg_temp = 0, wdg_adxl = 0, wdg_acs = 0, wdg_mqtt = 0;

// ============================================================
//  ĐỐI TƯỢNG NGOẠI VI
// ============================================================
LiquidCrystal_I2C lcd(0x27, 16, 2);
OneWire           oneWire(ONE_WIRE_PIN);
DallasTemperature ds18b20(&oneWire);
WiFiClient        espClient;
PubSubClient      mqtt(espClient);

// ============================================================
//  ADXL345 - ĐỌC THỦ CÔNG QUA Wire1
// ============================================================
static void adxl_write(uint8_t reg, uint8_t val) {
    Wire1.beginTransmission(ADXL_ADDR);
    Wire1.write(reg); Wire1.write(val);
    Wire1.endTransmission();
}

static bool adxl_init() {
    Wire1.beginTransmission(ADXL_ADDR);
    Wire1.write(0x00);
    Wire1.endTransmission(false);
    Wire1.requestFrom(ADXL_ADDR, (uint8_t)1);
    uint8_t id = Wire1.read();
    Serial.printf("[ADXL] DEVID=0x%02X %s\n", id, id == 0xE5 ? "OK" : "FAIL");
    if (id != 0xE5) return false;
    adxl_write(0x2D, 0x00); // Standby
    adxl_write(0x2D, 0x08); // Measure
    adxl_write(0x31, 0x00); // ±2G full res
    return true;
}

static void adxl_read(float &ax, float &ay, float &az) {
    Wire1.beginTransmission(ADXL_ADDR);
    Wire1.write(0x32);
    Wire1.endTransmission(false);
    Wire1.requestFrom(ADXL_ADDR, (uint8_t)6);
    int16_t rx = Wire1.read() | (Wire1.read() << 8);
    int16_t ry = Wire1.read() | (Wire1.read() << 8);
    int16_t rz = Wire1.read() | (Wire1.read() << 8);
    const float sc = 0.00390625f;  // [FIX] LSB -> g (bỏ nhân 9.80665, thống nhất đơn vị g)
    ax = rx * sc; ay = ry * sc; az = rz * sc;
}

// ============================================================
//  ACS712-05B - HÀM ĐỌC ĐIỆN ÁP TRUNG BÌNH (V)
// ============================================================
static float acs712_readVoltageAvg() {
    long sum = 0;
    for (int i = 0; i < ACS_N_SAMPLES; i++) {
        sum += analogRead(ACS712_PIN);
        delayMicroseconds(200);
    }
    return (sum / (float)ACS_N_SAMPLES) * (ADC_VREF / ADC_MAX);
}

// ============================================================
//  [FIX 7] LƯU V_ZERO VÀO NVS
// ============================================================
static void nvs_saveVzero(float v) {
    g_prefs.begin(NVS_NAMESPACE, false);
    g_prefs.putFloat(NVS_KEY_VZERO, v);
    g_prefs.end();
    Serial.printf("[NVS] V_zero saved: %.4f V\n", v);
}

static float nvs_loadVzero() {
    g_prefs.begin(NVS_NAMESPACE, true);
    float v = g_prefs.getFloat(NVS_KEY_VZERO, ACS_VZERO_THEORY);
    g_prefs.end();
    return v;
}

// ============================================================
//  ACS712-05B - CALIBRATE LẦN ĐẦU
//  [FIX NVS] Luôn calibrate từ phần cứng khi khởi động.
//  Xóa NVS cũ trước để tránh dùng V_zero sai từ lần trước.
//  YÊU CẦU: RÚT HẾT TẢI TRƯỚC KHI CẤP NGUỒN / NẠP CODE!
// ============================================================
static void acs712_initialCalibrate() {
    // Xóa NVS cũ để tránh khôi phục giá trị sai
    g_prefs.begin(NVS_NAMESPACE, false);
    g_prefs.clear();
    g_prefs.end();
    Serial.println("[ACS712] NVS da xoa, calibrate lai tu phan cung...");

    Serial.println("[ACS712] Dang calibrate V_zero... (RUT TAI RA TRUOC!)");
    long sum = 0;
    const int n = 5000;
    for (int i = 0; i < n; i++) {
        sum += analogRead(ACS712_PIN);
        delayMicroseconds(200);
    }
    g_acsVZero = (sum / (float)n) * (ADC_VREF / ADC_MAX);
    nvs_saveVzero(g_acsVZero);

    Serial.printf("[ACS712] V_zero khoi dong = %.4f V\n", g_acsVZero);
    Serial.printf("[ACS712] V_zero ly thuyet = %.4f V\n", ACS_VZERO_THEORY);
    Serial.printf("[ACS712] Offset           = %+.4f V\n\n", g_acsVZero - ACS_VZERO_THEORY);
}

// ============================================================
//  ACS712-05B - CẬP NHẬT V_ZERO BẰNG EMA KHI KHÔNG TẢI
// ============================================================
static void acs712_updateVzeroEMA(float v_adc) {
    float vZeroOld = g_acsVZero;
    g_acsVZero = ACS_EMA_ALPHA * v_adc + (1.0f - ACS_EMA_ALPHA) * g_acsVZero;
    Serial.printf("[ACS712][RECAL] V_zero: %.4f -> %.4f V (drift=%+.4f V)\n",
        vZeroOld, g_acsVZero, g_acsVZero - vZeroOld);
    nvs_saveVzero(g_acsVZero);   // [FIX 7] lưu sau mỗi lần EMA update
}

// ============================================================
//  ACS712-05B - ĐỌC DÒNG ĐIỆN (A) VỚI DYNAMIC V_ZERO
// ============================================================
static float acs712_readCurrent() {
    float v_adc   = acs712_readVoltageAvg();
    float delta   = v_adc - g_acsVZero;
    float current = delta / ACS_SENS_ADC;

    if (fabsf(current) < 0.020f) current = 0.0f;

    if (fabsf(current) < ACS_NO_LOAD_THR) {
        g_acsNoLoadCount++;
        if (g_acsNoLoadCount >= ACS_NO_LOAD_COUNT) {
            acs712_updateVzeroEMA(v_adc);
            g_acsNoLoadCount = 0;
            g_acsRecalFlag   = true;
        }
    } else {
        g_acsNoLoadCount = 0;
        g_acsRecalFlag   = false;
    }

    return fabsf(current);
}

// ============================================================
//  ĐÁNH GIÁ TRẠNG THÁI
// ============================================================
static SystemState_t eval_state(float t, float i, float rms) {
    if (t >= TEMP_DANGER || i >= CURRENT_DANGER || rms >= VIB_DANGER)
        return STATE_DANGER;
    if (t >= TEMP_WARN || i >= CURRENT_WARN || rms >= VIB_WARN)
        return STATE_WARNING;
    return STATE_NORMAL;
}

// Helper: chuyển state sang chuỗi
static const char* stateStr(SystemState_t s) {
    return s == STATE_NORMAL ? "NORMAL" : s == STATE_WARNING ? "WARNING" : "DANGER";
}

// ============================================================
//  MQTT CALLBACK - Nhận lệnh từ Dashboard/Server
// ============================================================
static void mqtt_callback(char* topic, byte* payload, unsigned int len) {
    char msg[64] = {};
    if (len < sizeof(msg)) memcpy(msg, payload, len);
    String t = String(topic);
    Serial.printf("[MQTT IN] %s -> %s\n", topic, msg);

    if (t == TOPIC_CTRL_BUZZER) {
        bool val = (strcmp(msg, "1") == 0 || strcmp(msg, "ON") == 0);
        setCtrl(&val, nullptr);  // [FIX 2] dùng mutex
        digitalWrite(PIN_LED_STATUS, val ? HIGH : LOW);
        Serial.printf("[LED IO2] %s\n", val ? "ON - Nhan lenh buzzer" : "OFF - Dashboard tat");
    }
    else if (t == TOPIC_CTRL_RELAY) {
        bool val = (strcmp(msg, "1") == 0 || strcmp(msg, "ON") == 0);
        setCtrl(nullptr, &val);  // [FIX 2]
    }
    else if (t == TOPIC_CONFIG_WIFI) {
        StaticJsonDocument<256> doc;
        if (!deserializeJson(doc, msg)) {
            const char* new_ssid = doc["ssid"];
            const char* new_pass = doc["password"];
            if (new_ssid && new_pass) {
                Serial.printf("[CONFIG] New WiFi SSID: %s\n", new_ssid);
                WiFi.begin(new_ssid, new_pass);
            }
        }
    }
}

// ============================================================
//  TASK 1: DS18B20 - ĐỌC NHIỆT ĐỘ
//  Priority 3 | Core 1 | Chu kỳ 1000ms
// ============================================================
static void task_ds18b20(void*) {
    Serial.println("[DS18B20] Task started");
    for (;;) {
        ds18b20.requestTemperatures();
        float t = ds18b20.getTempCByIndex(0);
        if (t != DEVICE_DISCONNECTED_C && t > -50.0f) {
            if (xSemaphoreTake(g_mutex, pdMS_TO_TICKS(50)) == pdTRUE) {
                g_data.temp_c = t;
                xSemaphoreGive(g_mutex);
            }
        }
        Serial.printf("[DS18B20] Nhiet do: %.2f °C\n", t);
        wdg_temp = millis();
        vTaskDelay(pdMS_TO_TICKS(1000));
    }
}

// ============================================================
//  TASK 2: ADXL345 - ĐỌC RUNG ĐỘNG
//  Priority 3 | Core 1 | Chu kỳ 100ms
// ============================================================
static void task_adxl345(void*) {
    Serial.println("[ADXL] Task started");
    for (;;) {
        float ax, ay, az;
        adxl_read(ax, ay, az);
        float rms = sqrtf((ax*ax + ay*ay + az*az) / 3.0f);

        if (xSemaphoreTake(g_mutex, pdMS_TO_TICKS(50)) == pdTRUE) {
            g_data.accel_x = ax; g_data.accel_y = ay; g_data.accel_z = az;
            g_data.vibration_rms = rms;
            xSemaphoreGive(g_mutex);
        }

        // [FIX] đơn vị đúng là g
        Serial.printf("[ADXL345] X: %.4f g | Y: %.4f g | Z: %.4f g | RMS: %.4f g\n",
            ax, ay, az, rms);
        wdg_adxl = millis();
        vTaskDelay(pdMS_TO_TICKS(100));
    }
}

// ============================================================
//  TASK 3: ACS712 - ĐỌC DÒNG ĐIỆN + CẬP NHẬT TRẠNG THÁI
//  Priority 3 | Core 1 | Chu kỳ 500ms
// ============================================================
static void task_acs712(void*) {
    Serial.println("[ACS712] Task started");
    for (;;) {
        float cur = acs712_readCurrent();

        if (xSemaphoreTake(g_mutex, pdMS_TO_TICKS(50)) == pdTRUE) {
            g_data.current_a = cur;
            g_data.state = eval_state(g_data.temp_c, cur, g_data.vibration_rms);
            xSemaphoreGive(g_mutex);
        }

        const char* calTag = g_acsRecalFlag ? " [CAL]" : "";
        if (cur < 1.0f) {
            Serial.printf("[ACS712] Dong dien: %+7.1f mA | VZero: %.4fV | Trang thai: %s%s\n",
                cur * 1000.0f, g_acsVZero,
                g_data.state == STATE_NORMAL  ? "NORMAL"  :
                g_data.state == STATE_WARNING ? "WARNING" : "DANGER",
                calTag);
        } else {
            Serial.printf("[ACS712] Dong dien: %+6.3f A  | VZero: %.4fV | Trang thai: %s%s\n",
                cur, g_acsVZero,
                g_data.state == STATE_NORMAL  ? "NORMAL"  :
                g_data.state == STATE_WARNING ? "WARNING" : "DANGER",
                calTag);
        }

        wdg_acs = millis();
        vTaskDelay(pdMS_TO_TICKS(500));
    }
}

// ============================================================
//  TASK 4: MQTT PUBLISH + RECONNECT
//  Priority 2 | Core 0 | Chu kỳ 2000ms
// ============================================================
static void task_mqtt(void*) {
    mqtt.setServer(mqtt_server, mqtt_port);
    mqtt.setCallback(mqtt_callback);
    mqtt.setBufferSize(512);
    Serial.println("[MQTT] Task started");

    for (;;) {
        if (!WiFi.isConnected()) {
            Serial.println("[WiFi] Reconnecting...");
            WiFi.reconnect();
            // [FIX 4] KHÔNG reset wdg_mqtt ở đây
            //         chỉ reset sau khi publish thành công (cuối loop)
            vTaskDelay(pdMS_TO_TICKS(3000));
            continue;
        }

        if (!mqtt.connected()) {
            Serial.printf("[MQTT] Connecting to %s...\n", mqtt_server);
            char clientId[32];
            snprintf(clientId, sizeof(clientId), "esp32-%06X", (uint32_t)ESP.getEfuseMac());
            if (mqtt.connect(clientId)) {
                Serial.println("[MQTT] Connected!");
                mqtt.subscribe(TOPIC_CTRL_BUZZER);
                mqtt.subscribe(TOPIC_CTRL_RELAY);
                mqtt.subscribe(TOPIC_CONFIG_WIFI);
            } else {
                Serial.printf("[MQTT] Failed rc=%d\n", mqtt.state());
                vTaskDelay(pdMS_TO_TICKS(5000));
                continue;
            }
        }

        mqtt.loop();

        SensorData_t snap;
        if (xSemaphoreTake(g_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
            snap = g_data; xSemaphoreGive(g_mutex);
        }

        bool buzCmd = false, relCmd = false;
        getCtrl(buzCmd, relCmd);

        // [FIX 8] Thêm timestamp; [FIX 6] bỏ field trùng lặp
        // [FIX 9] Kiểm tra kích thước output trước khi publish
        {
            StaticJsonDocument<320> doc;
            doc["device_id"]  = device_id;
            doc["ts_ms"]      = millis();           // [FIX 8] timestamp
            doc["temp_c"]     = round(snap.temp_c * 100) / 100.0;    // [FIX 6] chỉ 1 key
            doc["current_a"]  = round(snap.current_a * 1000) / 1000.0;
            doc["vib_rms"]    = round(snap.vibration_rms * 1000) / 1000.0;
            doc["vib_alarm"]  = snap.vibration_rms >= VIB_WARN;
            doc["state"]      = (int)snap.state;
            doc["state_str"]  = stateStr(snap.state);
            JsonObject accel  = doc.createNestedObject("accel");
            accel["x"] = round(snap.accel_x * 1000) / 1000.0;
            accel["y"] = round(snap.accel_y * 1000) / 1000.0;
            accel["z"] = round(snap.accel_z * 1000) / 1000.0;

            char buf[320];
            size_t written = serializeJson(doc, buf, sizeof(buf));
            if (written > 0 && written < sizeof(buf)) {   // [FIX 9]
                mqtt.publish(TOPIC_TELEMETRY, buf, false);
            } else {
                Serial.println("[MQTT] ERR: JSON telemetry qua lon hoac rong!");
            }

            Serial.printf("[MQTT PUB] %.2f°C | %.3fA | RMS: %.4f g | %s | WiFi: %d dBm\n",
                snap.temp_c, snap.current_a, snap.vibration_rms,
                stateStr(snap.state), WiFi.RSSI());
        }

        // WiFi Status
        {
            StaticJsonDocument<160> doc;
            doc["device_id"] = device_id;
            doc["ts_ms"]     = millis();
            doc["connected"] = WiFi.isConnected();
            doc["ssid"]      = WiFi.SSID();
            doc["rssi"]      = WiFi.RSSI();
            char buf[160];
            size_t written = serializeJson(doc, buf, sizeof(buf));
            if (written > 0 && written < sizeof(buf))
                mqtt.publish(TOPIC_WIFI_STATUS, buf, false);
        }

        // Device Status
        {
            StaticJsonDocument<160> doc;
            doc["device_id"] = device_id;
            doc["ts_ms"]     = millis();
            doc["buzzer"]    = buzCmd || (snap.state == STATE_DANGER);
            doc["relay"]     = relCmd && (snap.state != STATE_DANGER);  // [FIX] khớp logic fanShouldRun
            char buf[160];
            size_t written = serializeJson(doc, buf, sizeof(buf));
            if (written > 0 && written < sizeof(buf))
                mqtt.publish(TOPIC_DEVICE_STATUS, buf, false);
        }

        wdg_mqtt = millis();  // [FIX 4] chỉ reset sau khi publish thành công
        vTaskDelay(pdMS_TO_TICKS(2000));
    }
}

// ============================================================
//  TASK 5: BUZZER + LED FAULT + LED NORMAL
//  Priority 4 | Core 1
//
//  [FIX 3] Cấu trúc lại logic để tránh bật buzzer 2 lần
//  khi DANGER (cũ: chạy nhánh else DANGER xong còn check
//  buzzerActive && NORMAL -> không chạy nhưng dễ nhầm)
// ============================================================
static void task_buzzer_led(void*) {
    Serial.println("[ALERT] Task started");
    for (;;) {
        SystemState_t state = STATE_NORMAL;
        if (xSemaphoreTake(g_mutex, pdMS_TO_TICKS(30)) == pdTRUE) {
            state = g_data.state;
            xSemaphoreGive(g_mutex);
        }

        bool buzCmd = false, relCmd = false;
        getCtrl(buzCmd, relCmd);  // [FIX 2]

        // Dashboard yêu cầu buzzer (khi state NORMAL)
        bool dashboardBuzzer = buzCmd && (state == STATE_NORMAL);

        if (state == STATE_NORMAL && !dashboardBuzzer) {
            // ✅ NORMAL, không có lệnh buzzer: LED xanh sáng, đỏ tắt, buzzer tắt
            digitalWrite(PIN_LED_NORMAL, HIGH);
            digitalWrite(PIN_LED_FAULT,  LOW);
            digitalWrite(BUZZER_PIN,     LOW);
            vTaskDelay(pdMS_TO_TICKS(200));

        } else if (state == STATE_NORMAL && dashboardBuzzer) {
            // 📡 Dashboard bật buzzer khi hệ thống NORMAL
            digitalWrite(PIN_LED_NORMAL, LOW);
            digitalWrite(PIN_LED_FAULT,  HIGH);
            digitalWrite(BUZZER_PIN,     HIGH);
            vTaskDelay(pdMS_TO_TICKS(100));
            digitalWrite(PIN_LED_FAULT,  LOW);
            digitalWrite(BUZZER_PIN,     LOW);
            vTaskDelay(pdMS_TO_TICKS(100));

        } else if (state == STATE_WARNING) {
            // ⚠️ WARNING: LED đỏ chớp chậm + buzzer
            digitalWrite(PIN_LED_NORMAL, LOW);
            digitalWrite(PIN_LED_FAULT,  HIGH);
            digitalWrite(BUZZER_PIN,     HIGH);
            vTaskDelay(pdMS_TO_TICKS(400));
            digitalWrite(PIN_LED_FAULT,  LOW);
            digitalWrite(BUZZER_PIN,     LOW);
            vTaskDelay(pdMS_TO_TICKS(600));

        } else {
            // 🔴 DANGER: LED đỏ chớp nhanh + buzzer liên tục
            digitalWrite(PIN_LED_NORMAL, LOW);
            digitalWrite(PIN_LED_FAULT,  HIGH);
            digitalWrite(BUZZER_PIN,     HIGH);
            vTaskDelay(pdMS_TO_TICKS(100));
            digitalWrite(PIN_LED_FAULT,  LOW);
            digitalWrite(BUZZER_PIN,     LOW);
            vTaskDelay(pdMS_TO_TICKS(100));
        }
        // [FIX 3] Không còn đoạn "if (buzzerActive && state == STATE_NORMAL)"
        // gây bật buzzer lần 2 sau khi xử lý DANGER xong
    }
}

// ============================================================
//  TASK 6: RELAY - NGẮT TẢI
//  Priority 4 | Core 1 | Chu kỳ 200ms
// ============================================================
static void task_relay(void*) {
    Serial.println("[RELAY] Task started");
    for (;;) {
        SystemState_t state = STATE_NORMAL;
        if (xSemaphoreTake(g_mutex, pdMS_TO_TICKS(30)) == pdTRUE) {
            state = g_data.state;
            xSemaphoreGive(g_mutex);
        }
        bool relCmd = false, dummy = false;
        getCtrl(dummy, relCmd);  // [FIX 2]

        // [FIX] relCmd: lệnh thủ công từ dashboard, true = MUỐN quạt chạy.
        // Mặc định (chưa có lệnh nào) cũng phải chạy → relCmd nên init = true.
        // DANGER luôn được ưu tiên cao nhất, ép ngắt bất kể lệnh thủ công.
        bool fanShouldRun = relCmd && (state != STATE_DANGER);
        digitalWrite(RELAY_PIN, fanShouldRun ? HIGH : LOW);
        vTaskDelay(pdMS_TO_TICKS(200));
    }
}

// ============================================================
//  TASK 7: LCD - HIỂN THỊ
//  Priority 1 | Core 0 | Chu kỳ 1000ms
// ============================================================
static void task_lcd(void*) {
    Serial.println("[LCD] Task started");
    for (;;) {
        SensorData_t snap = {};
        if (xSemaphoreTake(g_mutex, pdMS_TO_TICKS(50)) == pdTRUE) {
            snap = g_data;
            xSemaphoreGive(g_mutex);
        }

        // Mỗi dòng LCD 16 ký tự - format cố định, pad space cuối để xóa ký tự cũ
        // Dòng 1: "T:25.3C I:1.2A  " → tối đa 16 ký tự
        // Dòng 2: "RMS: 9.81g       " → tối đa 16 ký tự
        char line1[17], line2[17];
        snprintf(line1, sizeof(line1), "T:%.1fC I:%.2fA", snap.temp_c, snap.current_a);
        snprintf(line2, sizeof(line2), "RMS:%.3fg", snap.vibration_rms);

        // Pad space đến đúng 16 ký tự để xóa ký tự cũ còn sót
        int len1 = strlen(line1);
        for (int i = len1; i < 16; i++) line1[i] = ' ';
        line1[16] = '\0';

        int len2 = strlen(line2);
        for (int i = len2; i < 16; i++) line2[i] = ' ';
        line2[16] = '\0';

        lcd.setCursor(0, 0); lcd.print(line1);
        lcd.setCursor(0, 1); lcd.print(line2);

        vTaskDelay(pdMS_TO_TICKS(1000));
    }
}

// ============================================================
//  TASK 8: WATCHDOG - GIÁM SÁT TẤT CẢ TASK
//  Priority 5 (cao nhất) | Core 0 | Chu kỳ 5000ms
// ============================================================
static void task_watchdog(void*) {
    Serial.println("[WDG] Task started");
    vTaskDelay(pdMS_TO_TICKS(8000));

    for (;;) {
        uint32_t now = millis();
        bool ok = true;

        if (now - wdg_temp > WDG_TIMEOUT_MS)      { Serial.println("[WDG] DS18B20 hung!"); ok = false; }
        if (now - wdg_adxl > WDG_TIMEOUT_MS)      { Serial.println("[WDG] ADXL hung!");    ok = false; }
        if (now - wdg_acs  > WDG_TIMEOUT_MS)      { Serial.println("[WDG] ACS712 hung!");  ok = false; }
        if (now - wdg_mqtt > WDG_TIMEOUT_MS * 2)  { Serial.println("[WDG] MQTT hung!");    ok = false; }

        if (!ok) {
            Serial.println("[WDG] *** HUNG - Restarting ***");
            lcd.clear();
            lcd.setCursor(0, 0); lcd.print("SYSTEM ERROR");
            lcd.setCursor(0, 1); lcd.print("Restarting...");
            vTaskDelay(pdMS_TO_TICKS(3000));
            esp_restart();
        } else {
            SensorData_t snap = {};
            if (xSemaphoreTake(g_mutex, pdMS_TO_TICKS(50)) == pdTRUE) {
                snap = g_data;
                xSemaphoreGive(g_mutex);
            }
            Serial.printf("[WDG] OK | Nhiet do: %.2f °C | Dong dien: %.3f A | "
                          "RMS: %.4f g | VZero: %.4f V | Trang thai: %s\n",  // [FIX 5]
                snap.temp_c, snap.current_a, snap.vibration_rms, g_acsVZero,
                stateStr(snap.state));
        }
        vTaskDelay(pdMS_TO_TICKS(5000));
    }
}

// ============================================================
//  SETUP
// ============================================================
void setup() {
    Serial.begin(115200);
    delay(500);
    Serial.println("\n=== ESP32 BAO TRI MAY MOC - FREERTOS ===");

    // --- GPIO ---
    pinMode(BUZZER_PIN,     OUTPUT); digitalWrite(BUZZER_PIN,     LOW);
    pinMode(RELAY_PIN,      OUTPUT); digitalWrite(RELAY_PIN,      HIGH);  // [FIX] mặc định ON (quạt chạy), vì quạt nối NO
    pinMode(PIN_LED_NORMAL, OUTPUT); digitalWrite(PIN_LED_NORMAL, LOW);
    pinMode(PIN_LED_FAULT,  OUTPUT); digitalWrite(PIN_LED_FAULT,  LOW);
    pinMode(PIN_LED_STATUS, OUTPUT); digitalWrite(PIN_LED_STATUS, LOW);

    // --- ADC - ACS712 ---
    analogReadResolution(12);
    analogSetAttenuation(ADC_11db);

    // --- I2C Bus 1: LCD ---
    Wire.begin(LCD_SDA, LCD_SCL);
    Wire.setClock(100000);
    lcd.init(); lcd.backlight(); lcd.clear();
    lcd.setCursor(0, 0); lcd.print("Booting...");

    // --- I2C Bus 2: ADXL345 ---
    Wire1.begin(ADXL_SDA, ADXL_SCL);
    Wire1.setClock(400000);
    if (!adxl_init()) {
        lcd.clear(); lcd.print("ERR: ADXL345");
        Serial.println("[SETUP] ADXL345 FAILED");
        while (1) delay(1000);
    }
    Serial.println("[SETUP] ADXL345 OK");

    // --- DS18B20 ---
    ds18b20.begin();
    Serial.println("[SETUP] DS18B20 OK");

    // --- ACS712 - Calibrate V_zero từ phần cứng (RÚT TẢI TRƯỚC!) ---
    lcd.setCursor(0, 1); lcd.print("ACS Cal...");
    acs712_initialCalibrate();
    lcd.setCursor(0, 1); lcd.print("ACS OK!     ");
    delay(500);

    // --- WiFi ---
    WiFi.mode(WIFI_STA);
    WiFi.begin(ssid, password);
    lcd.setCursor(0, 1); lcd.print("WiFi...");
    Serial.print("[WiFi] Connecting");
    for (int i = 0; i < 30 && !WiFi.isConnected(); i++) {
        delay(500); Serial.print(".");
    }
    if (WiFi.isConnected()) {
        Serial.printf("\n[WiFi] OK IP=%s\n", WiFi.localIP().toString().c_str());
        lcd.setCursor(0, 1); lcd.print("WiFi OK!        ");
    } else {
        Serial.println("\n[WiFi] FAILED - offline mode");
        lcd.setCursor(0, 1); lcd.print("WiFi FAIL!      ");
    }
    delay(1000);

    // --- Mutex & Watchdog init ---
    g_mutex     = xSemaphoreCreateMutex();
    g_ctrlMutex = xSemaphoreCreateMutex();  // [FIX 2]
    uint32_t now = millis();
    wdg_temp = wdg_adxl = wdg_acs = wdg_mqtt = now;

    // --- Tạo FreeRTOS Tasks ---
    xTaskCreatePinnedToCore(task_ds18b20,    "DS18",  3072, NULL, 3, NULL, 1);
    xTaskCreatePinnedToCore(task_adxl345,    "ADXL",  3072, NULL, 3, NULL, 1);
    xTaskCreatePinnedToCore(task_acs712,     "ACS",   4096, NULL, 3, NULL, 1);
    xTaskCreatePinnedToCore(task_mqtt,       "MQTT",  8192, NULL, 2, NULL, 0);
    xTaskCreatePinnedToCore(task_buzzer_led, "BUZ",   3072, NULL, 4, NULL, 1);
    xTaskCreatePinnedToCore(task_relay,      "RLY",   2048, NULL, 4, NULL, 1);
    xTaskCreatePinnedToCore(task_lcd,        "LCD",   4096, NULL, 1, NULL, 0);
    xTaskCreatePinnedToCore(task_watchdog,   "WDG",   3072, NULL, 5, NULL, 0);

    lcd.clear();
    lcd.setCursor(0, 0); lcd.print("System Ready!   ");
    Serial.println("[SETUP] All 8 tasks running!");
}

void loop() {
    vTaskDelay(pdMS_TO_TICKS(10000));
}