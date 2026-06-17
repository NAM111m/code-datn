#!/usr/bin/env python3
# server_full.py
"""
Flask + MQTT server for Industrial IoT monitoring.
- MQTT subscribe to devices/+/telemetry and devices/+/wifi_status
- Compute RMS vibration per device (sliding window)
- Load scaler.pkl and rf_model.pkl and run classification
- Save telemetry and wifi_status to PostgreSQL
- Send email alerts when thresholds or AI indicate issues (WARNING / DANGER)
- API: /, /api/latest, /api/ai_predict, /api/wifi_status
"""
 
import os
import re
import json
import math
import time
import pickle
import traceback
import threading
import smtplib
from email.mime.text import MIMEText
from collections import defaultdict, deque
from datetime import datetime, timezone
import numpy as np
import pandas as pd
import psycopg2
from psycopg2 import pool as pg_pool
from paho.mqtt import client as mqtt
from flask import Flask, jsonify, render_template, request
from flask_cors import CORS

# ============================================================
#  CẤU HÌNH MẠNG & MQTT
# ============================================================
BROKER_URI  = "10.29.199.42"
BROKER_PORT = 1883

# ============================================================
#  CẤU HÌNH DATABASE
# ============================================================
DB_CONFIG = {
    "host":     "localhost",
    "database": "IoT_DoAnTotNghiep",
    "user":     "postgres",
    "password": "111212",
    "port":     5432
}

# ============================================================
#  CẤU HÌNH EMAIL GMAIL
# ============================================================
EMAIL_CONFIG = {
    "SMTP_SERVER":    "smtp.gmail.com",
    "SMTP_PORT":      587,
    "EMAIL_SENDER":   "namhero2003@gmail.com",
    "EMAIL_PASSWORD": "htdc omir vncb seeh",   # App Password 16 ký tự
    "EMAIL_RECEIVER": "nguyenbro1545@gmail.com"
}

# ============================================================
#  NGƯỠNG DANGER — load từ DB khi khởi động
#  Đơn vị: °C | g | A
# ============================================================
THRESHOLD_TEMP      = 70.0
THRESHOLD_VIBRATION = 5.0     # g    (= 5g, khớp với VIB_DANGER trên ESP32)
THRESHOLD_CURRENT   = 0.5     # [FIX] thực tế current chỉ 0.05-0.12A

# Ngưỡng WARNING (nhẹ hơn DANGER) — khớp với ESP32
THRESHOLD_TEMP_WARN      = 50.0
THRESHOLD_VIB_WARN       = 2.5   # g    (= 2.5g, khớp với VIB_WARN trên ESP32)
THRESHOLD_CURRENT_WARN   = 0.8  # [FIX] khớp với firmware ESP32 (CURRENT_WARN)

# [FIX] Hysteresis — độ trễ chống nhảy qua nhảy lại (chattering) khi giá trị
# dao động sát ngưỡng. Để THOÁT khỏi WARNING/DANGER, giá trị phải giảm xuống
# dưới (ngưỡng - margin), không chỉ cần thấp hơn ngưỡng một chút.
HYST_TEMP_MARGIN    = 2.0    # °C
HYST_VIB_MARGIN     = 0.5    # g
HYST_CURRENT_MARGIN = 0.02   # A

# State hiện tại mỗi device, dùng để áp dụng hysteresis (so sánh "trạng thái
# trước" để biết nên dùng ngưỡng nào — ngưỡng vào hay ngưỡng ra)
_device_state = defaultdict(lambda: "NORMAL")

# ============================================================
#  COOLDOWN EMAIL — tránh spam
# ============================================================
EMAIL_COOLDOWN_SEC = 900          # [FIX] tăng lên 15 phút mỗi thiết bị, giảm spam
_email_last_sent   = {}           # { device_id: timestamp }
_email_last_state  = {}           # { device_id: "WARNING" | "DANGER" }

# ============================================================
#  AI MODEL
# ============================================================
RMS_WINDOW_SIZE  = 100
FEATURE_COLUMNS  = ['temperature', 'current', 'accel_x', 'accel_y', 'accel_z']
MODEL_SCALER_PATH = "scaler.pkl"
MODEL_RF_PATH     = "rf_model.pkl"

# ============================================================
#  KHỞI TẠO APP
# ============================================================
app = Flask(__name__, static_folder='static', template_folder='templates')
CORS(app)

# ============================================================
#  BIẾN TOÀN CỤC
# ============================================================
vibration_buffers    = defaultdict(lambda: deque(maxlen=RMS_WINDOW_SIZE))
# Offset gia tốc tĩnh mỗi thiết bị (sửa lỗi 4 — bias trọng lực)
accel_offset         = defaultdict(lambda: {"x": 0.0, "y": 0.0, "z": 0.0})
accel_calibrated     = defaultdict(bool)
# [FIX 6] Buffer các mẫu dùng để tính offset baseline (warm-up + trung bình)
accel_calib_buffer   = defaultdict(list)
CALIB_WARMUP_SAMPLES = 5    # bỏ qua N mẫu đầu (sensor/board chưa ổn định lúc mới boot)
CALIB_AVG_SAMPLES    = 10   # lấy trung bình M mẫu kế tiếp làm offset baseline
# [FIX] Track ts_ms (millis() của ESP32) cuối nhận được mỗi device, để phát hiện
# ESP32 reboot (millis() reset về số nhỏ) và tự động recalibrate offset rung.
last_ts_ms           = defaultdict(lambda: None)
scaler               = None
rf_model             = None
mqtt_publisher_client = None

# ── Connection pool — Sửa lỗi 3 (DB Connection Leak) ──────────
_db_pool = None

def get_db_pool():
    global _db_pool
    if _db_pool is None or _db_pool.closed:
        _db_pool = pg_pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            **DB_CONFIG
        )
        print("✓ DB connection pool khởi tạo (2–10 connections)")
    return _db_pool

def get_conn():
    """Lấy connection từ pool thay vì tạo mới."""
    return get_db_pool().getconn()

def put_conn(conn):
    """Trả connection về pool."""
    if conn is not None:
        try:
            get_db_pool().putconn(conn)
        except Exception:
            pass

# ── Email queue — Sửa lỗi 2 (Email blocking MQTT) ─────────────
import queue as _queue
_email_queue = _queue.Queue(maxsize=50)

def _email_worker():
    """Thread nền xử lý email — không bao giờ block on_message."""
    while True:
        item = _email_queue.get()
        if item is None:
            break
        try:
            _do_send_email(**item)
        except Exception:
            traceback.print_exc()
        finally:
            _email_queue.task_done()

_email_thread = threading.Thread(target=_email_worker, daemon=True)
_email_thread.start()


# ============================================================
#  LOAD NGƯỠNG TỪ DATABASE
# ============================================================
def load_thresholds():
    global THRESHOLD_TEMP, THRESHOLD_VIBRATION, THRESHOLD_CURRENT
    conn = None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT threshold_temp, threshold_vibration, threshold_current
            FROM thresholds WHERE id = 1
        """)
        row = cur.fetchone()
        if row:
            THRESHOLD_TEMP      = float(row[0])
            THRESHOLD_VIBRATION = float(row[1])
            THRESHOLD_CURRENT   = float(row[2])
            print(f"[Threshold] Temp={THRESHOLD_TEMP}°C | "
                  f"Vib={THRESHOLD_VIBRATION} g | "
                  f"Current={THRESHOLD_CURRENT} A")
        cur.close()
    except Exception as e:
        print(f"[Threshold] Loi load: {e}")
    finally:
        put_conn(conn)


def save_thresholds(temp, vibration, current):
    global THRESHOLD_TEMP, THRESHOLD_VIBRATION, THRESHOLD_CURRENT
    conn = None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            UPDATE thresholds
            SET threshold_temp=%s, threshold_vibration=%s,
                threshold_current=%s, updated_at=NOW()
            WHERE id = 1
        """, (float(temp), float(vibration), float(current)))
        conn.commit()
        cur.close()
        THRESHOLD_TEMP      = float(temp)
        THRESHOLD_VIBRATION = float(vibration)
        THRESHOLD_CURRENT   = float(current)
        print(f"[Threshold] Da cap nhat: T={temp} | V={vibration} | I={current}")
        return True
    except Exception:
        print("[Threshold] Loi luu:")
        traceback.print_exc()
        return False
    finally:
        put_conn(conn)


# ============================================================
#  MQTT PUBLISHER — gửi lệnh điều khiển xuống ESP32
# ============================================================
def init_mqtt_publisher():
    global mqtt_publisher_client
    try:
        mqtt_publisher_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
        mqtt_publisher_client.connect(BROKER_URI, BROKER_PORT, keepalive=60)
        mqtt_publisher_client.loop_start()
        print("✓ MQTT Publisher initialized.")
    except Exception as e:
        print(f"✗ Failed to initialize MQTT Publisher: {e}")


def publish_control_command(device_id: str, control_type: str, state: bool) -> bool:
    if mqtt_publisher_client is None:
        print("publish_control_command: MQTT publisher not initialized")
        return False
    if not mqtt_publisher_client.is_connected():
        try:
            mqtt_publisher_client.reconnect()
            time.sleep(0.5)
            if not mqtt_publisher_client.is_connected():
                return False
        except Exception as e:
            print(f"publish_control_command reconnect error: {e}")
            return False
    try:
        topic   = f"devices/{device_id}/control/{control_type}"
        payload = "1" if state else "0"
        result  = mqtt_publisher_client.publish(topic, payload, qos=1)
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print(f"✓ publish_control_command: {topic} -> {payload}")
            return True
        print(f"✗ publish_control_command failed: rc={result.rc}")
        return False
    except Exception as e:
        print(f"publish_control_command error: {e}")
        traceback.print_exc()
        return False


def publish_wifi_config(device_id, ssid, password, mqtt_broker_ip, mqtt_broker_port):
    if mqtt_publisher_client is None:
        print("MQTT publisher client not initialized")
        return False
    if not mqtt_publisher_client.is_connected():
        try:
            mqtt_publisher_client.reconnect()
            time.sleep(0.5)
            if not mqtt_publisher_client.is_connected():
                return False
        except Exception as e:
            print(f"Error reconnecting MQTT publisher: {e}")
            return False
    try:
        topic   = f"devices/{device_id}/config/wifi"
        payload = json.dumps({
            "device_id":        device_id,
            "ssid":             ssid,
            "password":         password,
            "mqtt_broker_ip":   mqtt_broker_ip,
            "mqtt_broker_port": mqtt_broker_port,
            "timestamp":        datetime.now(timezone.utc).isoformat()
        })
        result = mqtt_publisher_client.publish(topic, payload, qos=1)
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print(f"✓ Published WiFi config to {topic}")
            return True
        print(f"✗ Failed to publish WiFi config: error code {result.rc}")
        return False
    except Exception as e:
        print(f"Error publishing WiFi config: {e}")
        traceback.print_exc()
        return False


# ============================================================
#  UTILITIES
# ============================================================
def safe_json_parse(raw: str):
    if not isinstance(raw, str):
        raw = str(raw)
    try:
        return json.loads(raw)
    except Exception:
        pass
    s = raw.replace("NaN", "null").replace("nan", "null") \
           .replace("INF", "null").replace("inf", "null") \
           .replace("'", '"') \
           .replace("True", "true").replace("False", "false") \
           .replace("None", "null")
    s = ''.join(ch for ch in s if ord(ch) >= 32 or ch in ("\n", "\r", "\t"))
    return json.loads(s)


def safe_float(x, default=0.0):
    try:
        if x is None:
            return default
        v = float(x)
        return default if (math.isnan(v) or math.isinf(v)) else v
    except Exception:
        return default


def detect_esp32_reboot(device_id: str, ts_ms) -> bool:
    """
    Phát hiện ESP32 mới reboot dựa vào ts_ms (millis()) bị reset về số nhỏ.
    Nếu phát hiện, xóa cờ calibrated để hàm update_vibration_rms tự
    calibrate lại offset rung từ đầu (tránh dùng offset cũ đã lệch).
    """
    if ts_ms is None:
        return False
    try:
        ts_ms = int(ts_ms)
    except (TypeError, ValueError):
        return False

    prev = last_ts_ms[device_id]
    last_ts_ms[device_id] = ts_ms

    # Lần đầu nhận data của device này -> chưa có gì để so sánh
    if prev is None:
        return False

    # ts_ms nhỏ hơn đáng kể so với lần trước -> millis() đã reset -> ESP32 reboot
    if ts_ms < prev - 2000:  # trừ hao 2s để tránh nhiễu/out-of-order packet
        if accel_calibrated[device_id]:
            print(f"[RMS] Phát hiện {device_id} reboot (ts_ms {prev} -> {ts_ms}). "
                  f"Reset calibrate offset rung.")
        accel_calibrated[device_id]   = False
        accel_offset[device_id]       = {"x": 0.0, "y": 0.0, "z": 0.0}
        accel_calib_buffer[device_id] = []
        vibration_buffers[device_id].clear()
        return True

    return False


def update_vibration_rms(device_id: str, ax: float, ay: float, az: float) -> float:
    # [FIX 6] Calibrate offset bằng warm-up + trung bình nhiều mẫu, tránh lấy
    # đúng mẫu đầu tiên (lúc sensor/ESP32 có thể chưa ổn định) làm baseline.
    if not accel_calibrated[device_id]:
        buf_calib = accel_calib_buffer[device_id]
        buf_calib.append((ax, ay, az))

        total_needed = CALIB_WARMUP_SAMPLES + CALIB_AVG_SAMPLES
        if len(buf_calib) < total_needed:
            # Chưa đủ mẫu để chốt offset -> coi như chưa có rung (a_rms=0)
            return 0.0

        # Bỏ qua CALIB_WARMUP_SAMPLES mẫu đầu, lấy trung bình các mẫu còn lại
        samples = buf_calib[CALIB_WARMUP_SAMPLES:total_needed]
        ox = sum(s[0] for s in samples) / len(samples)
        oy = sum(s[1] for s in samples) / len(samples)
        oz = sum(s[2] for s in samples) / len(samples)

        accel_offset[device_id]     = {"x": ox, "y": oy, "z": oz}
        accel_calibrated[device_id] = True
        accel_calib_buffer[device_id] = []  # giải phóng buffer, không cần nữa
        print(f"[RMS] Calibrate offset {device_id} (warm-up {CALIB_WARMUP_SAMPLES}, "
              f"avg {CALIB_AVG_SAMPLES}): ax0={ox:.4f} ay0={oy:.4f} az0={oz:.4f}")

    ox = accel_offset[device_id]["x"]
    oy = accel_offset[device_id]["y"]
    oz = accel_offset[device_id]["z"]

    # Gia tốc động (loại bỏ thành phần tĩnh / trọng lực)
    dax = (ax or 0.0) - ox
    day = (ay or 0.0) - oy
    daz = (az or 0.0) - oz
    a_mag = math.sqrt(dax**2 + day**2 + daz**2)

    buf = vibration_buffers[device_id]
    buf.append(a_mag)
    arr = np.array(buf, dtype=float)
    return float(np.sqrt(np.mean(arr**2))) if arr.size > 0 else 0.0


# ============================================================
#  LOAD AI MODEL
# ============================================================
def load_model():
    global scaler, rf_model
    try:
        if os.path.exists(MODEL_SCALER_PATH):
            with open(MODEL_SCALER_PATH, "rb") as f:
                scaler = pickle.load(f)
            print("Loaded scaler:", MODEL_SCALER_PATH)
        else:
            print("scaler.pkl not found; continuing without scaler")
    except Exception:
        print("Failed to load scaler:")
        traceback.print_exc()
        scaler = None

    try:
        if os.path.exists(MODEL_RF_PATH):
            with open(MODEL_RF_PATH, "rb") as f:
                rf_model = pickle.load(f)
            print("Loaded RF model:", MODEL_RF_PATH)
        else:
            print("rf_model.pkl not found; continuing without model")
    except Exception:
        print("Failed to load RF model:")
        traceback.print_exc()
        rf_model = None


# ============================================================
#  DATABASE HELPERS
# ============================================================
def init_database():
    conn = None
    try:
        conn = get_conn()
        cur  = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS machine_data (
                id        SERIAL PRIMARY KEY,
                device_id VARCHAR(50) NOT NULL,
                temperature REAL,
                current     REAL,
                accel_x     REAL,
                accel_y     REAL,
                accel_z     REAL,
                a_rms       REAL,
                failure     VARCHAR(50),
                timestamp   TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS wifi_status (
                id        SERIAL PRIMARY KEY,
                device_id VARCHAR(50) NOT NULL,
                connected BOOLEAN,
                ssid      VARCHAR(100),
                rssi      INTEGER,
                timestamp TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS device_status (
                id        SERIAL PRIMARY KEY,
                device_id VARCHAR(50) NOT NULL,
                buzzer    BOOLEAN,
                relay     BOOLEAN,
                timestamp TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS thresholds (
                id                  INTEGER PRIMARY KEY DEFAULT 1,
                threshold_temp      REAL DEFAULT 70.0,
                threshold_vibration REAL DEFAULT 5.0,
                threshold_current   REAL DEFAULT 10.0,
                updated_at          TIMESTAMPTZ DEFAULT NOW(),
                CONSTRAINT single_row CHECK (id = 1)
            );
        """)
        # Chèn dòng mặc định với đơn vị g đúng (5.0 = 5g, khớp ESP32 VIB_DANGER)
        cur.execute("""
            INSERT INTO thresholds (id, threshold_temp, threshold_vibration, threshold_current)
            VALUES (1, 70.0, 5.0, 10.0)
            ON CONFLICT DO NOTHING;
        """)

        conn.commit()
        cur.close()
        print("✅ Database khoi tao thanh cong!")
    except Exception:
        print("❌ Loi khoi tao DB:")
        traceback.print_exc()
    finally:
        put_conn(conn)


def save_extended_telemetry(device_id, temperature, vibration_rms, current,
                            accel_x, accel_y, accel_z, failure=None):
    conn = None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO machine_data
                (device_id, temperature, current, accel_x, accel_y, accel_z, a_rms, failure, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            device_id,
            None if temperature   is None else float(temperature),
            None if current       is None else float(current),
            None if accel_x       is None else float(accel_x),
            None if accel_y       is None else float(accel_y),
            None if accel_z       is None else float(accel_z),
            None if vibration_rms is None else float(vibration_rms),
            failure,
            datetime.now(timezone.utc)  # [FIX] timestamp UTC tường minh, tránh lệch múi giờ
        ))
        conn.commit()
        cur.close()
        print(f"[DB] Saved {device_id} | T={temperature} | I={current} "
              f"| RMS={vibration_rms:.3f} g | status={failure}")
    except Exception:
        print("[DB] Save telemetry error:")
        traceback.print_exc()
    finally:
        put_conn(conn)


def save_wifi_status(device_id, connected, ssid, rssi):
    conn = None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO wifi_status (device_id, connected, ssid, rssi, timestamp)
            VALUES (%s, %s, %s, %s, %s)
        """, (device_id, bool(connected), ssid, None if rssi is None else int(rssi),
              datetime.now(timezone.utc)))  # [FIX] timestamp UTC tường minh
        conn.commit()
        cur.close()
        print(f"[DB] Saved wifi {device_id} | ssid={ssid} | rssi={rssi}")
    except Exception:
        print("[DB] Save wifi error:")
        traceback.print_exc()
    finally:
        put_conn(conn)


# ============================================================
#  EMAIL ALERT — có cooldown, phân biệt WARNING / DANGER
# ============================================================
def _do_send_email(device_id, temp, vibration_rms, current,
                   accel_x, accel_y, accel_z, alert_level="WARNING"):
    """Hàm thực sự gửi email — chạy trong thread nền (_email_worker).

    Sửa lỗi: KHÔNG check lại cooldown/state_changed ở đây, vì
    send_alert_email() đã check và cập nhật _email_last_sent /
    _email_last_state TRƯỚC KHI enqueue. Nếu check lại ở đây sẽ luôn
    fail (now - last_time ~ 0, alert_level == last_state) -> email
    không bao giờ được gửi.
    """
    try:
        icon    = "⚠️" if alert_level == "WARNING" else "🔴"
        subject = f"[{alert_level}] {icon} May {device_id} vuot nguong an toan"

        body = (
            f"{'='*48}\n"
            f"  CANH BAO HE THONG MAY MOC\n"
            f"{'='*48}\n"
            f"Thiet bi     : {device_id}\n"
            f"Muc canh bao : {alert_level} {icon}\n"
            f"Thoi gian    : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"--- Thong so hien tai ---\n"
            f"Nhiet do  : {temp:.2f} °C\n"
            f"  Nguong WARNING >= {THRESHOLD_TEMP_WARN} °C\n"
            f"  Nguong DANGER  >= {THRESHOLD_TEMP} °C\n\n"
            f"Dong dien : {current:.3f} A\n"
            f"  Nguong WARNING >= {THRESHOLD_CURRENT_WARN} A\n"
            f"  Nguong DANGER  >= {THRESHOLD_CURRENT} A\n\n"
            f"Rung RMS  : {vibration_rms:.3f} g\n"
            f"  Nguong WARNING >= {THRESHOLD_VIB_WARN} g\n"
            f"  Nguong DANGER  >= {THRESHOLD_VIBRATION} g\n\n"
            f"Gia toc   : X={accel_x:.3f}  Y={accel_y:.3f}  Z={accel_z:.3f}  (g)\n\n"
            f"{'='*48}\n"
            f"Vui long kiem tra may moc som nhat co the!\n"
        )

        msg            = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"]    = EMAIL_CONFIG["EMAIL_SENDER"]
        msg["To"]      = EMAIL_CONFIG["EMAIL_RECEIVER"]

        server = smtplib.SMTP(
            EMAIL_CONFIG["SMTP_SERVER"],
            EMAIL_CONFIG["SMTP_PORT"],
            timeout=10
        )
        server.starttls()
        server.login(EMAIL_CONFIG["EMAIL_SENDER"], EMAIL_CONFIG["EMAIL_PASSWORD"])
        server.sendmail(
            EMAIL_CONFIG["EMAIL_SENDER"],
            EMAIL_CONFIG["EMAIL_RECEIVER"],
            msg.as_string()
        )
        server.quit()

        # Cập nhật cooldown sau khi gửi thành công
        _email_last_sent[device_id]  = now
        _email_last_state[device_id] = alert_level
        print(f"[EMAIL] ✅ Gui thanh cong [{alert_level}] cho {device_id}")

    except Exception:
        print("[EMAIL] ❌ Loi gui email:")
        traceback.print_exc()


def send_alert_email(device_id, temp, vibration_rms, current,
                     accel_x, accel_y, accel_z, alert_level="WARNING"):
    """
    Sửa lỗi 2: Đưa email vào hàng đợi — KHÔNG block luồng MQTT on_message.
    Cooldown và state-change check vẫn giữ nguyên logic, chỉ chuyển sang
    thread nền để xử lý SMTP (2–5 giây) không làm trễ gói tin tiếp theo.
    """
    now        = time.time()
    last_time  = _email_last_sent.get(device_id, 0)
    last_state = _email_last_state.get(device_id, "")

    cooldown_ok   = (now - last_time) >= EMAIL_COOLDOWN_SEC
    state_changed = (alert_level != last_state)

    if not cooldown_ok and not state_changed:
        remaining = int(EMAIL_COOLDOWN_SEC - (now - last_time))
        print(f"[EMAIL] Bo qua {device_id} [{alert_level}] - cooldown con {remaining}s")
        return

    # Cập nhật cooldown ngay (trước khi enqueue) để tránh race condition
    _email_last_sent[device_id]  = now
    _email_last_state[device_id] = alert_level

    try:
        _email_queue.put_nowait({
            "device_id":    device_id,
            "temp":         temp,
            "vibration_rms": vibration_rms,
            "current":      current,
            "accel_x":      accel_x,
            "accel_y":      accel_y,
            "accel_z":      accel_z,
            "alert_level":  alert_level,
        })
        print(f"[EMAIL] Enqueued [{alert_level}] cho {device_id} (xử lý nền)")
    except _queue.Full:
        print(f"[EMAIL] ⚠ Queue đầy, bỏ qua email [{alert_level}] cho {device_id}")


# ============================================================
#  AI HELPERS
# ============================================================
def get_prediction_and_probs(temp, current, ax, ay, az):
    default = {"label": "Unknown",
               "probabilities": {"NORMAL": 0.0, "WARNING": 0.0, "FAULT": 0.0}}
    if rf_model is None:
        return default

    X_df = pd.DataFrame([[temp, current, ax, ay, az]], columns=FEATURE_COLUMNS)
    try:
        X_in = scaler.transform(X_df) if scaler is not None else X_df.values
    except Exception:
        X_in = X_df.values

    try:
        pred_raw = rf_model.predict(X_in)[0]
    except Exception:
        pred_raw = None

    probs = {"NORMAL": 0.0, "WARNING": 0.0, "FAULT": 0.0}
    try:
        if hasattr(rf_model, "predict_proba"):
            probs_arr = rf_model.predict_proba(X_in)[0]
            if hasattr(rf_model, "classes_"):
                for idx, class_val in enumerate(rf_model.classes_):
                    c = int(class_val) if isinstance(
                        class_val, (int, float, np.integer, np.floating)) else class_val
                    if c == 0:
                        probs["NORMAL"] = float(probs_arr[idx])
                    elif c == 1:
                        probs["WARNING"] = float(probs_arr[idx])
                    elif c == 2:
                        probs["FAULT"] = float(probs_arr[idx])
            else:
                for i, p in enumerate(probs_arr):
                    if i == 0:   probs["NORMAL"]  = float(p)
                    elif i == 1: probs["WARNING"] = float(p)
                    else:        probs["FAULT"]  += float(p)
    except Exception as e:
        print(f"Error getting probabilities: {e}")
        traceback.print_exc()

    label = "Unknown"
    try:
        if pred_raw is None:
            label = "Unknown"
        elif hasattr(rf_model, "classes_"):
            pred_int = int(pred_raw) if isinstance(
                pred_raw, (int, float, np.integer, np.floating)) else pred_raw
            label = {0: "NORMAL", 1: "WARNING", 2: "FAULT"}.get(pred_int, f"Class_{pred_int}")
        else:
            pred_int = int(pred_raw) if isinstance(
                pred_raw, (int, float, np.integer, np.floating)) else pred_raw
            label = {0: "NORMAL", 1: "WARNING"}.get(pred_int, "FAULT")
    except Exception as e:
        print(f"Error determining label: {e}")
        label = str(pred_raw)

    return {"label": label, "probabilities": probs}


def predict_machine_status(temp, current, ax, ay, az):
    # Sửa lỗi 1: luôn trả về nhãn UPPER CASE nhất quán
    res = get_prediction_and_probs(temp, current, ax, ay, az)
    lbl = res.get("label", "Unknown").upper()
    if lbl in ("WARNING", "FAULT", "ERROR"):
        return "WARNING"
    if lbl in ("NORMAL", "OK"):
        return "NORMAL"
    if temp    is not None and temp    > THRESHOLD_TEMP:    return "WARNING"
    if current is not None and current > THRESHOLD_CURRENT: return "WARNING"
    return "NORMAL"


# ============================================================
#  MQTT CALLBACKS
# ============================================================
def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        raw   = msg.payload.decode(errors="replace")
        try:
            payload = safe_json_parse(raw)
        except Exception:
            print("Failed to parse payload:", raw)
            return

        print(f"MQTT [{topic}] -> {payload}")
        device_id = payload.get("device_id", "unknown")

        # ── device_status (buzzer / relay) ──────────────────
        if "device_status" in topic:
            buzzer = payload.get("buzzer", False)
            relay  = payload.get("relay",  False)
            conn_ds = None
            try:
                conn_ds = get_conn()
                cur  = conn_ds.cursor()
                cur.execute("""
                    INSERT INTO device_status (device_id, buzzer, relay, timestamp)
                    VALUES (%s, %s, %s, %s)
                """, (device_id, bool(buzzer), bool(relay),
                      datetime.now(timezone.utc)))  # [FIX] timestamp UTC tường minh
                conn_ds.commit()
                cur.close()
                print(f"[DB] Device status {device_id} - Buzzer:{buzzer} Relay:{relay}")
            except Exception:
                print("[DB] Failed to save device status:")
                traceback.print_exc()
            finally:
                put_conn(conn_ds)

        # ── telemetry ────────────────────────────────────────
        elif "telemetry" in topic:
            temp    = safe_float(payload.get("temperature") or payload.get("temp_c"))
            current = safe_float(payload.get("current")     or payload.get("current_a"))
            accel   = payload.get("accel") if isinstance(payload.get("accel"), dict) else {}
            accel_x = safe_float(payload.get("accel_x") or accel.get("x"))
            accel_y = safe_float(payload.get("accel_y") or accel.get("y"))
            accel_z = safe_float(payload.get("accel_z") or accel.get("z"))

            # [FIX] Dùng trực tiếp vib_rms (đơn vị g) mà ESP32 đã tính sẵn,
            # KHÔNG tự tính lại từ accel_x/y/z trên server nữa. Lý do: ESP32
            # tính RMS theo cửa sổ mẫu thời gian thực, ổn định và nhất quán
            # đơn vị (g) với toàn hệ thống; server tự tính lại từng gây lệch
            # do baseline calibrate riêng dễ trôi theo thời gian.
            a_rms = safe_float(payload.get("vib_rms"))

            # [FIX] Phát hiện ESP32 reboot qua ts_ms -> tự recalibrate offset rung
            ts_ms = payload.get("ts_ms")
            detect_esp32_reboot(device_id, ts_ms)

            ai       = get_prediction_and_probs(temp, current, accel_x, accel_y, accel_z)
            ai_label = ai.get("label", "Unknown")

            # ── Override AI label nếu sensor vượt ngưỡng ──────────────────────────
            if (temp    is not None and temp    >= THRESHOLD_TEMP)      or                (a_rms   is not None and a_rms   >= THRESHOLD_VIBRATION) or                (current is not None and current >= THRESHOLD_CURRENT):
                ai_label = "FAULT"
            elif (temp    is not None and temp    >= THRESHOLD_TEMP_WARN)      or                  (a_rms   is not None and a_rms   >= THRESHOLD_VIB_WARN)       or                  (current is not None and current >= THRESHOLD_CURRENT_WARN):
                if ai_label.upper() == "NORMAL":
                    ai_label = "WARNING"

            # ── Logic phân cấp WARNING / DANGER (dùng chung cho status + email) ──
            is_danger = (
                (temp    is not None and temp    >= THRESHOLD_TEMP)        or
                (a_rms   is not None and a_rms   >= THRESHOLD_VIBRATION)   or
                (current is not None and current >= THRESHOLD_CURRENT)     or
                ai_label.upper() == "FAULT"
            )
            is_warning = (
                (temp    is not None and temp    >= THRESHOLD_TEMP_WARN)   or
                (a_rms   is not None and a_rms   >= THRESHOLD_VIB_WARN)    or
                (current is not None and current >= THRESHOLD_CURRENT_WARN) or
                ai_label.upper() == "WARNING"
            )

            # Sửa lỗi 1: status lưu DB phải đồng bộ với ngưỡng WARNING/DANGER,
            # không chỉ phụ thuộc vào nhãn AI (nếu AI vẫn báo NORMAL nhưng
            # nhiệt độ/dòng/độ rung đã vượt ngưỡng thì vẫn phải ghi WARNING/DANGER)
            # [FIX] Ngưỡng cứng luôn thắng AI — 1 trong 3 yếu tố vượt ngưỡng là đủ
            if is_danger:
                status = "DANGER"
            elif is_warning:
                status = "WARNING"
            else:
                status = "NORMAL"

            save_extended_telemetry(device_id, temp, a_rms, current,
                                    accel_x, accel_y, accel_z, failure=status)

            if is_danger:
                send_alert_email(device_id, temp, a_rms, current,
                                 accel_x, accel_y, accel_z, alert_level="DANGER")
            elif is_warning:
                send_alert_email(device_id, temp, a_rms, current,
                                 accel_x, accel_y, accel_z, alert_level="WARNING")

            print(f"[MQTT] {device_id} | T={temp} | I={current} "
                  f"| RMS={a_rms:.3f} g | AI={ai_label} "
                  f"| probs={ai.get('probabilities')}")

        # ── wifi_status ───────────────────────────────────────
        elif "wifi_status" in topic:
            connected = bool(payload.get("connected", False))
            ssid      = payload.get("ssid", "") or ""
            rssi      = payload.get("rssi")
            try:
                rssi = int(rssi) if rssi is not None else None
            except Exception:
                rssi = None
            save_wifi_status(device_id, connected, ssid, rssi)
            print(f"[MQTT] wifi_status {device_id} | ssid={ssid} | rssi={rssi}")

    except Exception:
        print("[MQTT] on_message error:")
        traceback.print_exc()


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ MQTT ket noi thanh cong!")
        client.subscribe("devices/+/telemetry")
        client.subscribe("devices/+/wifi_status")
        client.subscribe("devices/+/device_status")
        print("✅ Subscribed: telemetry | wifi_status | device_status")
    else:
        print(f"❌ MQTT ket noi that bai, ma loi: {rc}")


def start_mqtt_loop():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
    client.on_connect = on_connect
    client.on_message = on_message
    try:
        print(f"Dang ket noi toi Broker: {BROKER_URI}...")
        client.connect(BROKER_URI, BROKER_PORT, 60)
        client.loop_forever()
    except Exception as e:
        print(f"❌ Loi vong lap MQTT: {e}")


# ============================================================
#  FLASK ROUTES
# ============================================================
@app.route("/")
def index():
    try:
        return render_template("dashboard.html")
    except Exception as e:
        return f"Loi: Khong tim thay dashboard.html. Chi tiet: {e}", 404


@app.route("/api/latest")
def api_latest():
    conn = None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT device_id, temperature, current,
                   accel_x, accel_y, accel_z, a_rms, failure, timestamp
            FROM machine_data
            ORDER BY id DESC
            LIMIT 100
        """)
        rows = cur.fetchall()
        cur.close()

        now    = datetime.now(timezone.utc)
        result = []
        for r in rows:
            device_id, temp, curr, ax, ay, az, a_rms, failure, ts = r
            result.append({
                "device_id":    device_id,
                "temperature":  temp,
                "current":      curr,
                "accel_x":      ax,
                "accel_y":      ay,
                "accel_z":      az,
                "vibration_rms": a_rms,
                "status":       failure if failure else "Unknown",
                "created_at":   ts.isoformat() if ts else now.isoformat()
            })
        return jsonify(result)
    except Exception:
        print("api_latest error:")
        traceback.print_exc()
        return jsonify({"error": "internal"}), 500
    finally:
        put_conn(conn)


@app.route("/api/ai_predict")
def api_ai_predict():
    conn = None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT temperature, current, accel_x, accel_y, accel_z
            FROM machine_data
            ORDER BY id DESC LIMIT 1
        """)
        row = cur.fetchone()
        cur.close()

        if not row:
            return jsonify({"error": "No data"}), 404

        temp, current, ax, ay, az = row
        res   = get_prediction_and_probs(temp, current, ax, ay, az)
        label = res.get("label", "Unknown")
        probs = res.get("probabilities", {})

        # [FIX] Override AI label theo ngưỡng cứng — giống logic trong on_message
        if (temp    is not None and temp    >= THRESHOLD_TEMP)      or            (current is not None and current >= THRESHOLD_CURRENT):
            label = "FAULT"
            probs = {"NORMAL": 0.0, "WARNING": 0.0, "FAULT": 1.0}
        elif (temp    is not None and temp    >= THRESHOLD_TEMP_WARN) or              (current is not None and current >= THRESHOLD_CURRENT_WARN):
            if label.upper() == "NORMAL":
                label = "WARNING"
                probs = {"NORMAL": 0.0, "WARNING": 1.0, "FAULT": 0.0}

        return jsonify({
            "label": label,
            "probabilities": {
                "NORMAL":  float(probs.get("NORMAL",  0.0)),
                "WARNING": float(probs.get("WARNING", 0.0)),
                "FAULT":   float(probs.get("FAULT",   0.0))
            }
        })
    except Exception as e:
        print(f"AI Predict Error: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        put_conn(conn)


@app.route("/api/wifi_status")
def api_wifi_status():
    conn = None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT device_id, connected, ssid, rssi, timestamp
            FROM wifi_status
            ORDER BY timestamp DESC
            LIMIT 20
        """)
        rows = cur.fetchall()
        cur.close()

        now    = datetime.now(timezone.utc)
        result = []
        for r in rows:
            device_id, connected, ssid, rssi, ts = r
            elapsed = (now - ts).total_seconds() if ts else None
            result.append({
                "device_id": device_id,
                "connected": bool(connected),
                "ssid":      ssid,
                "rssi":      rssi,
                "elapsed_s": elapsed,
                "timestamp": ts.isoformat() if ts else None
            })
        return jsonify(result)
    except Exception as e:
        print(f"WiFi Status API Error: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        put_conn(conn)


@app.route("/api/stats")
def api_stats():
    conn = None
    try:
        conn = get_conn()
        cur  = conn.cursor()

        cur.execute("SELECT COUNT(DISTINCT device_id) FROM wifi_status")
        total_devices = cur.fetchone()[0] or 0

        cur.execute("""
            SELECT COUNT(DISTINCT device_id) FROM wifi_status
            WHERE connected = true AND timestamp > NOW() - INTERVAL '5 minutes'
        """)
        online_devices = cur.fetchone()[0] or 0

        cur.execute("""
            SELECT failure, COUNT(*) FROM (
                SELECT DISTINCT ON (device_id) failure
                FROM machine_data
                ORDER BY device_id, timestamp DESC
            ) latest
            GROUP BY failure
        """)
        status_counts = {row[0] or "Unknown": row[1] for row in cur.fetchall()}

        cur.execute("""
            SELECT AVG(temperature), AVG(current), AVG(a_rms)
            FROM machine_data
            WHERE timestamp > NOW() - INTERVAL '1 hour'
        """)
        avg_row = cur.fetchone()

        cur.execute("""
            SELECT COUNT(*) FROM machine_data
            WHERE failure IN ('WARNING', 'FAULT')
            AND timestamp > NOW() - INTERVAL '24 hours'
        """)
        alerts_24h = cur.fetchone()[0] or 0

        cur.close()
        return jsonify({
            "total_devices":   total_devices,
            "online_devices":  online_devices,
            "offline_devices": total_devices - online_devices,
            "alerts_24h":      alerts_24h,
            "status_counts":   status_counts,
            "averages": {
                "temperature": float(avg_row[0]) if avg_row[0] else 0.0,
                "current":     float(avg_row[1]) if avg_row[1] else 0.0,
                "vibration":   float(avg_row[2]) if avg_row[2] else 0.0
            }
        })
    except Exception as e:
        print(f"api_stats error: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        put_conn(conn)


@app.route("/api/alerts")
def api_alerts():
    conn = None
    try:
        limit     = int(request.args.get("limit", 50))
        device_id = request.args.get("device_id")

        conn = get_conn()
        cur  = conn.cursor()

        query  = """
            SELECT device_id, temperature, current, a_rms, failure, timestamp
            FROM machine_data
            WHERE failure IN ('WARNING', 'FAULT')
        """
        params = []
        if device_id:
            query += " AND device_id = %s"
            params.append(device_id)
        query += " ORDER BY timestamp DESC LIMIT %s"
        params.append(limit)

        cur.execute(query, params)
        rows = cur.fetchall()
        cur.close()

        now    = datetime.now(timezone.utc)
        result = []
        for r in rows:
            device_id, temp, curr, a_rms, failure, ts = r
            result.append({
                "device_id":    device_id,
                "temperature":  float(temp)   if temp   else None,
                "current":      float(curr)   if curr   else None,
                "vibration_rms": float(a_rms) if a_rms  else None,
                "status":       failure,
                "timestamp":    ts.isoformat() if ts else None,
                "elapsed_s":    (now - ts).total_seconds() if ts else None
            })
        return jsonify(result)
    except Exception:
        print("api_alerts error:")
        traceback.print_exc()
        return jsonify({"error": "internal"}), 500
    finally:
        put_conn(conn)


@app.route("/api/devices")
def api_devices():
    conn = None
    try:
        conn = get_conn()
        cur  = conn.cursor()

        cur.execute("""
            SELECT DISTINCT ON (device_id)
                device_id, temperature, current, a_rms, failure, timestamp
            FROM machine_data
            ORDER BY device_id, timestamp DESC
        """)
        machine_rows = cur.fetchall()

        cur.execute("""
            SELECT DISTINCT ON (device_id)
                device_id, connected, ssid, rssi, timestamp
            FROM wifi_status
            ORDER BY device_id, timestamp DESC
        """)
        wifi_rows = cur.fetchall()

        cur.close()

        devices = {}
        now     = datetime.now(timezone.utc)

        for row in machine_rows:
            device_id, temp, curr, a_rms, failure, ts = row
            devices[device_id] = {
                "device_id":    device_id,
                "temperature":  float(temp)   if temp   else None,
                "current":      float(curr)   if curr   else None,
                "vibration_rms": float(a_rms) if a_rms  else None,
                "status":       failure or "Unknown",
                "last_update":  ts.isoformat() if ts else None,
                "connected":    False,
                "ssid":         "",
                "rssi":         None
            }

        for row in wifi_rows:
            device_id, connected, ssid, rssi, ts = row
            if device_id in devices:
                devices[device_id]["connected"] = bool(connected)
                devices[device_id]["ssid"]      = ssid or ""
                devices[device_id]["rssi"]      = int(rssi) if rssi else None

        return jsonify(list(devices.values()))
    except Exception:
        print("api_devices error:")
        traceback.print_exc()
        return jsonify({"error": "internal"}), 500
    finally:
        put_conn(conn)


@app.route("/api/thresholds", methods=["GET"])
def api_get_thresholds():
    conn = None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT threshold_temp, threshold_vibration, threshold_current
            FROM thresholds WHERE id = 1
        """)
        row = cur.fetchone()
        cur.close()
        if row:
            return jsonify({
                "threshold_temp":      float(row[0]),
                "threshold_vibration": float(row[1]),
                "threshold_current":   float(row[2])
            })
        return jsonify({"error": "Thresholds not found"}), 404
    except Exception as e:
        print(f"Error fetching thresholds: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        put_conn(conn)


@app.route("/api/thresholds", methods=["PUT", "POST"])
def api_update_thresholds():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Invalid JSON"}), 400

        temp      = data.get("threshold_temp")
        vibration = data.get("threshold_vibration")
        current   = data.get("threshold_current")

        if temp is None or vibration is None or current is None:
            return jsonify({"error": "Missing: threshold_temp, threshold_vibration, threshold_current"}), 400

        if not isinstance(temp,      (int, float)) or not (0 <= temp      <= 200):
            return jsonify({"error": "Invalid threshold_temp (0-200)"}), 400
        if not isinstance(vibration, (int, float)) or not (0 <= vibration <= 50):
            return jsonify({"error": "Invalid threshold_vibration (0-50 g)"}), 400
        if not isinstance(current,   (int, float)) or not (0 <= current   <= 100):
            return jsonify({"error": "Invalid threshold_current (0-100)"}), 400

        success = save_thresholds(temp, vibration, current)
        if success:
            return jsonify({
                "success":              True,
                "message":              "Thresholds updated successfully",
                "threshold_temp":       THRESHOLD_TEMP,
                "threshold_vibration":  THRESHOLD_VIBRATION,
                "threshold_current":    THRESHOLD_CURRENT
            })
        return jsonify({"error": "Failed to save thresholds"}), 500
    except Exception as e:
        print("api_update_thresholds error:")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/device_status")
def api_device_status():
    conn = None
    try:
        device_id = request.args.get("device_id")
        conn = get_conn()
        cur  = conn.cursor()

        if device_id:
            cur.execute("""
                SELECT device_id, buzzer, relay, timestamp
                FROM device_status
                WHERE device_id = %s
                ORDER BY timestamp DESC LIMIT 1
            """, (device_id,))
        else:
            cur.execute("""
                SELECT DISTINCT ON (device_id) device_id, buzzer, relay, timestamp
                FROM device_status
                ORDER BY device_id, timestamp DESC
            """)

        rows   = cur.fetchall()
        cur.close()
        result = []
        for row in rows:
            device_id_val, buzzer, relay, ts = row
            result.append({
                "device_id": device_id_val,
                "buzzer":    bool(buzzer),
                "relay":     bool(relay),
                "timestamp": ts.isoformat() if ts else None
            })

        if device_id and len(result) == 1:
            return jsonify(result[0])
        return jsonify(result)
    except Exception as e:
        print(f"api_device_status error: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        put_conn(conn)


@app.route("/api/control", methods=["POST"])
def api_control():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Invalid JSON"}), 400

        device_id    = data.get("device_id")
        control_type = data.get("control_type")
        state        = data.get("state")

        if not device_id or not control_type or state is None:
            return jsonify({"error": "Missing: device_id, control_type, state"}), 400
        if control_type not in ["buzzer", "relay"]:
            return jsonify({"error": "Invalid control_type. Must be 'buzzer' or 'relay'"}), 400
        if not isinstance(state, bool):
            return jsonify({"error": "Invalid state. Must be true or false"}), 400

        success = publish_control_command(device_id, control_type, state)
        if success:
            return jsonify({
                "success":      True,
                "message":      f"Control command sent: {control_type} -> {'ON' if state else 'OFF'}",
                "device_id":    device_id,
                "control_type": control_type,
                "state":        state
            })
        return jsonify({"error": "Failed to publish control command via MQTT"}), 500
    except Exception as e:
        print("api_control error:")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/config/wifi", methods=["POST"])
def api_config_wifi():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Invalid JSON"}), 400

        device_id       = data.get("device_id")
        ssid            = data.get("ssid")
        password        = data.get("password")
        mqtt_broker_ip  = data.get("mqtt_broker_ip")
        mqtt_broker_port = data.get("mqtt_broker_port", 1883)

        if not device_id or not ssid or not password or not mqtt_broker_ip:
            return jsonify({"error": "Missing: device_id, ssid, password, mqtt_broker_ip"}), 400
        if len(ssid) > 32:
            return jsonify({"error": "SSID too long (max 32)"}), 400
        if len(password) > 64:
            return jsonify({"error": "Password too long (max 64)"}), 400

        ip_pattern = re.compile(r'^([0-9]{1,3}\.){3}[0-9]{1,3}$')
        if not ip_pattern.match(mqtt_broker_ip):
            return jsonify({"error": "Invalid MQTT broker IP format"}), 400

        success = publish_wifi_config(device_id, ssid, password,
                                      mqtt_broker_ip, mqtt_broker_port)
        if success:
            return jsonify({
                "success":      True,
                "message":      f"WiFi configuration sent to {device_id}",
                "device_id":    device_id,
                "ssid":         ssid,
                "mqtt_broker":  f"{mqtt_broker_ip}:{mqtt_broker_port}"
            })
        return jsonify({"error": "Failed to publish WiFi config via MQTT"}), 500
    except Exception as e:
        print("api_config_wifi error:")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/history")
def api_history():
    conn = None
    try:
        device_id = request.args.get("device_id")
        hours     = int(request.args.get("hours", 24))
        limit     = int(request.args.get("limit", 1000))

        conn  = get_conn()
        cur   = conn.cursor()
        query = """
            SELECT device_id, temperature, current,
                   accel_x, accel_y, accel_z, a_rms, failure, timestamp
            FROM machine_data
            WHERE timestamp > NOW() - INTERVAL '%s hours'
        """ % hours

        params = []
        if device_id:
            query += " AND device_id = %s"
            params.append(device_id)
        query += " ORDER BY timestamp DESC LIMIT %s"
        params.append(limit)

        cur.execute(query, params)
        rows = cur.fetchall()
        cur.close()

        now    = datetime.now(timezone.utc)
        result = []
        for r in rows:
            device_id, temp, curr, ax, ay, az, a_rms, failure, ts = r
            result.append({
                "device_id":    device_id,
                "temperature":  float(temp)   if temp   else None,
                "current":      float(curr)   if curr   else None,
                "accel_x":      float(ax)     if ax     else None,
                "accel_y":      float(ay)     if ay     else None,
                "accel_z":      float(az)     if az     else None,
                "vibration_rms": float(a_rms) if a_rms  else None,
                "status":       failure if failure else "Unknown",
                "timestamp":    ts.isoformat() if ts else now.isoformat()
            })
        return jsonify(result)
    except Exception as e:
        print(f"api_history error: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        put_conn(conn)


# ============================================================
#  MAIN
# ============================================================
if __name__ == "__main__":
    print("Starting server...")
    load_model()
    get_db_pool()        # Khởi tạo connection pool sớm
    init_database()
    load_thresholds()
    init_mqtt_publisher()

    mqtt_thread = threading.Thread(target=start_mqtt_loop, daemon=True)
    mqtt_thread.start()

    print("Flask running on http://0.0.0.0:5000")
    app.run(host="0.0.0.0", port=5000, debug=False)