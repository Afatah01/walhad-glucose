"""
WALHAD GLUCOSE MONITOR - Standalone server (no Replit)
Fetches Freestyle Libre glucose via LibreLinkUp, shows a live dashboard,
sends Pushover siren alerts, Telegram messages, and queues SMS + location
requests for the father's phone bridge (walhad_phone.py).
"""
import os
import time
import json
import sqlite3
import threading
import logging
from datetime import datetime, timezone

import requests
from flask import Flask, request, jsonify, Response

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("walhad")

# ---------------- Config (set as environment variables) ----------------
LLU_EMAIL = os.environ.get("LLU_EMAIL", "")
LLU_PASS = os.environ.get("LLU_PASS", "")
LLU_REGION = os.environ.get("LLU_REGION", "")  # optional, auto-detected
PUSHOVER_USER = os.environ.get("PUSHOVER_USER", "")
PUSHOVER_TOKEN = os.environ.get("PUSHOVER_TOKEN", "")
PUSHOVER_SOUND = os.environ.get("PUSHOVER_SOUND", "Walhad_Emergency")
TG_TOKEN = os.environ.get("TG_TOKEN", "")
TG_CHAT = os.environ.get("TG_CHAT", "")
SMS_NUMBERS = [n.strip() for n in os.environ.get("SMS_NUMBERS", "").split(",") if n.strip()]
LOW = float(os.environ.get("LOW_THRESHOLD", "70"))
HIGH = float(os.environ.get("HIGH_THRESHOLD", "180"))
ALERT_COOLDOWN = 300          # seconds between repeated alerts of same type
LOCATION_INTERVAL = 1200      # request GPS every 20 min
FETCH_INTERVAL = 60           # poll LibreLinkUp every 60 s
WATCHDOG_TIMEOUT = 300        # phone silent for 5 min -> warn
WATCHDOG_COOLDOWN = 1800      # repeat warning at most every 30 min
DB = os.environ.get("DB_PATH", "walhad.db")

app = Flask(__name__)
lock = threading.Lock()

state = {
    "glucose": None, "trend": None, "reading_time": None,
    "battery": None, "battery_time": None,
    "lat": None, "lon": None, "location_time": None,
    "last_phone_seen": None,
    "last_low_alert": 0, "last_high_alert": 0,
    "last_location_request": 0, "location_pending": False,
    "last_watchdog_warn": 0,
    "llu_region": LLU_REGION or None,
    "llu_token": None, "llu_patient": None, "llu_token_time": 0,
    "last_error": None, "fetch_count": 0,
}

# ---------------- Database (bulletproof SMS queue) ----------------
def db():
    con = sqlite3.connect(DB, timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    return con

with db() as con:
    con.execute("""CREATE TABLE IF NOT EXISTS sms_queue(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        to_number TEXT, msg TEXT,
        created TEXT, delivered INTEGER DEFAULT 0)""")

def queue_sms(numbers, msg):
    with lock, db() as con:
        for n in numbers:
            con.execute("INSERT INTO sms_queue(to_number,msg,created) VALUES(?,?,?)",
                        (n, msg, datetime.now(timezone.utc).isoformat()))
    log.info(f"Queued SMS to {len(numbers)} numbers")

# ---------------- LibreLinkUp ----------------
REGIONS = ["eu", "us", "la", "ae", "ap", "au"]

def llu_login(region):
    url = f"https://api-{region}.librelinkup.com/llu/auth/login"
    r = requests.post(url, json={"email": LLU_EMAIL, "password": LLU_PASS},
                      headers={"version": "4.7.0", "product": "llu.android"}, timeout=20)
    r.raise_for_status()
    data = r.json().get("data", {})
    # server may redirect to correct region
    redir = data.get("redirect")
    if redir and data.get("region"):
        return llu_login(data["region"])
    token = data.get("authTicket", {}).get("token")
    if not token:
        raise RuntimeError("no token")
    state["llu_region"] = region
    state["llu_token"] = token
    state["llu_token_time"] = time.time()
    log.info(f"LibreLinkUp login OK (region {region})")

def llu_headers():
    return {"authorization": f"Bearer {state['llu_token']}",
            "version": "4.7.0", "product": "llu.android"}

def llu_get_patient():
    url = f"https://api-{state['llu_region']}.librelinkup.com/llu/connections"
    r = requests.get(url, headers=llu_headers(), timeout=20)
    r.raise_for_status()
    conns = r.json().get("data", [])
    if conns:
        state["llu_patient"] = conns[0]["patientId"]
        log.info("LibreLinkUp patient found")

def fetch_glucose():
    """Return (value_mgdl, trend, reading_time) or None."""
    try:
        state["fetch_count"] = state.get("fetch_count", 0) + 1
        if not LLU_EMAIL or not LLU_PASS:
            return None
        if not state["llu_token"] or time.time() - state["llu_token_time"] > 3300:
            if state["llu_region"]:
                llu_login(state["llu_region"])
            else:
                for reg in REGIONS:
                    try:
                        llu_login(reg)
                        break
                    except Exception:
                        continue
            if not state["llu_token"]:
                return None
            if state["llu_region"] and not state["llu_patient"]:
                llu_get_patient()
        url = (f"https://api-{state['llu_region']}.librelinkup.com"
               f"/llu/connections/{state['llu_patient']}/graph")
        r = requests.get(url, headers=llu_headers(), timeout=20)
        if r.status_code in (401, 403):
            state["llu_token"] = None
            return None
        r.raise_for_status()
        m = r.json()["data"]["connection"]["glucoseMeasurement"]
        value = float(m["Value"])          # mg/dL
        trend = m.get("TrendArrow")
        ts = m.get("Timestamp", "")
        return value, trend, ts
    except Exception as e:
        state["last_error"] = f"{type(e).__name__}: {e}"
        log.warning(f"glucose fetch failed: {e}")
        return None

# ---------------- Notifications ----------------
def pushover(title, message, priority=1):
    if not PUSHOVER_USER or not PUSHOVER_TOKEN:
        return
    try:
        payload = {"token": PUSHOVER_TOKEN, "user": PUSHOVER_USER,
                   "title": title, "message": message, "priority": priority}
        if priority == 2:
            payload.update({"retry": 30, "expire": 600, "sound": PUSHOVER_SOUND})
        r = requests.post("https://api.pushover.net/1/messages.json",
                          data=payload, timeout=20)
        log.info(f"Pushover p{priority}: {r.status_code}")
    except Exception as e:
        log.warning(f"Pushover failed: {e}")

def telegram(message):
    if not TG_TOKEN or not TG_CHAT:
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                      json={"chat_id": TG_CHAT, "text": message}, timeout=20)
    except Exception as e:
        log.warning(f"Telegram failed: {e}")

def fire_alert(kind, value):
    """kind: 'LOW' or 'HIGH'. SMS first (bulletproof), then Pushover+Telegram."""
    msg = (f"WALHAD GLUCOSE {kind}: {value:.0f} mg/dL. "
           + ("Please give him something sweet now." if kind == "LOW"
              else "Please check on him now."))
    queue_sms(SMS_NUMBERS, msg)                      # 1. SMS - never lost
    state["location_pending"] = True                 # 2. ask phone for GPS
    pushover(f"GLUCOSE {kind}: {value:.0f}", msg, priority=2)   # 3. siren
    telegram(msg)                                    # 4. telegram

# ---------------- Background loops ----------------
def glucose_loop():
    while True:
        res = fetch_glucose()
        if res:
            value, trend, ts = res
            state.update(glucose=value, trend=trend, reading_time=ts)
            now = time.time()
            if value <= LOW and now - state["last_low_alert"] > ALERT_COOLDOWN:
                state["last_low_alert"] = now
                fire_alert("LOW", value)
            elif value >= HIGH and now - state["last_high_alert"] > ALERT_COOLDOWN:
                state["last_high_alert"] = now
                fire_alert("HIGH", value)
        # periodic GPS request every 20 min
        if time.time() - state["last_location_request"] > LOCATION_INTERVAL:
            state["last_location_request"] = time.time()
            state["location_pending"] = True
        time.sleep(FETCH_INTERVAL)

def watchdog_loop():
    while True:
        time.sleep(60)
        seen = state["last_phone_seen"]
        if seen and time.time() - seen > WATCHDOG_TIMEOUT:
            if time.time() - state["last_watchdog_warn"] > WATCHDOG_COOLDOWN:
                state["last_watchdog_warn"] = time.time()
                pushover("Phone bridge silent",
                         "Father's phone has not reported for 5+ minutes. "
                         "Check Termux is running and the phone is charged.",
                         priority=1)
                telegram("WARNING: Father's phone bridge is silent (5+ min).")

# ---------------- Phone bridge API ----------------
@app.route("/phone/update", methods=["POST"])
def phone_update():
    d = request.get_json(force=True, silent=True) or {}
    state["last_phone_seen"] = time.time()
    if d.get("battery") is not None:
        state["battery"] = d["battery"]
        state["battery_time"] = datetime.now(timezone.utc).isoformat()
    loc = d.get("location") or {}
    if loc.get("lat") and loc.get("lon"):
        state["lat"], state["lon"] = loc["lat"], loc["lon"]
        state["location_time"] = datetime.now(timezone.utc).isoformat()
        state["location_pending"] = False
    return jsonify(ok=True)

@app.route("/phone/tasks", methods=["POST", "GET"])
def phone_tasks():
    state["last_phone_seen"] = time.time()
    with lock, db() as con:
        rows = con.execute(
            "SELECT id,to_number,msg FROM sms_queue WHERE delivered=0 LIMIT 20"
        ).fetchall()
    tasks = {"sms": [{"id": r[0], "to": r[1], "msg": r[2]} for r in rows],
             "needLocation": bool(state["location_pending"])}
    return jsonify(tasks)

@app.route("/phone/sms_done", methods=["POST"])
def sms_done():
    d = request.get_json(force=True, silent=True) or {}
    ids = d.get("ids", [])
    with lock, db() as con:
        con.executemany("UPDATE sms_queue SET delivered=1 WHERE id=?",
                        [(i,) for i in ids])
    return jsonify(ok=True)

# ---------------- Dashboard data ----------------
@app.route("/api/data")
def api_data():
    return jsonify({
        "glucose": state["glucose"], "trend": state["trend"],
        "reading_time": state["reading_time"],
        "low": LOW, "high": HIGH,
        "battery": state["battery"], "battery_time": state["battery_time"],
        "lat": state["lat"], "lon": state["lon"],
        "location_time": state["location_time"],
        "phone_online": bool(state["last_phone_seen"]
                             and time.time() - state["last_phone_seen"] < 180),
        "server_time": datetime.now(timezone.utc).isoformat(),
    })

DASHBOARD = """<!doctype html><html><head>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>Walhad Glucose Monitor</title>
<style>
body{background:#0b0e14;color:#fff;font-family:system-ui;text-align:center;margin:0;padding:24px}
h1{font-size:20px;color:#9aa}
#num{font-size:110px;font-weight:800;margin:10px 0}
.g{color:#2ecc71}.y{color:#f1c40f}.r{color:#e74c3c}
.card{background:#151a23;border-radius:16px;padding:16px;margin:12px auto;max-width:360px}
.lbl{color:#9aa;font-size:14px}.val{font-size:26px;font-weight:700;margin-top:4px}
a{color:#4da3ff}
</style></head><body>
<h1>WALHAD GLUCOSE MONITOR</h1>
<div id=num>--</div>
<div class=lbl id=trend></div>
<div class=card><div class=lbl>&#128190; Current</div><div class=val id=cur>--</div></div>
<div class=card><div class=lbl>&#9200; Reading Time</div><div class=val id=rt style=font-size:18px>--</div></div>
<div class=card><div class=lbl>&#128267; Phone Battery</div><div class=val id=bat>--</div></div>
<div class=card><div class=lbl>&#128205; Last Location</div><div class=val id=loc style=font-size:18px>--</div></div>
<div class=card><div class=lbl>&#128241; Phone Bridge</div><div class=val id=br>--</div></div>
<div class=lbl>Low alert 70 &nbsp;&bull;&nbsp; High alert 180</div>
<script>
async function tick(){
 try{
  const d=await (await fetch('/api/data')).json();
  const n=document.getElementById('num');
  if(d.glucose==null){n.textContent='--';}
  else{
   n.textContent=Math.round(d.glucose);
   n.className=d.glucose<=d.low||d.glucose>=d.high?'r':'g';
   document.getElementById('cur').textContent=Math.round(d.glucose);
   document.getElementById('trend').textContent='Trend: '+(d.trend??'--');
  }
  document.getElementById('rt').textContent=d.reading_time||'--';
  document.getElementById('bat').innerHTML=d.battery==null?'--':'&#128267; '+d.battery+'%';
  if(d.lat){document.getElementById('loc').innerHTML=
    '<a href="https://maps.google.com/?q='+d.lat+','+d.lon+'">Open map</a> &middot; '+(d.location_time||'');}
  const br=document.getElementById('br');
  br.textContent=d.phone_online?'Online':'OFFLINE';
  br.className='val '+(d.phone_online?'g':'r');
 }catch(e){}
}
tick();setInterval(tick,15000);
</script></body></html>"""

@app.route("/")
def index():
    return Response(DASHBOARD, mimetype="text/html")

@app.route("/health")
def health():
    return "ok"

@app.route("/debug")
def debug():
    return jsonify({
        "fetch_count": state.get("fetch_count", 0),
        "llu_region": state["llu_region"],
        "logged_in": bool(state["llu_token"]),
        "patient_found": bool(state["llu_patient"]),
        "last_error": state["last_error"],
        "glucose": state["glucose"],
        "email_set": bool(LLU_EMAIL),
        "pass_set": bool(LLU_PASS),
        "pushover_set": bool(PUSHOVER_USER and PUSHOVER_TOKEN),
        "telegram_set": bool(TG_TOKEN and TG_CHAT),
        "sms_numbers_count": len(SMS_NUMBERS),
    })

# start background workers at import (works with gunicorn too)
threading.Thread(target=glucose_loop, daemon=True).start()
threading.Thread(target=watchdog_loop, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
