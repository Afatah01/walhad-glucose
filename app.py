"""
WALHAD GLUCOSE MONITOR - Standalone server (no Replit)
Fetches Freestyle Libre glucose via LibreLinkUp, shows a live dashboard,
sends Pushover siren alerts, Telegram messages, and queues SMS + location
requests for the father's phone bridge (walhad_phone.py).
"""
import os
import time
import json
import base64
import hashlib
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
    "last_watchdog_warn": 0, "last_phone_glucose": 0,
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

def sms_counts():
    try:
        with db() as con:
            p_ = con.execute("SELECT COUNT(*) FROM sms_queue WHERE delivered=0").fetchone()[0]
            d_ = con.execute("SELECT COUNT(*) FROM sms_queue WHERE delivered=1").fetchone()[0]
        return p_, d_
    except Exception:
        return 0, 0

# ---------------- LibreLinkUp ----------------
# Abbott retired api-*.librelinkup.com; API now lives on libreview.io
REGIONS = ["eu", "", "us", "de", "la", "ae", "ap", "au"]

def api_host(reg):
    return f"https://api-{reg}.libreview.io" if reg else "https://api.libreview.io"

def llu_login(region):
    url = api_host(region) + "/llu/auth/login"
    r = requests.post(url, json={"email": LLU_EMAIL, "password": LLU_PASS},
                      headers={"version": "4.16.0", "product": "llu.android"}, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:150]}")
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
    uid = data.get("user", {}).get("id", "")
    state["llu_account_id"] = hashlib.sha256(uid.encode()).hexdigest()
    log.info(f"LibreLinkUp login OK (region {region})")

def llu_headers():
    return {"authorization": f"Bearer {state['llu_token']}",
            "version": "4.16.0", "product": "llu.android",
            "account-id": state.get("llu_account_id", "")}

def llu_get_patient():
    url = api_host(state["llu_region"]) + "/llu/connections"
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
                errors = []
                for reg in REGIONS:
                    state["last_error"] = f"trying region {reg} ..."
                    try:
                        llu_login(reg)
                        break
                    except Exception as e:
                        errors.append(f"{reg}: {e}")
                        state["last_error"] = f"{reg} failed, trying next..."
                        continue
                if not state["llu_token"]:
                    state["last_error"] = "LOGIN FAILED -> " + " | ".join(errors)[:500]
            if not state["llu_token"]:
                return None
            if state["llu_region"] and not state["llu_patient"]:
                llu_get_patient()
        url = (api_host(state["llu_region"])
               + f"/llu/connections/{state['llu_patient']}/graph")
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
    state["last_alert"] = {"kind": kind, "value": round(value), "time": time.time()}
    queue_sms(SMS_NUMBERS, msg)                      # 1. SMS - never lost
    state["location_pending"] = True                 # 2. ask phone for GPS
    pushover(f"GLUCOSE {kind}: {value:.0f}", msg, priority=2)   # 3. siren
    telegram(msg)                                    # 4. telegram

def handle_reading(value, trend, ts):
    """Store a glucose reading and fire threshold alerts."""
    state.update(glucose=value, trend=trend, reading_time=ts)
    now = time.time()
    if value <= LOW and now - state["last_low_alert"] > ALERT_COOLDOWN:
        state["last_low_alert"] = now
        fire_alert("LOW", value)
    elif value >= HIGH and now - state["last_high_alert"] > ALERT_COOLDOWN:
        state["last_high_alert"] = now
        fire_alert("HIGH", value)

def phone_glucose_fresh():
    """True if the father's phone sent glucose in the last 3 minutes."""
    t = state.get("last_phone_glucose", 0)
    return bool(t and time.time() - t < 180)

# ---------------- Background loops ----------------
def glucose_loop():
    while True:
        # Plan B: father's phone fetches LibreLinkUp directly (Render's IPs
        # are blocked by Abbott). Only fetch from here as a fallback when the
        # phone has not reported glucose recently.
        if not phone_glucose_fresh():
            res = fetch_glucose()
            if res:
                value, trend, ts = res
                handle_reading(value, trend, ts)
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

@app.route("/phone/glucose", methods=["POST"])
def phone_glucose():
    """Father's phone reads LibreLinkUp directly and posts readings here."""
    d = request.get_json(force=True, silent=True) or {}
    value = d.get("glucose")
    if value is None:
        return jsonify(ok=False, error="no glucose"), 400
    state["last_phone_seen"] = time.time()
    state["last_phone_glucose"] = time.time()
    state["last_error"] = None
    handle_reading(float(value), d.get("trend"),
                   d.get("reading_time") or datetime.now(timezone.utc).isoformat())
    return jsonify(ok=True)


# ---------------- PWA (installable app) + extras ----------------
LOGO_B64 = {"192": "iVBORw0KGgoAAAANSUhEUgAAAMAAAADACAMAAABlApw1AAAAwFBMVEUhXmyhEizf4N7ekJ5ylaC0VmjeGznhbICYsrilu8KoKkJVY3I5dYG6d4O+0Nb3+vvU1tnm6OrtHDzZ3eHNzczc4uXtIkHvHUDrID7m19voyM/lp7MUV2bjd4niNlHkl6XhV23iZnqmCCfo3eHmusPkh5fpDi+qEzDhQ1zksrziTGMsZnPcKUXVhpTeV2veN1GbABrZp7GbByTTd4bcusNKeIjeRVzeS2LamKVYg5G1VmfcZXipN02MqLDbtLyzxstWn/jLAAAAQHRSTlP/////////////////////////////////////////////////////////////////////////////////////73leyQAAHZ9JREFUeNrtXXd327iWd+zUmd0lQPBSLFGziiUntuOSPkm+/7daAES5AC4oOc7seX+szjtvGJoC8cPtBdAJY6xihfzwSl5xdSVvMH2rcLe4u8WYuaKeZ498vnr68yf/gQCq/wfwhwFU/zaAZEIsnRAbAxD88feer459PgFQyY/5gvwYxPIzILa3uLvFqOfJFUqf9ytapc+PjV/Z59P5nPDf/jD+n/A5iZH5lWAjK3ckZUZWrhp//ljKVCePEb4DvF6NCTd75PjVkbJx8lva4M8I62MBkPM5eSziw9qDPVI7sd+mgAeAeY4lMsBGedS+gNROVVY7VUjGxmSGmo8DUBkAPFwhtxyn6S029jz7l5/nxB9PSGTkyg23tO5yV+4C9BW4WwD+CoYrdxEOwaNbgmKVmDKnTjudHCms3MwF7FyGi2hCYJFwDMUDgMMA/KiE7D3aF1J/BDWJYQZMCGHuC+bfJ+8zc1+Y++DeKf/EICQ92FuFsM9z4aA7TOyPOHOCq5UTw4vl++yLAQMQFpifEMO3wN5i8TjyMgdA3XwyBYQehgAAEFLAUSZRlxkKOIpZAHZoyXxguargxe8BcF+IVs692K20A8BC1pKzTYnCzFMcs5AHYGWGu1uGFF6IiQXyapQ54jFmIYCVQCS/kuzDigaUEdGEHFEA3zIASBaKJZyvzNWp56ocgMGz8o8pWugH0Yratystl7DEiMXNPA/x8278APBw5WhNGMQTz2UBgEirYABeKNGEhJstd390XJXKgMgCkFRFRIx0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", "512": "iVBORw0KGgoAAAANSUhEUgAAAgAAAAIACAMAAADDpiTIAAAAwFBMVEXbmqaqU2Lcc4VvkpufNUknWGXe4N+XrrVNZnKUJTmhu8K7dYPXDS7fXXG809l8BxvkME0pO0lYP1Xdt8D4+/zU1tnuHj7m6OvZ3ODNzc3c4eXvHUDuIUDtID7UGjfs3OLt1twZW2qnDSvPIjz0yNHLN07NKUOpEC3TVmrORFnxusTrp7TZd4fWZnjnmafjiJeUCSPZhpUcYG8UTFrusr3RTGJVgZJQeong4N6NBB2Ipq6NFiwXUV0vY2+vx82tV2ZJ/pUNAAAAQHRSTlP///////////////////////////7+////////////////////////////////////////////////////////gkKQUAAAhkVJREFUeNrtvYuC2zaSBdpxbCeZ2dnde68ggG86pGS92510T2zHdv7/ry7xBoEqEqSotj0bTuKJWy2RIg4L9Th16o7YIyEr56CJ8wp1XyHOK723dK8k8Ft6H9Z/y6Lnx95Co97yfvL5Yy857vz9DyOTz79Kksnnv/sbAPYtqfeW5KsCYMb3n3H+hQFArgEA+U8CAPm/CYDkuwYAXXALIH9bgO9kC0hwC/Dt+wDfJQDIt2UBnNV8jbwl+Q8AQPJdA4A8DwDIN2UBkhlRwHcBgORbBQBB79ktAZD87QR+M1sAvfL8zw+AWAvw/tkAQL5nJ/A/eQv4GwA3cUKXAECyIACS7wEA32wUQL4/C0C+Rwtwuyfw2kTQkgAgz2IBvj4AkmcxweR5MoF/A+BqACRL5gGS/xt5gG8cAMk0CxDxbcAFSCAAGAjc8AZc5wMsEQYO3TJ1DyZFAck0ACTOES6APrwLcA7PC435sBX2YdhbCHh+/lXh84OZsPnnJwPfP3jLwAJcccvizr9C3/Ie/f539O/j//Rxdy2CyLMgmHyz50ff8hXOP8eC32GJBPI1cvkOB8M7P5nhxOlvk/jnJxEb8vR6fgCACELJLaMYvJroHnfTc8lLZvLCJ2AcAEk0AIjckZOb5hGME3tLAJCbJZLurvaiF81koRaIzABg8tyJJAgAA1FUEv9h758JAM+Vy08mm2DyrRZT5lQTyZJh7HcJgK+cySPLbwF/A+B7AsDzbAHkeQC4LAC+MqNl0VoCuQoA5G8AfCfl3CVJ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"}

@app.route("/icons/icon-<int:size>.png")
def app_icon(size):
    return Response(base64.b64decode(LOGO_B64.get(str(size), LOGO_B64["192"])),
                    mimetype="image/png")

@app.route("/manifest.json")
def web_manifest():
    return jsonify({
        "name": "Walhad Glucose Monitor",
        "short_name": "Walhad",
        "description": "Family Dashboard - live glucose monitoring for Siciid Walhad",
        "start_url": "/", "display": "standalone",
        "background_color": "#f4f7f9", "theme_color": "#e63950",
        "icons": [
            {"src": "/icons/icon-192.png", "sizes": "192x192", "type": "image/png"},
            {"src": "/icons/icon-512.png", "sizes": "512x512", "type": "image/png"},
        ]})

SW_JS = """
self.addEventListener('install',e=>self.skipWaiting());
self.addEventListener('activate',e=>self.clients.claim());
self.addEventListener('fetch',e=>{
 if(e.request.url.includes('/api/')||e.request.method!=='GET')return;
 e.respondWith(caches.open('walhad-v1').then(c=>c.match(e.request).then(r=>{
  const f=fetch(e.request).then(n=>{c.put(e.request,n.clone());return n;}).catch(()=>r);
  return r||f;})));
});
"""

@app.route("/sw.js")
def service_worker():
    return Response(SW_JS, mimetype="text/javascript")

@app.route("/request_location", methods=["POST"])
def request_location():
    state["location_pending"] = True
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
        "location_pending": bool(state["location_pending"]),
        "sms_pending": sms_counts()[0], "sms_delivered": sms_counts()[1],
        "alert": (state.get("last_alert")
                  if state.get("last_alert")
                  and time.time() - state["last_alert"]["time"] < 600 else None),
        "server_time": datetime.now(timezone.utc).isoformat(),
    })

DASHBOARD = """<!doctype html><html lang=en><head>
<meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<meta name=theme-color content="#e63950">
<meta name=mobile-web-app-capable content=yes>
<meta name=apple-mobile-web-app-capable content=yes>
<meta name=apple-mobile-web-app-status-bar-style content=default>
<title>Walhad Glucose Monitor</title>
<link rel=manifest href="/manifest.json">
<link rel=icon href="/icons/icon-192.png">
<link rel=apple-touch-icon href="/icons/icon-192.png">
<style>
*{box-sizing:border-box;margin:0}
body{background:#f4f7f9;color:#1d3557;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;min-height:100vh;display:flex;flex-direction:column}
header{background:linear-gradient(135deg,#e63950,#c9184a);color:#fff;padding:18px 16px 22px;text-align:center;border-radius:0 0 28px 28px;box-shadow:0 4px 18px rgba(230,57,80,.35)}
header img{width:74px;height:74px;border-radius:50%;background:#fff;padding:6px;box-shadow:0 2px 10px rgba(0,0,0,.25)}
header h1{font-size:21px;margin-top:8px;letter-spacing:.5px}
header p{font-size:13px;opacity:.9;margin-top:2px}
#installBtn{margin-top:10px;background:#fff;color:#c9184a;border:0;border-radius:20px;padding:8px 18px;font-weight:700;font-size:13px;display:none;cursor:pointer}
main{flex:1;padding:16px;max-width:430px;margin:0 auto;width:100%}
#alertBanner{display:none;background:#e63950;color:#fff;border-radius:14px;padding:14px;text-align:center;font-weight:800;font-size:17px;margin-bottom:14px;animation:pulse 1s infinite}
@keyframes pulse{50%{opacity:.75}}
#bigCard{background:#fff;border-radius:22px;padding:26px 16px;text-align:center;box-shadow:0 3px 14px rgba(29,53,87,.08);margin-bottom:14px}
#num{font-size:120px;font-weight:800;line-height:1;background:linear-gradient(135deg,#2a9d8f,#21867a);-webkit-background-clip:text;background-clip:text;color:transparent}
#num.r{background:linear-gradient(135deg,#e63950,#b5179e);-webkit-background-clip:text;background-clip:text}
#unit{color:#8a97a8;font-size:15px;font-weight:600;margin-top:2px}
#trend{font-size:30px;margin-top:6px}
#trendLbl{color:#8a97a8;font-size:12px}
.card{background:#fff;border-radius:16px;padding:15px 18px;margin-bottom:12px;box-shadow:0 2px 10px rgba(29,53,87,.06);display:flex;align-items:center;gap:14px}
.card .ic{font-size:24px}
.card .t{flex:1}
.lbl{color:#8a97a8;font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:.5px}
.val{font-size:19px;font-weight:700;margin-top:2px}
.val.g{color:#2a9d8f}.val.r{color:#e63950}
.btn{background:#2a9d8f;color:#fff;border:0;border-radius:12px;padding:10px 16px;font-weight:700;font-size:13px;cursor:pointer;white-space:nowrap}
.btn:active{opacity:.8}
a.map{color:#2a9d8f;font-weight:700;text-decoration:none}
#thresh{text-align:center;color:#8a97a8;font-size:12px;margin:6px 0 14px}
footer{text-align:center;padding:16px;color:#8a97a8;font-size:13px;border-top:1px solid #e3e9ee;background:#fff}
footer b{color:#1d3557}
</style></head><body>
<header>
<img src="/icons/icon-192.png" alt=logo>
<h1>Walhad Glucose Monitor</h1>
<p>Family Dashboard</p>
<button id=installBtn>&#11015; Install App</button>
</header>
<main>
<div id=alertBanner></div>
<div id=bigCard>
<div id=num>--</div>
<div id=unit>mg/dL</div>
<div id=trend></div>
<div id=trendLbl></div>
</div>
<div class=card><div class=ic>&#9200;</div><div class=t><div class=lbl>Reading Time</div><div class=val id=rt style=font-size:15px>--</div></div></div>
<div class=card><div class=ic>&#128267;</div><div class=t><div class=lbl>Father&#39;s Phone Battery</div><div class=val id=bat>--</div></div></div>
<div class=card><div class=ic>&#128205;</div><div class=t><div class=lbl>Last Location</div><div class=val id=loc style=font-size:15px>--</div></div><button class=btn id=locBtn>&#8635; Refresh</button></div>
<div class=card><div class=ic>&#128241;</div><div class=t><div class=lbl>Phone Bridge</div><div class=val id=br>--</div></div></div>
<div class=card><div class=ic>&#9993;</div><div class=t><div class=lbl>SMS Status (8 family numbers)</div><div class=val id=sms style=font-size:15px>--</div></div></div>
<div id=thresh>Low alert 70 mg/dL &nbsp;&bull;&nbsp; High alert 180 mg/dL</div>
</main>
<footer>Powered by: <b>Abdifatah Elmi</b></footer>
<script>
const ARROWS={1:"&#8601;",2:"&#8595;",3:"&#8594;",4:"&#8593;",5:"&#8600;"};
const TNAMES={1:"Falling fast",2:"Falling",3:"Stable",4:"Rising",5:"Rising fast"};
let lastAlertTime=null, audioCtx=null;
function siren(){try{
 audioCtx=audioCtx||new (window.AudioContext||window.webkitAudioContext)();
 for(let i=0;i<6;i++){const o=audioCtx.createOscillator(),g=audioCtx.createGain();
  o.connect(g);g.connect(audioCtx.destination);
  o.frequency.value=i%2?880:660;const t=audioCtx.currentTime+i*0.4;
  g.gain.setValueAtTime(0.25,t);g.gain.setValueAtTime(0.0001,t+0.35);
  o.start(t);o.stop(t+0.35);}
 if(navigator.vibrate)navigator.vibrate([400,200,400,200,400]);
}catch(e){}}
async function tick(){
 try{
  const d=await (await fetch('/api/data')).json();
  const n=document.getElementById('num');
  if(d.glucose==null){n.textContent='--';}
  else{
   n.textContent=Math.round(d.glucose);
   n.className=(d.glucose<=d.low||d.glucose>=d.high)?'r':'';
   document.getElementById('trend').innerHTML=ARROWS[d.trend]||'';
   document.getElementById('trendLbl').textContent=TNAMES[d.trend]||'';
  }
  document.getElementById('rt').textContent=d.reading_time||'--';
  document.getElementById('bat').textContent=d.battery==null?'--':d.battery+'%';
  const loc=document.getElementById('loc');
  if(d.lat){loc.innerHTML='<a class=map href="https://maps.google.com/?q='+d.lat+','+d.lon+'">Open in Maps</a><div class=lbl>'+(d.location_time||'')+'</div>';}
  else loc.textContent=d.location_pending?'Requested - phone will send within ~1 min':'--';
  const br=document.getElementById('br');
  br.textContent=d.phone_online?'Online':'OFFLINE';
  br.className='val '+(d.phone_online?'g':'r');
  document.getElementById('sms').innerHTML='Sent: <b>'+d.sms_delivered+'</b> &nbsp; Waiting: <b>'+d.sms_pending+'</b>';
  const ab=document.getElementById('alertBanner');
  if(d.alert){ab.style.display='block';
   ab.textContent='\u26a0\ufe0f GLUCOSE '+d.alert.kind+': '+d.alert.value+' mg/dL';
   if(d.alert.time!==lastAlertTime){lastAlertTime=d.alert.time;siren();}
  }else ab.style.display='none';
 }catch(e){}
}
document.getElementById('locBtn').onclick=async()=>{
 const b=document.getElementById('locBtn');b.textContent='...';
 await fetch('/request_location',{method:'POST'});
 b.textContent='\u21bb Refresh';setTimeout(tick,3000);
};
let deferredPrompt=null;
window.addEventListener('beforeinstallprompt',e=>{e.preventDefault();deferredPrompt=e;
 document.getElementById('installBtn').style.display='inline-block';});
document.getElementById('installBtn').onclick=async()=>{if(deferredPrompt){deferredPrompt.prompt();}};
if('serviceWorker' in navigator)navigator.serviceWorker.register('/sw.js');
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
        "phone_glucose_fresh": phone_glucose_fresh(),
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
