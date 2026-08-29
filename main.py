import os
import json
import time
import math
import signal as os_signal
import threading
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from urllib.parse import urlencode

import numpy as np
import requests
from flask import Flask, jsonify, request
import websocket

# ============================================================
# BOT DE FUTUROS PRO - SEÑALES MANUALES (v3)
# ============================================================
# Binance USDⓈ-M Futures -> análisis + precio en vivo -> Telegram.
# NO coloca órdenes en Binance.
#
# Novedades v3:
#   - Alerta por Telegram si Binance empieza a fallar en serio
#     (errores consecutivos) o si devuelve 451 (bloqueo regional/IP).
#   - Límite de exposición TOTAL (pendientes + trades virtuales),
#     no solo de señales pendientes.
#   - Confirmación / cancelación de señales con BOTONES en el
#     mensaje de Telegram (webhook), además de los endpoints
#     manuales /confirmar y /cancelar que ya existían.
#
# Diseño principal:
#   1) REST: velas cerradas, universo, funding, OI, filtros.
#   2) WebSocket: precio BID/ASK y MARK PRICE en vivo.
#   3) Motor de señales: 1D/4H/1H/15M/5M, sin usar la vela abierta.
#   4) Señal PENDIENTE con zona de entrada y caducidad.
#   5) Vigilancia en vivo para entrada, cancelación, SL y TP.
#   6) Persistencia atómica del estado.
#
# IMPORTANTE: esto no garantiza ganancias. Es un sistema de señales.
# Probar primero en paper/demo y revisar cada señal manualmente.
# ============================================================

# ------------------------- CONFIG ----------------------------
BINANCE_URL = os.getenv("BINANCE_URL", "https://fapi.binance.com")
WS_URL = os.getenv("BINANCE_WS_URL", "wss://fstream.binance.com/market/stream")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "")  # opcional pero recomendado
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE_DIR, "estado_bot.json")
LOG_FILE = os.path.join(BASE_DIR, "bot.log")

TOTAL_MONEDAS = int(os.getenv("TOTAL_MONEDAS", "50"))
MIN_VOLUME_24H = float(os.getenv("MIN_VOLUME_24H", "20000000"))
MIN_SCORE = int(os.getenv("MIN_SCORE", "8"))
MIN_SCORE_GRADE_A = int(os.getenv("MIN_SCORE_GRADE_A", "9"))
MAX_SIGNAL_SLOTS = int(os.getenv("MAX_SIGNAL_SLOTS", "5"))

# Exposición total = señales pendientes + trades virtuales en seguimiento.
# Antes solo se limitaban las pendientes; esto evita acumular más
# posiciones virtuales de las que realmente podés seguir.
MAX_TOTAL_EXPOSURE = int(os.getenv("MAX_TOTAL_EXPOSURE", "8"))

# La señal final se mantiene válida durante este tiempo.
SIGNAL_TTL_SECONDS = int(os.getenv("SIGNAL_TTL_SECONDS", "240"))  # 4 min
PREALERT_TTL_SECONDS = int(os.getenv("PREALERT_TTL_SECONDS", "900"))

# Motor de análisis normal.
ANALYSIS_INTERVAL = int(os.getenv("ANALYSIS_INTERVAL", "180"))
UNIVERSE_REFRESH_SECONDS = int(os.getenv("UNIVERSE_REFRESH_SECONDS", "21600"))

# Entrada: el precio debe estar dentro de la zona.
ENTRY_ATR_WIDTH = float(os.getenv("ENTRY_ATR_WIDTH", "0.22"))
PREALERT_ATR = float(os.getenv("PREALERT_ATR", "1.00"))
ENTRY_MAX_CHASE_ATR = float(os.getenv("ENTRY_MAX_CHASE_ATR", "0.35"))

# Riesgo.
MAX_STOP_PCT = float(os.getenv("MAX_STOP_PCT", "2.50"))
MIN_RR_TP1 = float(os.getenv("MIN_RR_TP1", "1.50"))
MIN_RR_TP2 = float(os.getenv("MIN_RR_TP2", "2.00"))
MIN_NET_TP2 = float(os.getenv("MIN_NET_TP2", "1.50"))
MIN_LEVERAGE = int(os.getenv("MIN_LEVERAGE", "5"))
MAX_LEVERAGE = int(os.getenv("MAX_LEVERAGE", "10"))

# Comisión aproximada por lado (% del notional). Ajustar a la cuenta real.
TAKER_FEE_PCT_PER_SIDE = float(os.getenv("TAKER_FEE_PCT_PER_SIDE", "0.04"))

# Filtros de mercado.
RSI_OVERBOUGHT = 75.0
RSI_OVERSOLD = 25.0
ADX_MIN_5M = 18.0
VOLUME_MIN_5M = 0.90

# Funding expresado en porcentaje.
FUNDING_INFRA_PCT = -0.02
FUNDING_SOBRE_PCT = 0.08

# Alertas de salud de Binance (para no perder señales en silencio).
ALERT_ERROR_THRESHOLD = int(os.getenv("ALERT_ERROR_THRESHOLD", "20"))
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "900"))  # 15 min entre alertas repetidas
BLOCKED_ALERT_COOLDOWN_SECONDS = int(os.getenv("BLOCKED_ALERT_COOLDOWN_SECONDS", "60"))

# ------------------------- APP / LOG -------------------------
app = Flask(__name__)

logger = logging.getLogger("futures_bot")
logger.setLevel(logging.INFO)
if not logger.handlers:
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    logger.addHandler(stream)
    try:
        file_handler = RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=3, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception:
        pass

session = requests.Session()
session.headers.update({"User-Agent": "futures-signal-bot-pro/3.0"})

# ------------------------- ESTADO ----------------------------
state_lock = threading.RLock()
state = {
    "pending": {},       # señales que esperan entrada
    "virtual_trades": {},# operaciones que el usuario puede seguir manualmente
    "prealerts": {},
    "last_analysis": 0,
    "last_universe": 0,
    "ws_connected": False,
    "started_at": time.time(),
}

market_lock = threading.RLock()
live_market = {}  # symbol -> bid/ask/mark/last_event
symbol_filters = {}  # symbol -> tickSize
universe = []

stop_event = threading.Event()
ws_thread = None
analysis_thread = None
monitor_thread = None
ws_restart_event = threading.Event()

error_lock = threading.Lock()
consecutive_binance_errors = 0
last_binance_error = None
last_error_alert = 0.0
last_blocked_alert = 0.0


def utc_iso(ts=None):
    if ts is None:
        ts = time.time()
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def save_state():
    tmp = STATE_FILE + ".tmp"
    try:
        with state_lock:
            payload = dict(state)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, STATE_FILE)
    except Exception as exc:
        logger.exception("No se pudo guardar estado: %s", exc)


def load_state():
    global state
    if not os.path.exists(STATE_FILE):
        return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            loaded = json.load(f)
        with state_lock:
            for key in state:
                if key in loaded:
                    state[key] = loaded[key]
        logger.info("Estado recuperado: %d pendientes / %d trades", len(state["pending"]), len(state["virtual_trades"]))
    except Exception as exc:
        logger.exception("Estado corrupto/no recuperable: %s", exc)


# ------------------------- TELEGRAM --------------------------
def send_telegram(text, reply_markup=None):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram no configurado.")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": True}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        r = session.post(url, json=payload, timeout=12)
        if r.status_code != 200:
            logger.error("Telegram %s: %s", r.status_code, r.text[:300])
            return False
        return True
    except Exception as exc:
        logger.error("Telegram error: %s", exc)
        return False


def answer_callback(callback_query_id, text, show_alert=False):
    if not TELEGRAM_TOKEN or not callback_query_id:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery"
    try:
        r = session.post(url, json={
            "callback_query_id": callback_query_id,
            "text": text[:200],
            "show_alert": show_alert,
        }, timeout=8)
        return r.status_code == 200
    except Exception as exc:
        logger.warning("answerCallbackQuery error: %s", exc)
        return False


def confirm_keyboard(symbol):
    return {
        "inline_keyboard": [[
            {"text": "✅ Confirmar (precio actual)", "callback_data": f"confirm:{symbol}"},
            {"text": "❌ Cancelar", "callback_data": f"cancel:{symbol}"},
        ]]
    }


# ------------------------- ALERTAS DE SALUD -------------------
def maybe_alert_binance_down(blocked=False, status_code=None):
    """Avisa por Telegram si Binance empieza a fallar de forma sostenida,
    o inmediatamente si detecta un bloqueo regional (451/403 tipo geo-block)."""
    global last_error_alert, last_blocked_alert
    now = time.time()
    with error_lock:
        errors = consecutive_binance_errors
        last_err = last_binance_error

    if blocked:
        with error_lock:
            if now - last_blocked_alert < BLOCKED_ALERT_COOLDOWN_SECONDS:
                return
            last_blocked_alert = now
        send_telegram(
            "🚫 ALERTA BOT FUTUROS PRO\n\n"
            f"Binance devolvió {status_code} (bloqueo por región/IP del servidor).\n"
            "Reintentar no sirve: esto no se arregla solo. Hay que revisar "
            "el hosting/proxy o el endpoint de Binance usado.\n"
            "Mientras tanto el bot puede quedarse SIN analizar monedas."
        )
        return

    if errors < ALERT_ERROR_THRESHOLD:
        return
    with error_lock:
        if now - last_error_alert < ALERT_COOLDOWN_SECONDS:
            return
        last_error_alert = now
    send_telegram(
        "⚠️ ALERTA BOT FUTUROS PRO\n\n"
        f"Binance viene fallando: {errors} errores seguidos (último código: {last_err}).\n"
        "Es posible que el bot esté sin poder analizar el mercado. Revisar logs/Render."
    )


# ------------------------- BINANCE REST ----------------------
def binance_get(endpoint, params=None, retries=3):
    global consecutive_binance_errors, last_binance_error
    for attempt in range(retries):
        try:
            r = session.get(BINANCE_URL + endpoint, params=params, timeout=10)
            if r.status_code == 200:
                with error_lock:
                    consecutive_binance_errors = 0
                    last_binance_error = None
                return r.json()

            if r.status_code == 451:
                # Bloqueo geográfico/IP: reintentar es inútil, avisar ya.
                with error_lock:
                    consecutive_binance_errors += 1
                    last_binance_error = 451
                logger.error("Binance 451 (bloqueo regional) en %s", endpoint)
                maybe_alert_binance_down(blocked=True, status_code=451)
                return None

            if r.status_code in (418, 429):
                retry_after = r.headers.get("Retry-After")
                try:
                    wait = max(2, min(int(retry_after), 60))
                except (TypeError, ValueError):
                    wait = min(2 ** attempt, 30)
                logger.warning("Rate limit Binance %s; esperando %ss", r.status_code, wait)
                time.sleep(wait)
                continue

            with error_lock:
                consecutive_binance_errors += 1
                last_binance_error = r.status_code
            logger.error("Binance REST %s %s: %s", r.status_code, endpoint, r.text[:250])
            maybe_alert_binance_down()
            return None
        except Exception as exc:
            logger.warning("Binance REST intento %d/%d: %s", attempt + 1, retries, exc)
            time.sleep(min(2 ** attempt, 8))
    with error_lock:
        consecutive_binance_errors += 1
        last_binance_error = "NETWORK"
    maybe_alert_binance_down()
    return None


def get_exchange_info():
    data = binance_get("/fapi/v1/exchangeInfo")
    if not data:
        return []
    contracts = []
    filters = {}
    for item in data.get("symbols", []):
        if item.get("status") != "TRADING":
            continue
        if item.get("contractType") != "PERPETUAL":
            continue
        if item.get("quoteAsset") != "USDT":
            continue
        symbol = item.get("symbol")
        if not symbol or not symbol.endswith("USDT"):
            continue
        contracts.append(symbol)
        tick = None
        for f in item.get("filters", []):
            if f.get("filterType") == "PRICE_FILTER":
                tick = f.get("tickSize")
                break
        if tick:
            filters[symbol] = float(tick)
    symbol_filters.clear()
    symbol_filters.update(filters)
    return contracts


def get_24h_volumes():
    data = binance_get("/fapi/v1/ticker/24hr")
    result = {}
    if not data:
        return result
    for item in data:
        try:
            s = item.get("symbol")
            if s and s.endswith("USDT"):
                result[s] = float(item.get("quoteVolume", 0))
        except Exception:
            pass
    return result


def get_klines(symbol, interval, limit=150, closed_only=True):
    data = binance_get("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit})
    if not data:
        return None
    if closed_only and len(data) > 1:
        data = data[:-1]  # CRÍTICO: nunca usar vela abierta para confirmar señal.
    try:
        return [{
            "open": float(x[1]), "high": float(x[2]), "low": float(x[3]),
            "close": float(x[4]), "volume": float(x[5]), "time": int(x[0])
        } for x in data]
    except Exception:
        return None


def get_futures_context(symbol):
    premium = binance_get("/fapi/v1/premiumIndex", {"symbol": symbol})
    oi = binance_get("/fapi/v1/openInterest", {"symbol": symbol})
    funding = None
    mark = None
    if premium:
        try:
            mark = float(premium.get("markPrice"))
            funding = float(premium.get("lastFundingRate")) * 100.0
        except Exception:
            pass
    open_interest = None
    if oi:
        try:
            open_interest = float(oi.get("openInterest"))
        except Exception:
            pass
    return {"mark": mark, "funding": funding, "open_interest": open_interest}


# ------------------------- INDICADORES -----------------------
def ema_series(values, period):
    if len(values) < period:
        return []
    k = 2.0 / (period + 1.0)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def ema(values, period):
    s = ema_series(values, period)
    return s[-1] if s else None


def rsi(values, period=14):
    if len(values) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(values)):
        d = values[i] - values[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)


def stoch_rsi(values, rsi_period=14, stoch_period=14):
    if len(values) < rsi_period + stoch_period + 5:
        return 50.0
    rsis = [rsi(values[:i], rsi_period) for i in range(rsi_period + 1, len(values) + 1)]
    window = rsis[-stoch_period:]
    lo, hi = min(window), max(window)
    if hi == lo:
        return 50.0
    return (rsis[-1] - lo) / (hi - lo) * 100.0


def macd(values):
    e12, e26 = ema_series(values, 12), ema_series(values, 26)
    if not e12 or not e26:
        return 0.0, 0.0, 0.0
    line = [a - b for a, b in zip(e12, e26)]
    sig = ema_series(line, 9)
    if not sig:
        return line[-1], 0.0, 0.0
    return line[-1], sig[-1], line[-1] - sig[-1]


def atr(candles, period=14):
    if len(candles) < period + 2:
        return None
    trs = []
    for i in range(1, len(candles)):
        c, p = candles[i], candles[i - 1]
        trs.append(max(c["high"] - c["low"], abs(c["high"] - p["close"]), abs(c["low"] - p["close"])))
    return sum(trs[-period:]) / period


def adx(candles, period=14):
    if len(candles) < period + 3:
        return 0.0, 0.0, 0.0
    trs, plus, minus = [], [], []
    for i in range(1, len(candles)):
        c, p = candles[i], candles[i - 1]
        trs.append(max(c["high"] - c["low"], abs(c["high"] - p["close"]), abs(c["low"] - p["close"])))
        up = c["high"] - p["high"]
        down = p["low"] - c["low"]
        plus.append(up if up > down and up > 0 else 0.0)
        minus.append(down if down > up and down > 0 else 0.0)
    trn = sum(trs[-period:])
    if trn <= 0:
        return 0.0, 0.0, 0.0
    pdi = 100.0 * sum(plus[-period:]) / trn
    mdi = 100.0 * sum(minus[-period:]) / trn
    den = pdi + mdi
    dx = 100.0 * abs(pdi - mdi) / den if den else 0.0
    return dx, pdi, mdi


def vwap(candles, n=50):
    c = candles[-n:]
    vol = sum(x["volume"] for x in c)
    if vol <= 0:
        return None
    return sum(((x["high"] + x["low"] + x["close"]) / 3.0) * x["volume"] for x in c) / vol


def bollinger(values, period=20, deviations=2):
    if len(values) < period:
        return None, None, None
    w = np.array(values[-period:], dtype=float)
    mid = float(np.mean(w))
    sd = float(np.std(w))
    return mid + deviations * sd, mid, mid - deviations * sd


def support_resistance(candles, n=80):
    c = candles[-n:]
    return min(x["low"] for x in c), max(x["high"] for x in c)


def fibonacci(candles, n=100):
    c = candles[-n:]
    hi, lo = max(x["high"] for x in c), min(x["low"] for x in c)
    d = hi - lo
    if d <= 0:
        return {}
    return {"0.382": hi - d * .382, "0.500": hi - d * .5, "0.618": hi - d * .618, "0.786": hi - d * .786}


def volume_ratio(candles):
    if len(candles) < 25:
        return 1.0
    avg = sum(x["volume"] for x in candles[-21:-1]) / 20.0
    return candles[-1]["volume"] / avg if avg else 1.0


def trend(candles):
    closes = [x["close"] for x in candles]
    e20, e50 = ema(closes, 20), ema(closes, 50)
    if e20 is None or e50 is None:
        return "NEUTRAL"
    if closes[-1] > e20 > e50:
        return "ALCISTA"
    if closes[-1] < e20 < e50:
        return "BAJISTA"
    return "NEUTRAL"


def divergence(candles, lookback=10):
    if len(candles) < 50:
        return None
    closes = [x["close"] for x in candles]
    a = rsi(closes[:-lookback])
    b = rsi(closes)
    pa, pb = closes[-lookback], closes[-1]
    if pb < pa and b > a:
        return "ALCISTA"
    if pb > pa and b < a:
        return "BAJISTA"
    return None


def analyze_tf(candles):
    closes = [x["close"] for x in candles]
    m, ms, mh = macd(closes)
    adxv, pdi, mdi = adx(candles)
    a = atr(candles)
    sup, res = support_resistance(candles)
    bbu, bbm, bbl = bollinger(closes)
    price = closes[-1]
    return {
        "price": price,
        "rsi": rsi(closes),
        "stoch_rsi": stoch_rsi(closes),
        "ema20": ema(closes, 20),
        "ema50": ema(closes, 50),
        "macd": m,
        "macd_signal": ms,
        "macd_hist": mh,
        "adx": adxv,
        "plus_di": pdi,
        "minus_di": mdi,
        "atr": a,
        "atr_pct": a / price * 100 if a else 0,
        "vwap": vwap(candles),
        "support": sup,
        "resistance": res,
        "fib": fibonacci(candles),
        "volume_ratio": volume_ratio(candles),
        "trend": trend(candles),
        "divergence": divergence(candles),
        "bb_upper": bbu,
        "bb_mid": bbm,
        "bb_lower": bbl,
        "last_closed_time": candles[-1]["time"],
    }


# ------------------------- LIVE MARKET -----------------------
def live_price(symbol):
    with market_lock:
        x = live_market.get(symbol, {}).copy()
    if not x:
        return None
    bid, ask, mark = x.get("bid"), x.get("ask"), x.get("mark")
    if bid and ask:
        return (bid + ask) / 2.0
    return mark or x.get("last")


def live_snapshot(symbol):
    with market_lock:
        return dict(live_market.get(symbol, {}))


def websocket_url(symbols):
    streams = []
    for s in symbols:
        sl = s.lower()
        streams.append(f"{sl}@bookTicker")
        streams.append(f"{sl}@markPrice@1s")
    return WS_URL + "?" + urlencode({"streams": "/".join(streams)})


def ws_on_message(_ws, message):
    try:
        obj = json.loads(message)
        data = obj.get("data", obj)
        stream = obj.get("stream", "")
        event = data.get("e")
        symbol = data.get("s")
        if not symbol and stream:
            symbol = stream.split("@")[0].upper()
        if not symbol:
            return
        now = time.time()
        with market_lock:
            item = live_market.setdefault(symbol, {})
            if event == "bookTicker" or "b" in data and "a" in data:
                item["bid"] = float(data.get("b"))
                item["ask"] = float(data.get("a"))
                item["last_event"] = now
            elif event == "markPriceUpdate" or "p" in data:
                item["mark"] = float(data.get("p"))
                item["last_event"] = now
    except Exception as exc:
        logger.debug("WS parse error: %s", exc)


def ws_on_open(_ws):
    with state_lock:
        state["ws_connected"] = True
    logger.info("Binance WebSocket conectado.")


def ws_on_error(_ws, error):
    logger.warning("Binance WebSocket error: %s", error)


def ws_on_close(_ws, code, msg):
    with state_lock:
        state["ws_connected"] = False
    logger.warning("Binance WebSocket cerrado: %s %s", code, msg)


def websocket_loop():
    while not stop_event.is_set():
        symbols = list(universe)
        if not symbols:
            stop_event.wait(10)
            continue
        url = websocket_url(symbols)
        ws = None
        try:
            ws = websocket.WebSocketApp(url, on_open=ws_on_open, on_message=ws_on_message, on_error=ws_on_error, on_close=ws_on_close)
            ws_restart_event.clear()
            def _watch_restart():
                while not stop_event.is_set() and not ws_restart_event.is_set():
                    time.sleep(0.5)
                if ws_restart_event.is_set():
                    try:
                        ws.close()
                    except Exception:
                        pass
            watcher = threading.Thread(target=_watch_restart, name="ws-restart-watch", daemon=True)
            watcher.start()
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as exc:
            logger.warning("WS excepción: %s", exc)
        finally:
            with state_lock:
                state["ws_connected"] = False
        stop_event.wait(1 if ws_restart_event.is_set() else 3)


# ------------------------- UNIVERSO --------------------------
def select_universe():
    contracts = get_exchange_info()
    volumes = get_24h_volumes()
    candidates = []
    for symbol in contracts:
        vol = volumes.get(symbol, 0)
        if vol < MIN_VOLUME_24H:
            continue
        candles = get_klines(symbol, "4h", 70, closed_only=True)
        if not candles:
            continue
        a = atr(candles)
        price = candles[-1]["close"]
        if not a or price <= 0:
            continue
        volat = a / price * 100
        candidates.append((symbol, volat, vol))
        time.sleep(0.08)

    if not candidates:
        return []

    candidates.sort(key=lambda x: x[1])
    calm = candidates[: max(10, TOTAL_MONEDAS - 10)]
    liquid_volatile = sorted([x for x in candidates if x[2] >= 40_000_000], key=lambda x: x[1], reverse=True)[:10]
    combined = calm + liquid_volatile
    result = []
    seen = set()
    for s, _, _ in combined:
        if s not in seen:
            seen.add(s)
            result.append(s)
        if len(result) >= TOTAL_MONEDAS:
            break
    logger.info("Universo actualizado: %d símbolos", len(result))
    return result


# ------------------------- SIGNAL ENGINE ---------------------
def funding_class(funding):
    if funding is None:
        return "NEUTRAL", "⚪"
    if funding < FUNDING_INFRA_PCT:
        return "INFRAVALORADA", "🟢"
    if funding > FUNDING_SOBRE_PCT:
        return "SOBREVALORADA", "🔴"
    return "NEUTRAL", "⚪"


def risk_leverage(score, adx1h, vol_pct):
    # Nunca aumenta leverage solo porque el mercado esté más volátil.
    # Volatilidad alta reduce leverage.
    lev = 5
    if score >= 9 and adx1h >= 25:
        lev = 6
    if score >= 10 and adx1h >= 30 and vol_pct < 1.8:
        lev = 7
    if score >= 11 and adx1h >= 32 and vol_pct < 1.4:
        lev = 8
    if score >= 12 and adx1h >= 35 and vol_pct < 1.0:
        lev = 9
    return max(MIN_LEVERAGE, min(MAX_LEVERAGE, lev))


def confluence(d):
    long_s = short_s = 0
    lm, sm = [], []

    for tf, pts in (("1d", 3), ("4h", 3), ("1h", 2)):
        t = d[tf]["trend"]
        if t == "ALCISTA":
            long_s += pts
            lm.append(f"{tf} alcista")
        elif t == "BAJISTA":
            short_s += pts
            sm.append(f"{tf} bajista")

    r = d["1h"]["rsi"]
    if r <= 42:
        long_s += 1; lm.append(f"RSI 1H {r:.1f}")
    elif r >= 58:
        short_s += 1; sm.append(f"RSI 1H {r:.1f}")

    div = d["1h"]["divergence"]
    if div == "ALCISTA": long_s += 1; lm.append("divergencia alcista")
    if div == "BAJISTA": short_s += 1; sm.append("divergencia bajista")

    mh = d["1h"]["macd_hist"]
    if mh > 0: long_s += 1; lm.append("MACD 1H positivo")
    elif mh < 0: short_s += 1; sm.append("MACD 1H negativo")

    a = d["1h"]["adx"]
    if a >= 20:
        if d["1h"]["plus_di"] > d["1h"]["minus_di"]:
            long_s += 1; lm.append(f"ADX {a:.1f} a favor")
        elif d["1h"]["minus_di"] > d["1h"]["plus_di"]:
            short_s += 1; sm.append(f"ADX {a:.1f} a favor")

    v = d["15m"]["volume_ratio"]
    if v >= 1.20:
        if long_s > short_s: long_s += 1; lm.append(f"volumen 15M {v:.1f}x")
        elif short_s > long_s: short_s += 1; sm.append(f"volumen 15M {v:.1f}x")

    p, vw = d["1h"]["price"], d["1h"]["vwap"]
    if vw:
        if p > vw: long_s += 1; lm.append("sobre VWAP 1H")
        elif p < vw: short_s += 1; sm.append("bajo VWAP 1H")

    if long_s >= MIN_SCORE and long_s >= short_s + 2:
        return "LONG", long_s, lm
    if short_s >= MIN_SCORE and short_s >= long_s + 2:
        return "SHORT", short_s, sm
    return None, max(long_s, short_s), []


def build_analysis(symbol):
    d = {}
    for tf in ("1d", "4h", "1h", "15m", "5m"):
        candles = get_klines(symbol, tf, 150, closed_only=True)
        if not candles or len(candles) < 60:
            return None
        d[tf] = analyze_tf(candles)
        time.sleep(0.08)
    d["futures"] = get_futures_context(symbol)
    return d


def find_entry_zone(d, direction):
    p = d["1h"]["price"]
    a = d["4h"]["atr"]
    if not a:
        return None
    levels = []
    if direction == "LONG":
        if d["4h"]["support"] < p: levels.append(d["4h"]["support"])
        for k in ("0.618", "0.786"):
            x = d["4h"]["fib"].get(k)
            if x and x < p: levels.append(x)
        vw = d["4h"]["vwap"]
        if vw and vw < p: levels.append(vw)
        if not levels: return None
        center = max(levels)
    else:
        if d["4h"]["resistance"] > p: levels.append(d["4h"]["resistance"])
        for k in ("0.382", "0.500", "0.618"):
            x = d["4h"]["fib"].get(k)
            if x and x > p: levels.append(x)
        vw = d["4h"]["vwap"]
        if vw and vw > p: levels.append(vw)
        if not levels: return None
        center = min(levels)
    width = a * ENTRY_ATR_WIDTH
    return center - width, center + width, center


def current_entry_price(symbol):
    return live_price(symbol)


def make_plan(symbol, d, direction, score, zone):
    zone_min, zone_max, center = zone
    entry = center
    a = d["1h"]["atr"]
    a4 = d["4h"]["atr"]
    if not a or not a4:
        return None

    if direction == "LONG":
        structural = min(zone_min, d["1h"]["support"])
        stop = structural - a * 0.15
        tp1 = entry + a * 1.8
        tp2 = entry + a * 3.0
    else:
        structural = max(zone_max, d["1h"]["resistance"])
        stop = structural + a * 0.15
        tp1 = entry - a * 1.8
        tp2 = entry - a * 3.0

    stop_pct = abs(entry - stop) / entry * 100
    if stop_pct <= 0 or stop_pct > MAX_STOP_PCT:
        return None

    risk = abs(entry - stop)
    rr1 = abs(tp1 - entry) / risk
    rr2 = abs(tp2 - entry) / risk
    if rr1 < MIN_RR_TP1 or rr2 < MIN_RR_TP2:
        return None

    lev = risk_leverage(score, d["1h"]["adx"], d["1h"]["atr_pct"])
    move2 = abs(tp2 - entry) / entry * 100
    gross = move2 * lev
    fees = TAKER_FEE_PCT_PER_SIDE * 2
    net = gross - fees
    if net < MIN_NET_TP2:
        return None

    funding, = (d["futures"].get("funding"),)
    valuation, emoji = funding_class(funding)
    aligned = (direction == "LONG" and valuation == "INFRAVALORADA") or (direction == "SHORT" and valuation == "SOBREVALORADA")

    return {
        "symbol": symbol,
        "direction": direction,
        "score": score,
        "grade": "A" if score >= MIN_SCORE_GRADE_A else "B",
        "entry_min": zone_min,
        "entry_max": zone_max,
        "entry": entry,
        "stop": stop,
        "tp1": tp1,
        "tp2": tp2,
        "rr1": rr1,
        "rr2": rr2,
        "stop_pct": stop_pct,
        "leverage": lev,
        "move_tp2_pct": move2,
        "net_tp2_pct": net,
        "funding": funding,
        "valuation": valuation,
        "emoji": emoji,
        "aligned_funding": aligned,
        "motives": [],
        "created_at": time.time(),
        "expires_at": time.time() + SIGNAL_TTL_SECONDS,
        "status": "PENDING",
        "prealert_sent": False,
        "entered": False,
        "entry_price_real": None,
    }


def round_price(symbol, price):
    tick = symbol_filters.get(symbol)
    if not tick or price is None:
        return price
    q = Decimal(str(tick))
    return float((Decimal(str(price)) / q).quantize(Decimal("1"), rounding=ROUND_DOWN) * q)


def fmt_price(symbol, p):
    p = round_price(symbol, p)
    if p is None:
        return "N/D"
    if p >= 1000: return f"{p:,.2f}"
    if p >= 1: return f"{p:,.4f}"
    if p >= .01: return f"{p:.6f}"
    return f"{p:.10f}"


def send_prealert(plan, price):
    s = plan["symbol"]
    direction = plan["direction"]
    title = "🟡 PREALERTA LONG" if direction == "LONG" else "🟠 PREALERTA SHORT"
    msg = (
        f"{title} {s}\n\n"
        f"Precio vivo: {fmt_price(s, price)}\n"
        f"Zona: {fmt_price(s, plan['entry_min'])} – {fmt_price(s, plan['entry_max'])}\n"
        f"⏳ Prepará Binance: la señal final llegará SOLO si confirma la entrada."
    )
    return send_telegram(msg)


def send_final_signal(plan, price):
    s, d = plan["symbol"], plan["direction"]
    title = "🟢 ENTRADA LONG" if d == "LONG" else "🔴 ENTRADA SHORT"
    funding = "N/D" if plan["funding"] is None else f"{plan['funding']:.3f}%"
    valid = max(0, int(plan["expires_at"] - time.time()))
    msg = (
        f"{title} {s} | GRADO {plan['grade']}\n\n"
        f"💵 Precio vivo: {fmt_price(s, price)}\n"
        f"📍 LIMIT sugerida: {fmt_price(s, plan['entry_min'])} – {fmt_price(s, plan['entry_max'])}\n"
        f"🛑 STOP: {fmt_price(s, plan['stop'])}\n"
        f"🎯 TP1: {fmt_price(s, plan['tp1'])}\n"
        f"🎯 TP2: {fmt_price(s, plan['tp2'])}\n\n"
        f"📊 Score: {plan['score']} | R:R TP1 {plan['rr1']:.2f} | TP2 {plan['rr2']:.2f}\n"
        f"⚙️ Apalancamiento máximo sugerido: {plan['leverage']}x\n"
        f"💰 Movimiento TP2: +{plan['move_tp2_pct']:.2f}% | estimado neto con 2 taker: +{plan['net_tp2_pct']:.2f}%\n"
        f"{plan['emoji']} Funding: {funding} ({plan['valuation']})\n\n"
        f"⏱️ SEÑAL VÁLIDA {valid//60}m {valid%60:02d}s\n"
        f"⚠️ Si el precio sale de la zona o vence el tiempo: NO ENTRAR.\n"
        f"👤 Orden MANUAL. El bot NO compra.\n\n"
        f"Tocá 'Confirmar' recién DESPUÉS de haber cargado la orden en Binance:"
    )
    return send_telegram(msg, reply_markup=confirm_keyboard(s))


def send_expired(plan, reason="Tiempo agotado"):
    s = plan["symbol"]
    send_telegram(f"⚪ SEÑAL CANCELADA {s}\n\n{reason}.\nNo colocar la orden con la señal vieja.")


def send_entry_touched(plan, price):
    s = plan["symbol"]
    send_telegram(
        f"🟢 ZONA DE ENTRADA TOCADA {s}\n\n"
        f"Precio vivo: {fmt_price(s, price)}\n"
        f"Zona: {fmt_price(s, plan['entry_min'])} – {fmt_price(s, plan['entry_max'])}\n"
        f"La orden LIMIT debe respetar la zona."
    )


def send_virtual_exit(trade, price, reason):
    s, d = trade["symbol"], trade["direction"]
    entry = trade["entry_price_real"] or trade["entry"]
    pnl = ((price - entry) / entry * 100) if d == "LONG" else ((entry - price) / entry * 100)
    sign = "+" if pnl >= 0 else ""
    send_telegram(
        f"{'🟢' if pnl >= 0 else '🔴'} SALIDA {s}\n"
        f"Motivo: {reason}\n"
        f"Precio: {fmt_price(s, price)}\n"
        f"Resultado precio: {sign}{pnl:.2f}%\n"
        f"⚠️ Seguimiento virtual/manual: el bot no ejecutó la operación."
    )


# ------------------------- CONFIRMAR / CANCELAR ---------------
# Lógica compartida entre el endpoint manual (/confirmar, /cancelar)
# y los botones de Telegram (webhook), para no duplicar código.
def do_confirm(symbol, price):
    symbol = symbol.upper()
    with state_lock:
        plan = state["pending"].pop(symbol, None)
        if not plan:
            return False, "No hay señal pendiente para ese símbolo"
        plan["entered"] = True
        plan["entry_price_real"] = price
        plan["status"] = "TP1_WAIT"
        plan["confirmed_at"] = time.time()
        state["virtual_trades"][symbol] = plan
    save_state()
    send_telegram(
        f"🟢 ENTRADA CONFIRMADA {symbol}\n"
        f"Precio: {fmt_price(symbol, price)}\n"
        f"El bot inicia seguimiento virtual de SL/TP."
    )
    return True, f"Entrada confirmada a {fmt_price(symbol, price)}"


def do_cancel(symbol):
    symbol = symbol.upper()
    with state_lock:
        plan = state["pending"].pop(symbol, None)
    if not plan:
        return False, "No hay señal pendiente para ese símbolo"
    save_state()
    send_telegram(f"⚪ SEÑAL CANCELADA MANUALMENTE {symbol}")
    return True, "Señal cancelada"


# ------------------------- PROCESS ---------------------------
def process_symbol(symbol):
    try:
        # No gastar llamadas REST si ya hay una señal/trade de este símbolo,
        # ni si ya estamos en el tope de señales pendientes o de exposición total.
        with state_lock:
            if symbol in state["pending"] or symbol in state["virtual_trades"]:
                return
            if len(state["pending"]) >= MAX_SIGNAL_SLOTS:
                return
            total_exposure = len(state["pending"]) + len(state["virtual_trades"])
            if total_exposure >= MAX_TOTAL_EXPOSURE:
                return

        d = build_analysis(symbol)
        if not d:
            return

        direction, score, motives = confluence(d)
        if not direction or score < MIN_SCORE:
            return

        # Evitar perseguir precio con 1H extremo.
        r1 = d["1h"]["rsi"]
        sr1 = d["1h"]["stoch_rsi"]
        if direction == "LONG" and (r1 > RSI_OVERBOUGHT or sr1 > 90):
            return
        if direction == "SHORT" and (r1 < RSI_OVERSOLD or sr1 < 10):
            return

        # Confirmación 5M: dirección + fuerza.
        if d["5m"]["adx"] < ADX_MIN_5M or d["5m"]["volume_ratio"] < VOLUME_MIN_5M:
            return
        if direction == "LONG" and d["5m"]["plus_di"] <= d["5m"]["minus_di"]:
            return
        if direction == "SHORT" and d["5m"]["minus_di"] <= d["5m"]["plus_di"]:
            return

        zone = find_entry_zone(d, direction)
        if not zone:
            return
        plan = make_plan(symbol, d, direction, score, zone)
        if not plan:
            return
        plan["motives"] = motives[:6]

        price = current_entry_price(symbol)
        if price is None:
            return

        zmin, zmax = plan["entry_min"], plan["entry_max"]
        a = d["4h"]["atr"] or d["1h"]["atr"]
        distance = min(abs(price - zmin), abs(price - zmax)) if not (zmin <= price <= zmax) else 0
        if not (zmin <= price <= zmax) and distance > a * PREALERT_ATR:
            return

        # Prealert: preparar Binance, pero todavía no entrar.
        if not (zmin <= price <= zmax):
            key = symbol
            now = time.time()
            with state_lock:
                previous = state["prealerts"].get(key, 0)
                if now - previous >= PREALERT_TTL_SECONDS:
                    state["prealerts"][key] = now
                    save_state()
                    send_prealert(plan, price)
            return

        # Confirmación final: precio ya está en zona. Revalidar exposición
        # total por si cambió mientras se armaba el análisis.
        with state_lock:
            total_exposure = len(state["pending"]) + len(state["virtual_trades"])
            if total_exposure >= MAX_TOTAL_EXPOSURE:
                return

        plan["status"] = "PENDING"
        plan["created_at"] = time.time()
        plan["expires_at"] = plan["created_at"] + SIGNAL_TTL_SECONDS
        if send_final_signal(plan, price):
            with state_lock:
                state["pending"][symbol] = plan
            save_state()
            logger.info("SEÑAL %s %s score=%s precio=%s", direction, symbol, score, price)
    except Exception as exc:
        logger.exception("Error procesando %s: %s", symbol, exc)


# ------------------------- LIVE MONITOR ----------------------
def monitor_pending():
    while not stop_event.is_set():
        now = time.time()
        changed = False
        with state_lock:
            pending_items = list(state["pending"].items())
            trades = list(state["virtual_trades"].items())

        for symbol, plan in pending_items:
            price = live_price(symbol)
            if price is None:
                continue
            if now > float(plan.get("expires_at", now)):
                with state_lock:
                    state["pending"].pop(symbol, None)
                send_expired(plan, "La ventana de entrada venció")
                changed = True
                continue

            zmin, zmax = float(plan["entry_min"]), float(plan["entry_max"])
            if zmin <= price <= zmax and not plan.get("entered_alert_sent"):
                with state_lock:
                    if symbol in state["pending"]:
                        state["pending"][symbol]["entered_alert_sent"] = True
                send_entry_touched(plan, price)
                changed = True

            # Si se aleja demasiado después de la señal, cancelar antes del vencimiento.
            a = max(abs(plan["entry_max"] - plan["entry_min"]), 1e-12)
            if plan["direction"] == "LONG" and price > zmax + a * 2.0:
                with state_lock:
                    state["pending"].pop(symbol, None)
                send_expired(plan, "El precio se escapó por arriba de la zona")
                changed = True
            elif plan["direction"] == "SHORT" and price < zmin - a * 2.0:
                with state_lock:
                    state["pending"].pop(symbol, None)
                send_expired(plan, "El precio se escapó por debajo de la zona")
                changed = True

        for symbol, trade in trades:
            price = live_price(symbol)
            if price is None:
                continue
            d = trade["direction"]
            stop = trade["stop"]
            tp1 = trade["tp1"]
            tp2 = trade["tp2"]
            status = trade.get("status", "TP1_WAIT")
            if (d == "LONG" and price <= stop) or (d == "SHORT" and price >= stop):
                send_virtual_exit(trade, price, "STOP LOSS")
                with state_lock:
                    state["virtual_trades"].pop(symbol, None)
                changed = True
                continue
            if status == "TP1_WAIT" and ((d == "LONG" and price >= tp1) or (d == "SHORT" and price <= tp1)):
                if trade.get("aligned_funding"):
                    send_virtual_exit(trade, price, "TP1 alcanzado — tomar 50% y proteger el resto")
                    with state_lock:
                        if symbol in state["virtual_trades"]:
                            state["virtual_trades"][symbol]["status"] = "TP2_WAIT"
                            state["virtual_trades"][symbol]["stop"] = trade["entry_price_real"] or trade["entry"]
                else:
                    send_virtual_exit(trade, price, "TP1 alcanzado — salida total")
                    with state_lock:
                        state["virtual_trades"].pop(symbol, None)
                changed = True
                continue
            if status == "TP2_WAIT" and ((d == "LONG" and price >= tp2) or (d == "SHORT" and price <= tp2)):
                send_virtual_exit(trade, price, "TP2 alcanzado")
                with state_lock:
                    state["virtual_trades"].pop(symbol, None)
                changed = True

        if changed:
            save_state()
        stop_event.wait(1.0)


# ------------------------- ANALYSIS LOOP ---------------------
def analysis_loop():
    global universe
    while not stop_event.is_set():
        try:
            now = time.time()
            if not universe or now - state.get("last_universe", 0) >= UNIVERSE_REFRESH_SECONDS:
                new_universe = select_universe()
                if new_universe:
                    universe = new_universe
                    with state_lock:
                        state["last_universe"] = now
                    save_state()
                    # Reiniciar WS para suscribir el nuevo universo.
                    ws_restart_event.set()
                    with market_lock:
                        for s in list(live_market):
                            if s not in universe:
                                live_market.pop(s, None)

            if universe:
                for symbol in list(universe):
                    if stop_event.is_set():
                        break
                    process_symbol(symbol)
                    time.sleep(0.12)
                with state_lock:
                    state["last_analysis"] = time.time()
                save_state()
            stop_event.wait(ANALYSIS_INTERVAL)
        except Exception as exc:
            logger.exception("Error en ciclo principal: %s", exc)
            stop_event.wait(20)


# ------------------------- REPORTING -------------------------
def daily_report_loop():
    last_day = None
    while not stop_event.is_set():
        now = datetime.now()
        key = now.strftime("%Y-%m-%d")
        if now.hour == 9 and now.minute == 30 and last_day != key:
            with state_lock:
                pending = len(state["pending"])
                trades = len(state["virtual_trades"])
            with market_lock:
                live_count = sum(1 for x in live_market.values() if time.time() - x.get("last_event", 0) < 10)
            with state_lock:
                ws_ok = state["ws_connected"]
            with error_lock:
                errors = consecutive_binance_errors
                last_err = last_binance_error
            send_telegram(
                "✅ BOT FUTUROS PRO — CONTROL DIARIO\n\n"
                f"WS mercado: {'🟢' if ws_ok else '🔴'}\n"
                f"Precios vivos recientes: {live_count}\n"
                f"Señales pendientes: {pending}\n"
                f"Trades virtuales: {trades} (exposición máx: {MAX_TOTAL_EXPOSURE})\n"
                f"Errores Binance consecutivos: {errors} (último: {last_err})\n"
                "👤 Operación manual: el bot no ejecuta órdenes."
            )
            last_day = key
        stop_event.wait(20)


# ------------------------- WEB -------------------------------
def authorized():
    return bool(ADMIN_TOKEN) and request.args.get("token") == ADMIN_TOKEN


@app.route("/")
def root():
    return "BOT FUTUROS PRO ONLINE"


@app.route("/health")
def health():
    with state_lock:
        ws_ok = bool(state["ws_connected"])
        last_analysis = state["last_analysis"]
        exposure = len(state["pending"]) + len(state["virtual_trades"])
    with error_lock:
        errors = consecutive_binance_errors
        last_err = last_binance_error
    return jsonify({
        "status": "ok",
        "ws_connected": ws_ok,
        "universe_size": len(universe),
        "last_analysis": utc_iso(last_analysis) if last_analysis else None,
        "uptime_seconds": int(time.time() - state.get("started_at", time.time())),
        "total_exposure": exposure,
        "max_total_exposure": MAX_TOTAL_EXPOSURE,
        "consecutive_binance_errors": errors,
        "last_binance_error": last_err,
    })


@app.route("/estado")
def status():
    if not authorized():
        return "No autorizado", 403
    with state_lock:
        safe = {
            "pending": state["pending"],
            "virtual_trades": state["virtual_trades"],
            "last_analysis": utc_iso(state["last_analysis"]) if state["last_analysis"] else None,
            "last_universe": utc_iso(state["last_universe"]) if state["last_universe"] else None,
            "ws_connected": state["ws_connected"],
        }
    with error_lock:
        safe["consecutive_binance_errors"] = consecutive_binance_errors
        safe["last_binance_error"] = last_binance_error
    return jsonify(safe)


@app.route("/test")
def test():
    if not authorized():
        return "No autorizado", 403
    ok = send_telegram("🟢 TEST BOT FUTUROS PRO\n\nTelegram conectado correctamente.")
    return ("TEST ENVIADO" if ok else "FALLO TELEGRAM"), (200 if ok else 500)


@app.route("/senal/<symbol>")
def signal_detail(symbol):
    if not authorized():
        return "No autorizado", 403
    symbol = symbol.upper()
    with state_lock:
        p = state["pending"].get(symbol)
        t = state["virtual_trades"].get(symbol)
    with market_lock:
        live = live_snapshot(symbol)
    return jsonify({"symbol": symbol, "live": live, "pending": p, "virtual_trade": t})


# ------------------------- MANUAL CONFIRMATION --------------
# El bot no puede saber si realmente compraste en Binance sin API privada.
# Para que el seguimiento virtual use tu precio real, se puede llamar:
# /confirmar?token=...&symbol=BTCUSDT&price=12345
# (o, más cómodo, tocar el botón "Confirmar" en el mensaje de Telegram,
# que usa el precio en vivo en el momento del click).
@app.route("/confirmar")
def confirm():
    if not authorized():
        return "No autorizado", 403
    symbol = request.args.get("symbol", "").upper()
    try:
        price = float(request.args.get("price", ""))
    except ValueError:
        return "Precio inválido", 400
    ok, msg = do_confirm(symbol, price)
    return msg, (200 if ok else 404)


@app.route("/cancelar")
def cancel_signal():
    if not authorized():
        return "No autorizado", 403
    symbol = request.args.get("symbol", "").upper()
    ok, msg = do_cancel(symbol)
    return msg, (200 if ok else 404)


# ------------------------- TELEGRAM WEBHOOK -------------------
# Recibe los clicks de los botones "Confirmar" / "Cancelar" que van
# pegados al mensaje de señal. Para activarlo, una sola vez:
#   GET /setup-webhook?token=TU_ADMIN_TOKEN
# (usa automáticamente la URL pública de Render).
@app.route("/telegram-webhook", methods=["POST"])
def telegram_webhook():
    if TELEGRAM_WEBHOOK_SECRET:
        header = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if header != TELEGRAM_WEBHOOK_SECRET:
            return "No autorizado", 403

    update = request.get_json(silent=True) or {}
    cq = update.get("callback_query")
    if not cq:
        return jsonify({"ok": True})

    cq_id = cq.get("id")
    data = cq.get("data", "")
    try:
        action, symbol = data.split(":", 1)
    except ValueError:
        answer_callback(cq_id, "Dato inválido")
        return jsonify({"ok": True})

    symbol = symbol.upper()

    if action == "confirm":
        price = live_price(symbol)
        if price is None:
            answer_callback(cq_id, "Sin precio en vivo ahora, usá /confirmar con precio manual", show_alert=True)
            return jsonify({"ok": True})
        ok, msg = do_confirm(symbol, price)
        answer_callback(cq_id, msg, show_alert=not ok)
    elif action == "cancel":
        ok, msg = do_cancel(symbol)
        answer_callback(cq_id, msg, show_alert=not ok)
    else:
        answer_callback(cq_id, "Acción desconocida")

    return jsonify({"ok": True})


@app.route("/setup-webhook")
def setup_webhook():
    if not authorized():
        return "No autorizado", 403
    if not TELEGRAM_TOKEN:
        return "Falta TELEGRAM_TOKEN", 400
    base_url = request.args.get("url") or request.url_root.rstrip("/")
    webhook_url = base_url.rstrip("/") + "/telegram-webhook"
    payload = {"url": webhook_url, "allowed_updates": ["callback_query"]}
    if TELEGRAM_WEBHOOK_SECRET:
        payload["secret_token"] = TELEGRAM_WEBHOOK_SECRET
    try:
        r = session.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setWebhook", json=payload, timeout=12)
        return jsonify(r.json())
    except Exception as exc:
        return f"Error: {exc}", 500


# ------------------------- START / STOP ----------------------
def start_workers():
    global ws_thread, analysis_thread, monitor_thread
    load_state()
    send_telegram(
        "🤖 BOT FUTUROS PRO ONLINE (v3)\n\n"
        "📡 Precio en vivo: WebSocket Binance\n"
        "📊 Análisis: 1D / 4H / 1H / 15M / 5M\n"
        "🧠 Señales confirmadas con velas CERRADAS\n"
        f"⏱️ Caducidad señal final: {SIGNAL_TTL_SECONDS//60} min\n"
        f"🎯 RR mínimo TP1: {MIN_RR_TP1:.1f} | TP2: {MIN_RR_TP2:.1f}\n"
        f"📦 Exposición máxima simultánea: {MAX_TOTAL_EXPOSURE}\n"
        "🔔 Ahora avisa si Binance falla o bloquea la IP.\n"
        "✅ Confirmar/cancelar con botones en el mensaje de Telegram.\n"
        "👤 Órdenes manuales — el bot NO compra."
    )

    ws_thread = threading.Thread(target=websocket_loop, name="binance-ws", daemon=True)
    monitor_thread = threading.Thread(target=monitor_pending, name="live-monitor", daemon=True)
    analysis_thread = threading.Thread(target=analysis_loop, name="analysis", daemon=True)
    report_thread = threading.Thread(target=daily_report_loop, name="daily-report", daemon=True)

    ws_thread.start()
    monitor_thread.start()
    analysis_thread.start()
    report_thread.start()


def shutdown(*_args):
    logger.info("Apagando bot...")
    stop_event.set()


os_signal.signal(os_signal.SIGTERM, shutdown)
os_signal.signal(os_signal.SIGINT, shutdown)

if __name__ == "__main__":
    start_workers()
    port = int(os.getenv("PORT", "10000"))
    # Para Render/Gunicorn, main:app es el entrypoint. El worker se inicia al importar.
    # En ejecución directa también funciona con Flask.
    app.run(host="0.0.0.0", port=port, debug=False)
