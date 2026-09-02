import os
import json
import time
import math
import signal as os_signal
import threading
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from decimal import Decimal, ROUND_DOWN
from urllib.parse import urlencode

import numpy as np
import requests
from flask import Flask, jsonify, request
import websocket

# ============================================================
# BOT DE FUTUROS PRO - SEÑALES MANUALES (v4)
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

TOTAL_MONEDAS = 15
TOP_SCAN_SYMBOLS = 100
CALM_SYMBOLS = 7
VOLATILE_SYMBOLS = 8
SCAN_KLINES_LIMIT = 70
SCAN_KLINE_SLEEP = 0.35
SCAN_CACHE_SECONDS = 21600
ARG_TZ = ZoneInfo("America/Argentina/Buenos_Aires")
MAX_TRADES_PER_DAY = 4
MAX_OPEN_POSITIONS = 2
MIN_HOURS_BETWEEN_TRADES = 4.0
MARGIN_USDT = 30.0
LEVERAGE = 5
REST_MIN_INTERVAL = 0.15
MIN_VOLUME_24H = 300_000_000.0
MIN_SCORE = int(os.getenv("MIN_SCORE", "7"))
MIN_SCORE_GRADE_A = int(os.getenv("MIN_SCORE_GRADE_A", "8"))
MAX_SIGNAL_SLOTS = int(os.getenv("MAX_SIGNAL_SLOTS", "5"))

# Exposición total = señales pendientes + trades virtuales en seguimiento.
# Antes solo se limitaban las pendientes; esto evita acumular más
# posiciones virtuales de las que realmente podés seguir.
MAX_TOTAL_EXPOSURE = int(os.getenv("MAX_TOTAL_EXPOSURE", "8"))

# La señal final se mantiene válida durante este tiempo.
SIGNAL_TTL_SECONDS = int(os.getenv("SIGNAL_TTL_SECONDS", "240"))  # 4 min
PREALERT_TTL_SECONDS = int(os.getenv("PREALERT_TTL_SECONDS", "900"))

# Motor de análisis normal.
ANALYSIS_INTERVAL = int(os.getenv("ANALYSIS_INTERVAL", "60"))
UNIVERSE_REFRESH_SECONDS = SCAN_CACHE_SECONDS

# Entrada: el precio debe estar dentro de la zona.
ENTRY_ATR_WIDTH = float(os.getenv("ENTRY_ATR_WIDTH", "0.35"))
PREALERT_ATR = float(os.getenv("PREALERT_ATR", "1.00"))
ENTRY_MAX_CHASE_ATR = float(os.getenv("ENTRY_MAX_CHASE_ATR", "0.35"))
DEBUG_REJECTIONS = os.getenv("DEBUG_REJECTIONS", "true").lower() in ("1", "true", "yes", "on")
SIGNAL_COOLDOWN_SECONDS = int(os.getenv("SIGNAL_COOLDOWN_SECONDS", "1800"))

# Riesgo.
SL_PCT = 1.0
TP1_PCT = 1.5
TP2_PCT = 3.0
TP3_PCT = 5.0
MIN_LEVERAGE = 5
MAX_LEVERAGE = 5

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
session.headers.update({"User-Agent": "futures-signal-bot-pro/4.0"})

# ------------------------- ESTADO ----------------------------
state_lock = threading.RLock()
state = {
    "pending": {},       # señales que esperan entrada
    "virtual_trades": {},# operaciones que el usuario puede seguir manualmente
    "prealerts": {},
    "last_analysis": 0,
    "last_universe": 0,
    "last_scan": 0,
    "trading_day": "",
    "trades_today": 0,
    "last_trade_at": 0,
    "used_windows": [],
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
binance_block_until = 0.0
last_rest_request_at = 0.0
rest_lock = threading.Lock()
scan_cache = []
last_scan = 0.0


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
def _set_binance_backoff(seconds, status_code):
    global binance_block_until
    seconds = max(1, int(seconds))
    until = time.time() + seconds
    with error_lock:
        binance_block_until = max(binance_block_until, until)
    logger.warning("Binance %s: pausando REST durante %ss", status_code, seconds)


def _retry_after_seconds(response, default=60):
    value = response.headers.get("Retry-After")
    try:
        return max(1, int(float(value)))
    except (TypeError, ValueError):
        return default


def binance_get(endpoint, params=None, retries=2):
    global consecutive_binance_errors, last_binance_error, last_rest_request_at

    for attempt in range(retries):
        with error_lock:
            blocked_until = binance_block_until
        wait_global = blocked_until - time.time()
        if wait_global > 0:
            logger.warning("REST Binance pausado por rate limit: %.0fs restantes", wait_global)
            return None

        try:
            with rest_lock:
                gap = REST_MIN_INTERVAL - (time.time() - last_rest_request_at)
                if gap > 0:
                    time.sleep(gap)
                last_rest_request_at = time.time()
                r = session.get(BINANCE_URL + endpoint, params=params, timeout=12)

            if r.status_code == 200:
                with error_lock:
                    consecutive_binance_errors = 0
                    last_binance_error = None
                return r.json()

            if r.status_code == 451:
                with error_lock:
                    consecutive_binance_errors += 1
                    last_binance_error = 451
                logger.error("Binance 451 en %s", endpoint)
                maybe_alert_binance_down(blocked=True, status_code=451)
                return None

            if r.status_code in (418, 429):
                wait = _retry_after_seconds(r, 120 if r.status_code == 418 else 30)
                with error_lock:
                    consecutive_binance_errors += 1
                    last_binance_error = r.status_code
                _set_binance_backoff(wait, r.status_code)
                maybe_alert_binance_down()
                return None

            with error_lock:
                consecutive_binance_errors += 1
                last_binance_error = r.status_code
            logger.error("Binance REST %s %s: %s", r.status_code, endpoint, r.text[:250])
            maybe_alert_binance_down()
            return None

        except Exception as exc:
            logger.warning("Binance REST intento %d/%d: %s", attempt + 1, retries, exc)
            if attempt + 1 < retries:
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
    funding = None
    mark = None
    if premium:
        try:
            mark = float(premium.get("markPrice"))
            funding = float(premium.get("lastFundingRate")) * 100.0
        except Exception:
            pass
    return {"mark": mark, "funding": funding, "open_interest": None}


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
    """Escaneo 6h: TOP 100 por volumen, filtro >=300M, ATR% 4H y 7+8."""
    global scan_cache, last_scan
    now = time.time()
    if scan_cache and now - last_scan < SCAN_CACHE_SECONDS:
        return list(scan_cache)

    contracts = get_exchange_info()
    volumes = get_24h_volumes()
    if not contracts or not volumes:
        logger.warning("No se pudo obtener universo base de Binance.")
        return list(scan_cache)

    liquid = [(s, volumes.get(s, 0.0)) for s in contracts if volumes.get(s, 0.0) >= MIN_VOLUME_24H]
    liquid.sort(key=lambda x: x[1], reverse=True)
    top = liquid[:TOP_SCAN_SYMBOLS]
    logger.info("SCAN: TOP %d por volumen; %d cumplen volumen >= %.0f USDT", TOP_SCAN_SYMBOLS, len(top), MIN_VOLUME_24H)

    candidates = []
    for idx, (symbol, vol) in enumerate(top, 1):
        candles = get_klines(symbol, "4h", SCAN_KLINES_LIMIT, closed_only=True)
        if not candles:
            continue
        a = atr(candles, 14)
        price = candles[-1]["close"]
        if not a or price <= 0:
            continue
        atr_pct = a / price * 100.0
        candidates.append((symbol, atr_pct, vol))
        # Separación obligatoria del escaneo para no golpear REST.
        time.sleep(SCAN_KLINE_SLEEP)

    if not candidates:
        logger.warning("SCAN sin candidatos válidos; se conserva universo anterior.")
        return list(scan_cache)

    candidates.sort(key=lambda x: x[1])
    calm = candidates[:CALM_SYMBOLS]
    volatile = sorted(candidates, key=lambda x: x[1], reverse=True)[:VOLATILE_SYMBOLS]
    combined = calm + volatile
    result = []
    seen = set()
    for symbol, atr_pct, vol in combined:
        if symbol not in seen:
            seen.add(symbol)
            result.append(symbol)

    result = result[:TOTAL_MONEDAS]
    scan_cache = result
    last_scan = time.time()
    with state_lock:
        state["last_scan"] = last_scan
        state["last_universe"] = last_scan
    save_state()
    logger.info("UNIVERSO FINAL: %d símbolos | calm=%d volatile=%d", len(result), len([x for x in result if x in {s for s,_,_ in calm}]), len(result) - len([x for x in result if x in {s for s,_,_ in calm}]))
    return list(result)


def arg_now():
    return datetime.now(ARG_TZ)


def current_arg_window():
    h = arg_now().hour
    start = (h // 6) * 6
    end = start + 6
    return start, end


def arg_window_key():
    now = arg_now()
    start, end = current_arg_window()
    return f"{now.strftime('%Y-%m-%d')} {start:02d}:00-{end:02d}:00 ARG"


def reset_trading_day_if_needed():
    today = arg_now().strftime('%Y-%m-%d')
    with state_lock:
        if state.get("trading_day") != today:
            state["trading_day"] = today
            state["trades_today"] = 0
            state["last_trade_at"] = 0
            state["used_windows"] = []
            save_state()


def trade_gate_reason():
    reset_trading_day_if_needed()
    with state_lock:
        if state["trades_today"] >= MAX_TRADES_PER_DAY:
            return f"máximo diario alcanzado ({MAX_TRADES_PER_DAY})"
        if len(state["virtual_trades"]) >= MAX_OPEN_POSITIONS:
            return f"máximo de posiciones abiertas ({MAX_OPEN_POSITIONS})"
        last_trade = float(state.get("last_trade_at") or 0)
        if last_trade and time.time() - last_trade < MIN_HOURS_BETWEEN_TRADES * 3600:
            remaining = MIN_HOURS_BETWEEN_TRADES - (time.time() - last_trade) / 3600
            return f"faltan {remaining:.1f}h para el intervalo mínimo"
        if arg_window_key() in state.get("used_windows", []):
            return "ya hubo un trade confirmado en esta ventana ARG"
    return None


def register_confirmed_trade():
    reset_trading_day_if_needed()
    window = arg_window_key()
    with state_lock:
        if window in state.get("used_windows", []):
            return False
        if state["trades_today"] >= MAX_TRADES_PER_DAY:
            return False
        if len(state["virtual_trades"]) > MAX_OPEN_POSITIONS:
            return False
        state["trades_today"] += 1
        state["last_trade_at"] = time.time()
        state.setdefault("used_windows", []).append(window)
    save_state()
    return True


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
    return LEVERAGE


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
    """Construye el plan de señal. NO ejecuta órdenes."""
    zone_min, zone_max, center = zone
    entry = float(center)
    if entry <= 0:
        return None

    if direction == "LONG":
        stop = entry * (1.0 - SL_PCT / 100.0)
        tp1 = entry * (1.0 + TP1_PCT / 100.0)
        tp2 = entry * (1.0 + TP2_PCT / 100.0)
        tp3 = entry * (1.0 + TP3_PCT / 100.0)
    else:
        stop = entry * (1.0 + SL_PCT / 100.0)
        tp1 = entry * (1.0 - TP1_PCT / 100.0)
        tp2 = entry * (1.0 - TP2_PCT / 100.0)
        tp3 = entry * (1.0 - TP3_PCT / 100.0)

    risk_pct = SL_PCT
    rr1 = TP1_PCT / risk_pct
    rr2 = TP2_PCT / risk_pct
    rr3 = TP3_PCT / risk_pct
    lev = LEVERAGE

    funding = d["futures"].get("funding")
    valuation, emoji = funding_class(funding)
    aligned = (direction == "LONG" and valuation == "INFRAVALORADA") or (direction == "SHORT" and valuation == "SOBREVALORADA")

    return {
        "symbol": symbol, "direction": direction, "score": score,
        "grade": "A" if score >= MIN_SCORE_GRADE_A else "B",
        "entry_min": zone_min, "entry_max": zone_max, "entry": entry,
        "stop": stop, "tp1": tp1, "tp2": tp2, "tp3": tp3,
        "rr1": rr1, "rr2": rr2, "rr3": rr3,
        "stop_pct": SL_PCT, "leverage": lev,
        "move_tp1_pct": TP1_PCT, "move_tp2_pct": TP2_PCT, "move_tp3_pct": TP3_PCT,
        "net_tp2_pct": TP2_PCT * lev - (TAKER_FEE_PCT_PER_SIDE * 2),
        "funding": funding, "valuation": valuation, "emoji": emoji,
        "aligned_funding": aligned, "motives": [],
        "created_at": time.time(), "expires_at": time.time() + SIGNAL_TTL_SECONDS,
        "status": "PENDING", "prealert_sent": False, "entered": False,
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
    s, direction = plan["symbol"], plan["direction"]
    title = "🟢 SEÑAL DE COMPRA LONG" if direction == "LONG" else "🔴 SEÑAL DE VENTA SHORT"
    funding = "N/D" if plan["funding"] is None else f"{plan['funding']:.3f}%"
    valid = max(0, int(plan["expires_at"] - time.time()))
    msg = (
        f"{title} {s} | GRADO {plan['grade']}\n\n"
        f"💵 Precio vivo: {fmt_price(s, price)}\n"
        f"📍 ZONA LIMIT: {fmt_price(s, plan['entry_min'])} – {fmt_price(s, plan['entry_max'])}\n"
        f"🎯 Entrada guía: {fmt_price(s, plan['entry'])}\n\n"
        f"🛑 SL: {fmt_price(s, plan['stop'])}  (-{SL_PCT:.2f}% spot / aprox. -{SL_PCT*5:.2f}% con 5x)\n"
        f"🎯 TP1: {fmt_price(s, plan['tp1'])}  (+{TP1_PCT:.2f}% spot / +{TP1_PCT*5:.2f}% con 5x)\n"
        f"🎯 TP2: {fmt_price(s, plan['tp2'])}  (+{TP2_PCT:.2f}% spot / +{TP2_PCT*5:.2f}% con 5x)\n"
        f"🎯 TP3: {fmt_price(s, plan['tp3'])}  (+{TP3_PCT:.2f}% spot / +{TP3_PCT*5:.2f}% con 5x)\n\n"
        f"📊 Score: {plan['score']} | R:R 1={plan['rr1']:.1f} | 2={plan['rr2']:.1f} | 3={plan['rr3']:.1f}\n"
        f"⚙️ Estrategia: S × 5 | Aislado | margen sugerido: 30 USDT\n"
        f"💰 Exposición aproximada: {MARGIN_USDT * LEVERAGE:.0f} USDT\n"
        f"{plan['emoji']} Funding: {funding} ({plan['valuation']})\n\n"
        f"⏱️ SEÑAL VÁLIDA {valid//60}m {valid%60:02d}s\n"
        f"⚠️ No entrar si la señal vence o el precio se escapa de la zona.\n"
        f"👤 EJECUCIÓN MANUAL — el bot NO coloca órdenes en Binance.\n\n"
        f"Motivos: {', '.join(plan.get('motives', [])[:6])}"
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
    gate = trade_gate_reason()
    if gate:
        return False, f"No se puede confirmar: {gate}"
    with state_lock:
        plan = state["pending"].get(symbol)
        if not plan:
            return False, "No hay señal pendiente para ese símbolo"
        if len(state["virtual_trades"]) >= MAX_OPEN_POSITIONS:
            return False, f"Máximo de posiciones abiertas: {MAX_OPEN_POSITIONS}"
        plan["entered"] = True
        plan["entry_price_real"] = price
        plan["status"] = "TP1_WAIT"
        plan["confirmed_at"] = time.time()
        state["virtual_trades"][symbol] = plan
        state["pending"].pop(symbol, None)
    if not register_confirmed_trade():
        with state_lock:
            state["virtual_trades"].pop(symbol, None)
            state["pending"][symbol] = plan
        return False, "No se pudo registrar el trade por las reglas de límite"
    save_state()
    send_telegram(
        f"🟢 ENTRADA CONFIRMADA {symbol}\n"
        f"Precio: {fmt_price(symbol, price)}\n"
        f"Seguimiento VIRTUAL iniciado. El bot NO ejecutó ninguna orden en Binance."
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
def reject(symbol, reason):
    if DEBUG_REJECTIONS:
        logger.info("RECHAZADA %s | %s", symbol, reason)


def process_symbol(symbol):
    try:
        with state_lock:
            if symbol in state["pending"] or symbol in state["virtual_trades"]:
                reject(symbol, "ya tiene señal/trade en seguimiento")
                return
            if len(state["pending"]) >= MAX_SIGNAL_SLOTS:
                reject(symbol, "MAX_SIGNAL_SLOTS alcanzado")
                return
            if len(state["pending"]) + len(state["virtual_trades"]) >= MAX_TOTAL_EXPOSURE:
                reject(symbol, "MAX_TOTAL_EXPOSURE alcanzado")
                return

        gate = trade_gate_reason()
        if gate:
            reject(symbol, f"regla operativa: {gate}")
            return

        d = build_analysis(symbol)
        if not d:
            reject(symbol, "faltan velas/datos")
            return

        direction, score, motives = confluence(d)
        if not direction:
            reject(symbol, f"sin confluencia suficiente (score={score})")
            return
        if score < MIN_SCORE:
            reject(symbol, f"score {score} < mínimo {MIN_SCORE}")
            return

        r1, sr1 = d["1h"]["rsi"], d["1h"]["stoch_rsi"]
        if direction == "LONG" and (r1 > RSI_OVERBOUGHT or sr1 > 90):
            reject(symbol, f"LONG sobreextendido RSI={r1:.1f} StochRSI={sr1:.1f}")
            return
        if direction == "SHORT" and (r1 < RSI_OVERSOLD or sr1 < 10):
            reject(symbol, f"SHORT sobreextendido RSI={r1:.1f} StochRSI={sr1:.1f}")
            return

        if d["5m"]["adx"] < ADX_MIN_5M:
            reject(symbol, f"ADX 5M {d['5m']['adx']:.1f} < {ADX_MIN_5M}")
            return
        if d["5m"]["volume_ratio"] < VOLUME_MIN_5M:
            reject(symbol, f"volumen 5M {d['5m']['volume_ratio']:.2f}x < {VOLUME_MIN_5M}x")
            return
        if direction == "LONG" and d["5m"]["plus_di"] <= d["5m"]["minus_di"]:
            reject(symbol, "5M no confirma dirección LONG (+DI <= -DI)")
            return
        if direction == "SHORT" and d["5m"]["minus_di"] <= d["5m"]["plus_di"]:
            reject(symbol, "5M no confirma dirección SHORT (-DI <= +DI)")
            return

        zone = find_entry_zone(d, direction)
        if not zone:
            reject(symbol, "no se encontró zona técnica de entrada")
            return

        plan = make_plan(symbol, d, direction, score, zone)
        if not plan:
            reject(symbol, "no se pudo construir plan")
            return
        plan["motives"] = motives[:6]

        price = current_entry_price(symbol)
        if price is None:
            reject(symbol, "sin precio WebSocket")
            return

        zmin, zmax = plan["entry_min"], plan["entry_max"]
        a = d["4h"]["atr"] or d["1h"]["atr"]
        if not a:
            reject(symbol, "ATR inválido")
            return
        inside = zmin <= price <= zmax
        distance = min(abs(price-zmin), abs(price-zmax)) if not inside else 0
        if not inside and distance > a * PREALERT_ATR:
            reject(symbol, f"precio lejos de zona ({distance/a:.2f} ATR)")
            return

        if not inside:
            now = time.time()
            with state_lock:
                previous = state["prealerts"].get(symbol, 0)
                if now - previous >= PREALERT_TTL_SECONDS:
                    state["prealerts"][symbol] = now
                    save_state()
                    send_prealert(plan, price)
                    logger.info("PREALERTA %s %s score=%s precio=%s zona=%s-%s", direction, symbol, score, price, zmin, zmax)
            return

        with state_lock:
            if len(state["pending"]) + len(state["virtual_trades"]) >= MAX_TOTAL_EXPOSURE:
                reject(symbol, "exposición máxima alcanzada al final del análisis")
                return

        plan["status"] = "PENDING"
        plan["created_at"] = time.time()
        plan["expires_at"] = plan["created_at"] + SIGNAL_TTL_SECONDS

        # Anti-spam: no repetir la misma señal continuamente.
        with state_lock:
            last_sent = state["prealerts"].get(f"final:{symbol}", 0)
        if time.time() - last_sent < SIGNAL_COOLDOWN_SECONDS:
            reject(symbol, "cooldown de señal")
            return

        if send_final_signal(plan, price):
            with state_lock:
                state["pending"][symbol] = plan
                state["prealerts"][f"final:{symbol}"] = time.time()
            save_state()
            logger.info("🚨 SEÑAL FINAL %s %s score=%s precio=%s zona=%s-%s", direction, symbol, score, price, zmin, zmax)
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
    last_window_checked = None
    while not stop_event.is_set():
        try:
            reset_trading_day_if_needed()
            now = time.time()
            window = arg_window_key()

            # Escaneo TOP100 + ATR solo cada 6 horas.
            if not universe or now - last_scan >= SCAN_CACHE_SECONDS:
                with error_lock:
                    blocked = binance_block_until > time.time()
                if blocked:
                    stop_event.wait(30)
                    continue
                new_universe = select_universe()
                if new_universe:
                    universe = new_universe
                    with state_lock:
                        state["last_universe"] = time.time()
                    save_state()
                    ws_restart_event.set()
                    with market_lock:
                        for s in list(live_market):
                            if s not in universe:
                                live_market.pop(s, None)

            # Un único análisis por ventana argentina de 6 horas.
            if universe and window != last_window_checked:
                last_window_checked = window
                logger.info("ANÁLISIS VENTANA %s | universo=%s", window, universe)
                before_pending = len(state["pending"])
                for symbol in list(universe):
                    if stop_event.is_set():
                        break
                    process_symbol(symbol)
                    time.sleep(0.15)

                with state_lock:
                    state["last_analysis"] = time.time()
                    has_new_signal = len(state["pending"]) > before_pending
                save_state()

                if not has_new_signal:
                    msg = f"⚠️ Ventana {window} sin entradas: escaneé {universe} y 0 dieron -1%. Sigo en la próxima ventana"
                    send_telegram(msg)
                    logger.warning(msg)

            stop_event.wait(30)
        except Exception as exc:
            logger.exception("Error en ciclo principal: %s", exc)
            stop_event.wait(30)


# ------------------------- REPORTING -------------------------
def daily_report_loop():
    last_day = None
    while not stop_event.is_set():
        now = arg_now()
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
                "🤖 Tu bot está funcionando — 09:30 ARG\n\n"
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
        "binance_rest_block_seconds": max(0, int(binance_block_until - time.time())),
        "scan_age_seconds": max(0, int(time.time() - last_scan)) if last_scan else None,
        "trades_today": state.get("trades_today", 0),
        "max_trades_per_day": MAX_TRADES_PER_DAY,
        "open_positions": len(state.get("virtual_trades", {})),
        "max_open_positions": MAX_OPEN_POSITIONS,
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
            "trading_day": state.get("trading_day"),
            "trades_today": state.get("trades_today", 0),
            "last_trade_at": utc_iso(state["last_trade_at"]) if state.get("last_trade_at") else None,
            "used_windows": state.get("used_windows", []),
            "last_scan": utc_iso(state.get("last_scan")) if state.get("last_scan") else None,
        }
    with error_lock:
        safe["consecutive_binance_errors"] = consecutive_binance_errors
        safe["last_binance_error"] = last_binance_error
    return jsonify(safe)


@app.route("/diagnostico")
def diagnostico():
    if not authorized():
        return "No autorizado", 403
    with state_lock:
        return jsonify({
            "config": {
                "MIN_VOLUME_24H": MIN_VOLUME_24H, "MIN_SCORE": MIN_SCORE,
                "ENTRY_ATR_WIDTH": ENTRY_ATR_WIDTH, "ADX_MIN_5M": ADX_MIN_5M,
                "VOLUME_MIN_5M": VOLUME_MIN_5M, "SL_PCT": SL_PCT,
                "TP1_PCT": TP1_PCT, "TP2_PCT": TP2_PCT, "TP3_PCT": TP3_PCT,
                "leverage": LEVERAGE, "margin_usdt": MARGIN_USDT,
                "top_scan_symbols": TOP_SCAN_SYMBOLS, "final_universe": TOTAL_MONEDAS,
                "calm_symbols": CALM_SYMBOLS, "volatile_symbols": VOLATILE_SYMBOLS,
                "scan_cache_seconds": SCAN_CACHE_SECONDS, "scan_kline_sleep": SCAN_KLINE_SLEEP,
                "max_trades_per_day": MAX_TRADES_PER_DAY, "max_open_positions": MAX_OPEN_POSITIONS,
                "min_hours_between_trades": MIN_HOURS_BETWEEN_TRADES, "execution": "MANUAL_ONLY"
            },
            "universe": universe,
            "pending": list(state["pending"].keys()),
            "virtual_trades": list(state["virtual_trades"].keys()),
        })


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
    if ws_thread and ws_thread.is_alive():
        return
    load_state()
    send_telegram(
        "🤖 BOT FUTUROS PRO ONLINE (v4)\n\n"
        "📡 Precio en vivo: WebSocket Binance\n"
        "📊 Análisis: 1D / 4H / 1H / 15M / 5M\n"
        "🧠 Señales confirmadas con velas CERRADAS\n"
        f"⏱️ Caducidad señal final: {SIGNAL_TTL_SECONDS//60} min\n"
        f"🎯 SL {SL_PCT:.1f}% | TP1 {TP1_PCT:.1f}% | TP2 {TP2_PCT:.1f}% | TP3 {TP3_PCT:.1f}%\n"
        f"📦 Universo: TOP {TOP_SCAN_SYMBOLS} → {CALM_SYMBOLS} calmadas + {VOLATILE_SYMBOLS} volátiles = {TOTAL_MONEDAS}\n"
        f"💵 Margen: {MARGIN_USDT:.0f} USDT | Leverage: {LEVERAGE}x | Máx/día: {MAX_TRADES_PER_DAY} | Máx abiertas: {MAX_OPEN_POSITIONS}\n"
        "🔔 Ahora avisa si Binance falla o bloquea la IP.\n"
        "✅ Confirmar/cancelar con botones en el mensaje de Telegram.\n"
        "👤 SOLO SEÑALES: el bot NO ejecuta órdenes."
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

# Render/Gunicorn importa main:app, por lo que los workers deben arrancar también
# al importar el módulo. El guard evita arrancarlos dos veces en ejecución directa.
start_workers()

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=False)
