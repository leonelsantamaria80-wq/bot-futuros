import ccxt
import pandas as pd
import ta
import requests
import time
import os

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "TU_TOKEN_AQUI")
CHAT_ID = os.environ.get("CHAT_ID", "TU_CHAT_ID_AQUI")
GANANCIA_MINIMA_X5 = 1.0

MONEDAS = [
    "BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT",
    "DOGE/USDT", "ADA/USDT", "AVAX/USDT", "SHIB/USDT", "DOT/USDT",
    "LINK/USDT", "TRX/USDT", "MATIC/USDT", "LTC/USDT", "BCH/USDT",
    "UNI/USDT", "XLM/USDT", "ETC/USDT", "PEPE/USDT", "NEAR/USDT",
    "APT/USDT", "ARB/USDT", "OP/USDT", "SUI/USDT", "INJ/USDT",
    "RENDER/USDT", "FET/USDT", "WIF/USDT", "BONK/USDT", "FLOKI/USDT",
    "SEI/USDT", "TIA/USDT", "STX/USDT", "IMX/USDT", "AAVE/USDT"
]

exchange = ccxt.binance()
senal_enviada = {}

def obtener_datos(simbolo):
    try:
        ohlcv_15m = exchange.fetch_ohlcv(simbolo, '15m', limit=200)
        ohlcv_1h = exchange.fetch_ohlcv(simbolo, '1h', limit=200)
        ohlcv_4h = exchange.fetch_ohlcv(simbolo, '4h', limit=200)
        def procesar(ohlcv):
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['ema9'] = ta.trend.ema_indicator(df['close'], 9)
            df['ema20'] = ta.trend.ema_indicator(df['close'], 20)
            df['ema50'] = ta.trend.ema_indicator(df['close'], 50)
            df['rsi'] = ta.momentum.rsi(df['close'], 14)
            df['macd'] = ta.trend.macd_diff(df['close'])
            df['atr'] = ta.volatility.average_true_range(df['high'], df['low'], df['close'], 14)
            df['vwap'] = (df['close'] * df['volume']).cumsum() / df['volume'].cumsum()
            df['vol_avg'] = df['volume'].rolling(20).mean()
            return df
        return {"15m": procesar(ohlcv_15m), "1h": procesar(ohlcv_1h), "4h": procesar(ohlcv_4h)}
    except Exception as e:
        print(f"Error con {simbolo}: {e}")
        return None

def detectar_soportes_resistencias(df):
    return {"soporte_cercano": df['low'].tail(50).min(), "resistencia_cercana": df['high'].tail(50).max()}

def sugerir_apalancamiento(ganancia_sin_apalancar):
    if ganancia_sin_apalancar >= 4.0: return 10
    if ganancia_sin_apalancar >= 3.0: return 9
    if ganancia_sin_apalancar >= 2.5: return 8
    if ganancia_sin_apalancar >= 2.0: return 7
    if ganancia_sin_apalancar >= 1.5: return 6
    return 5

def analizar_senal(datos):
    df = datos["15m"]
    df_1h = datos["1h"]
    df_4h = datos["4h"]
    c = df.iloc[-1]
    ema50_4h = ta.trend.ema_indicator(df_4h['close'], 50).iloc[-1]
    ema200_4h = ta.trend.ema_indicator(df_4h['close'], 200).iloc[-1]
    ema50_1h = ta.trend.ema_indicator(df_1h['close'], 50).iloc[-1]
    niveles = detectar_soportes_resistencias(df)

    long_cond = (
        df_4h['close'].iloc[-1] > ema50_4h > ema200_4h and
        df_1h['close'].iloc[-1] > ema50_1h and
        c['close'] > c['ema50'] and c['ema9'] > c['ema20'] > c['ema50'] and
        c['close'] > c['vwap'] and c['volume'] > c['vol_avg'] and
        40 < c['rsi'] < 65 and c['macd'] > 0
    )
    if long_cond:
        entry = c['close']
        sl = niveles["soporte_cercano"]
        riesgo = entry - sl
        tp = max(entry + (riesgo * 1.5), entry + (c['atr'] * 3), niveles["resistencia_cercana"])
        if tp > entry + (riesgo * 4): tp = entry + (riesgo * 4)
        ganancia_base = ((tp - entry)/entry)*100
        lev = sugerir_apalancamiento(ganancia_base)
        ganancia_final = ganancia_base * lev
        if ganancia_final >= GANANCIA_MINIMA_X5:
            return {"tipo_operacion": "LONG", "entry": entry, "sl": sl, "tp": tp, "lev": lev, "ganancia": ganancia_final}

    short_cond = (
        df_4h['close'].iloc[-1] < ema50_4h < ema200_4h and
        df_1h['close'].iloc[-1] < ema50_1h and
        c['close'] < c['ema50'] and c['ema9'] < c['ema20'] < c['ema50'] and
        c['close'] < c['vwap'] and c['volume'] > c['vol_avg'] and
        35 < c['rsi'] < 60 and c['macd'] < 0
    )
    if short_cond:
        entry = c['close']
        sl = niveles["resistencia_cercana"]
        riesgo = sl - entry
        tp = min(entry - (riesgo * 1.5), entry - (c['atr'] * 3), niveles["soporte_cercano"])
        if tp < entry - (riesgo * 4): tp = entry - (riesgo * 4)
        ganancia_base = ((entry - tp)/entry)*100
        lev = sugerir_apalancamiento(ganancia_base)
        ganancia_final = ganancia_base * lev
        if ganancia_final >= GANANCIA_MINIMA_X5:
            return {"tipo_operacion": "SHORT", "entry": entry, "sl": sl, "tp": tp, "lev": lev, "ganancia": ganancia_final}
    return None

def enviar_telegram(moneda, senal):
    clave = f"{moneda}_{senal['tipo_operacion']}_{round(senal['entry'], 4)}"
    if senal_enviada.get(moneda) == clave: return
    nombre = moneda.split('/')[0]
    if senal['tipo_operacion'] == "LONG":
        mensaje = f"🟢 COMPRA LONG {moneda}\n\nCompra {nombre} a {senal['entry']:.6f} USDT\nStop Loss a {senal['sl']:.6f} USDT\nTP a {senal['tp']:.6f} USDT\nCompra en LONG\nApalancamiento sugerido: x{senal['lev']}\nGanancia estimada: {senal['ganancia']:.2f}% con x{senal['lev']}"
    else:
        mensaje = f"🔴 COMPRA SHORT {moneda}\n\nCompra {nombre} a {senal['entry']:.6f} USDT\nStop Loss a {senal['sl']:.6f} USDT\nTP a {senal['tp']:.6f} USDT\nCompra en SHORT\nApalancamiento sugerido: x{senal['lev']}\nGanancia estimada: {senal['ganancia']:.2f}% con x{senal['lev']}"
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": CHAT_ID, "text": mensaje})
    print(f"Enviada: {moneda} {senal['tipo_operacion']} x{senal['lev']}")
    senal_enviada[moneda] = clave

print("Bot iniciado - TP dinamico + Apalancamiento x5 a x10")
while True:
    for moneda in MONEDAS:
        datos = obtener_datos(moneda)
        if datos:
            senal = analizar_senal(datos)
            if senal:
                enviar_telegram(moneda, senal)
    time.sleep(60)
