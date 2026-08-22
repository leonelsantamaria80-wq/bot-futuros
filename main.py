import requests
import time
import threading
from flask import Flask

# ============================================================
# CONFIGURACIÓN
# ============================================================

TOKEN = "8746064456:AAGTiH-yfpwPwwjFTmqLuw4Z2-wg--eks-M"
CHAT_ID = "8898482159"

# Ganancia mínima que queremos obtener SOBRE EL MARGEN
MIN_GANANCIA_NETA = 1.5

# Comisión estimada por cada lado de la operación.
# EJEMPLO: 0.05% entrada + 0.05% salida = 0.10% total.
# CAMBIALA según la comisión real que tengas en Binance.
COMISION_POR_LADO = 0.05

# Apalancamiento
MIN_APALANCAMIENTO = 5
MAX_APALANCAMIENTO = 10

# Temporalidades
TIMEFRAMES = ["5m", "15m", "1h", "4h", "1d"]

# ============================================================
# MONEDAS
# ============================================================

MONEDAS = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "SHIBUSDT",
    "AVAXUSDT",
    "DOTUSDT",
    "LINKUSDT",
    "LTCUSDT",
    "MATICUSDT",
    "TRXUSDT",
    "UNIUSDT",
    "ATOMUSDT",
    "ETCUSDT",
    "FILUSDT",
    "XLMUSDT",
    "HBARUSDT",
    "NEARUSDT",
    "APTUSDT",
    "ARBUSDT",
    "OPUSDT",
    "INJUSDT",
    "RNDRUSDT",
    "PEPEUSDT",
    "SUIUSDT",
    "TIAUSDT",
    "SEIUSDT",
    "WIFUSDT",
    "BONKUSDT",
    "FLOKIUSDT",
    "JUPUSDT",
    "ENAUSDT",
    "WUSDT",
    "PYTHUSDT",
    "STRKUSDT",
    "LDOUSDT",
    "STXUSDT"
]

# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)

# Guarda la última señal enviada de cada moneda.
# Ejemplo:
# BTCUSDT = LONG
#
# De esta forma no manda LONG, LONG, LONG cada 3 minutos.
ultima_senal_enviada = {}


# ============================================================
# TELEGRAM
# ============================================================

def send(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            json={
                "chat_id": CHAT_ID,
                "text": msg
            },
            timeout=15
        )
    except Exception as e:
        print("Error Telegram:", e)


# ============================================================
# DATOS BINANCE
# ============================================================

def get_data(symbol, interval):
    try:
        url = (
            "https://data-api.binance.vision/api/v3/klines"
            f"?symbol={symbol}&interval={interval}&limit=100"
        )

        respuesta = requests.get(url, timeout=10)

        if respuesta.status_code != 200:
            return None, None, None, None

        d = respuesta.json()

        if not isinstance(d, list) or len(d) < 50:
            return None, None, None, None

        closes = [float(x[4]) for x in d]
        highs = [float(x[2]) for x in d]
        lows = [float(x[3]) for x in d]
        vols = [float(x[5]) for x in d]

        return closes, highs, lows, vols

    except Exception as e:
        print("Error datos", symbol, interval, e)
        return None, None, None, None


# ============================================================
# EMA
# ============================================================

def ema(arr, p):
    if not arr:
        return 0

    k = 2 / (p + 1)
    e = arr[0]

    for x in arr[1:]:
        e = x * k + e * (1 - k)

    return e


def calc_ema_list(arr, p):
    if not arr:
        return []

    k = 2 / (p + 1)

    lista = [arr[0]]

    for x in arr[1:]:
        lista.append(x * k + lista[-1] * (1 - k))

    return lista


# ============================================================
# RSI
# ============================================================

def calc_rsi(closes, period=14):

    if len(closes) < period + 1:
        return 50

    gains = 0
    losses = 0

    for i in range(-period, 0):

        d = closes[i] - closes[i - 1]

        if d > 0:
            gains += d
        else:
            losses -= d

    if losses == 0:
        return 100

    rs = gains / losses

    return 100 - (100 / (1 + rs))


# ============================================================
# MACD
# ============================================================

def calc_macd(closes):

    if len(closes) < 35:
        return 0, 0

    e12 = calc_ema_list(closes, 12)
    e26 = calc_ema_list(closes, 26)

    macd = [
        a - b
        for a, b in zip(e12, e26)
    ]

    sig = calc_ema_list(macd, 9)

    return macd[-1], sig[-1]


# ============================================================
# ATR
# ============================================================

def calc_atr(highs, lows, closes, period=14):

    if len(closes) < period + 1:
        return 0

    trs = []

    for i in range(1, len(closes)):

        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1])
        )

        trs.append(tr)

    return sum(trs[-period:]) / period


# ============================================================
# SOPORTE Y RESISTENCIA
# ============================================================

def calc_soporte_resistencia(highs, lows):

    if len(lows) < 20:
        return lows[-1], highs[-1]

    soporte = min(lows[-20:])
    resistencia = max(highs[-20:])

    return soporte, resistencia


# ============================================================
# ADX
# ============================================================

def calc_adx(highs, lows, closes, period=14):

    try:

        tr_list = []
        plus_dm_list = []
        minus_dm_list = []

        for i in range(1, len(closes)):

            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1])
            )

            up = highs[i] - highs[i - 1]
            down = lows[i - 1] - lows[i]

            plus_dm = (
                up
                if up > down and up > 0
                else 0
            )

            minus_dm = (
                down
                if down > up and down > 0
                else 0
            )

            tr_list.append(tr)
            plus_dm_list.append(plus_dm)
            minus_dm_list.append(minus_dm)

        if len(tr_list) < period:
            return 0, 0, 0

        tr14 = sum(tr_list[-period:])
        plus14 = sum(plus_dm_list[-period:])
        minus14 = sum(minus_dm_list[-period:])

        if tr14 == 0:
            return 0, 0, 0

        plus_di = 100 * plus14 / tr14
        minus_di = 100 * minus14 / tr14

        if plus_di + minus_di == 0:
            dx = 0
        else:
            dx = (
                100
                * abs(plus_di - minus_di)
                / (plus_di + minus_di)
            )

        return dx, plus_di, minus_di

    except Exception:
        return 0, 0, 0


# ============================================================
# ANÁLISIS DE CADA TEMPORALIDAD
# ============================================================

def check_signal_pro(closes, highs, lows, vols):

    if not closes or len(closes) < 50:
        return None

    # EMA actual
    e9_now = ema(closes[-9:], 9)
    e21_now = ema(closes[-21:], 21)

    # EMA anterior
    e9_prev = ema(closes[-10:-1], 9)
    e21_prev = ema(closes[-22:-1], 21)

    # Indicadores
    rsi = calc_rsi(closes)

    macd, sig = calc_macd(closes)

    atr = calc_atr(
        highs,
        lows,
        closes
    )

    sop, res = calc_soporte_resistencia(
        highs,
        lows
    )

    adx, plus_di, minus_di = calc_adx(
        highs,
        lows,
        closes
    )

    # ========================================================
    # VOLUMEN
    # ========================================================

    vol_prom = sum(vols[-20:]) / 20

    vol_ok = vols[-1] > vol_prom * 1.2

    # ========================================================
    # FUERZA
    # ========================================================

    fuerza = (
        abs(e9_now - e21_now)
        / closes[-1]
        * 100
    )

    # ========================================================
    # LONG
    # ========================================================

    if (
        e9_prev < e21_prev
        and e9_now > e21_now
        and 35 < rsi < 68
        and macd > sig
        and vol_ok
        and adx > 20
        and plus_di > minus_di
    ):

        return (
            "LONG",
            rsi,
            fuerza,
            atr,
            sop,
            res,
            adx
        )

    # ========================================================
    # SHORT
    # ========================================================

    if (
        e9_prev > e21_prev
        and e9_now < e21_now
        and rsi > 35
        and macd < sig
        and vol_ok
        and adx > 20
        and minus_di > plus_di
    ):

        return (
            "SHORT",
            rsi,
            fuerza,
            atr,
            sop,
            res,
            adx
        )

    return None


# ============================================================
# APALANCAMIENTO
# ============================================================

def get_leverage(conf, fuerza, adx):

    # Señal muy fuerte
    if conf >= 5 and adx >= 30 and fuerza >= 0.50:
        return 10

    # Señal fuerte
    if conf >= 5 and adx >= 27:
        return 9

    if conf >= 4 and adx >= 28:
        return 8

    if conf >= 4 and adx >= 24:
        return 7

    if conf >= 3 and adx >= 25:
        return 6

    return 5


# ============================================================
# CÁLCULO DE GANANCIA NETA
# ============================================================

def calcular_ganancia_neta(precio, tp, leverage):

    movimiento = abs(tp - precio) / precio * 100

    ganancia_bruta = movimiento * leverage

    comisiones = COMISION_POR_LADO * 2

    ganancia_neta = ganancia_bruta - comisiones

    return movimiento, ganancia_bruta, ganancia_neta


# ============================================================
# FORMATO DE PRECIO
# ============================================================

def formato_precio(precio):

    if precio < 0.000001:
        return ".10f"

    if precio < 0.01:
        return ".8f"

    if precio < 1:
        return ".5f"

    if precio < 100:
        return ".3f"

    return ".2f"


# ============================================================
# BOT PRINCIPAL
# ============================================================

def bot_loop():

    print("==========================================")
    print(" BOT FUTUROS 40 MONEDAS ONLINE")
    print(" Apalancamiento: x5 - x10")
    print(" Ganancia mínima neta: 1.5%")
    print("==========================================")

    send(
        "🤖 BOT FUTUROS ONLINE\n\n"
        "40 monedas\n"
        "5 temporalidades\n"
        "LONG + SHORT\n"
        "Apalancamiento sugerido x5-x10\n"
        "Objetivo mínimo neto 1.5%"
    )

    while True:

        for moneda in MONEDAS:

            long_c = 0
            short_c = 0

            señales = []

            precio_actual = None

            # =================================================
            # ANALIZAR LAS 5 TEMPORALIDADES
            # =================================================

            for tf in TIMEFRAMES:

                closes, highs, lows, vols = get_data(
                    moneda,
                    tf
                )

                if not closes:
                    continue

                if precio_actual is None:
                    precio_actual = closes[-1]

                resultado = check_signal_pro(
                    closes,
                    highs,
                    lows,
                    vols
                )

                if resultado:

                    (
                        tipo,
                        rsi,
                        fuerza,
                        atr,
                        sop,
                        resis,
                        adx
                    ) = resultado

                    señales.append(
                        (
                            tf,
                            tipo,
                            rsi,
                            fuerza,
                            atr,
                            sop,
                            resis,
                            adx
                        )
                    )

                    if tipo == "LONG":
                        long_c += 1

                    else:
                        short_c += 1

                time.sleep(0.15)

            # =================================================
            # NO HAY 3 TEMPORALIDADES COINCIDENTES
            # =================================================

            if long_c < 3 and short_c < 3:

                # Importante:
                # si la señal desapareció,
                # permitimos una nueva señal futura.

                ultima_senal_enviada.pop(
                    moneda,
                    None
                )

                continue

            # =================================================
            # DETERMINAR LONG / SHORT
            # =================================================

            if long_c >= 3 and long_c >= short_c:

                tipo_final = "LONG"
                conf = long_c

            elif short_c >= 3:

                tipo_final = "SHORT"
                conf = short_c

            else:
                continue

            # =================================================
            # EVITAR REPETIR LA MISMA SEÑAL
            # =================================================

            if ultima_senal_enviada.get(moneda) == tipo_final:

                continue

            señales_final = [
                s
                for s in señales
                if s[1] == tipo_final
            ]

            if not señales_final:
                continue

            # =================================================
            # PROMEDIOS
            # =================================================

            fuerza_prom = (
                sum(s[3] for s in señales_final)
                / len(señales_final)
            )

            atr_prom = (
                sum(s[4] for s in señales_final)
                / len(señales_final)
            )

            adx_prom = (
                sum(s[7] for s in señales_final)
                / len(señales_final)
            )

            rsi_prom = (
                sum(s[2] for s in señales_final)
                / len(señales_final)
            )

            precio = precio_actual

            if not precio or atr_prom <= 0:
                continue

            # =================================================
            # APALANCAMIENTO
            # =================================================

            lev = get_leverage(
                conf,
                fuerza_prom,
                adx_prom
            )

            lev = max(
                MIN_APALANCAMIENTO,
                min(
                    lev,
                    MAX_APALANCAMIENTO
                )
            )

            # =================================================
            # STOP LOSS / TAKE PROFIT
            # =================================================

            if tipo_final == "LONG":

                sl = precio - (
                    atr_prom * 1.5
                )

                tp = precio + (
                    atr_prom * 2.5
                )

            else:

                sl = precio + (
                    atr_prom * 1.5
                )

                tp = precio - (
                    atr_prom * 2.5
                )

            if tp <= 0 or sl <= 0:
                continue

            # =================================================
            # GANANCIA
            # =================================================

            (
                movimiento,
                ganancia_bruta,
                ganancia_neta
            ) = calcular_ganancia_neta(
                precio,
                tp,
                lev
            )

            # =================================================
            # FILTRO 1.5% NETO
            # =================================================

            if ganancia_neta < MIN_GANANCIA_NETA:

                print(
                    f"{moneda} {tipo_final} descartada - "
                    f"ganancia neta {ganancia_neta:.2f}%"
                )

                continue

            # =================================================
            # TEMPORALIDADES
            # =================================================

            estados_tf = []

            for tf in TIMEFRAMES:

                encontrada = [
                    s
                    for s in señales
                    if s[0] == tf
                ]

                if encontrada:

                    if encontrada[0][1] == "LONG":
                        estados_tf.append(
                            f"{tf}: 🟢 LONG"
                        )
                    else:
                        estados_tf.append(
                            f"{tf}: 🔴 SHORT"
                        )

                else:

                    estados_tf.append(
                        f"{tf}: ⚪"
                    )

            # =================================================
            # CONFIANZA
            # =================================================

            confianza = int(
                (conf / 5) * 10
            )

            if adx_prom >= 30:
                confianza += 1

            if confianza > 10:
                confianza = 10

            # =================================================
            # FORMATO
            # =================================================

            fmt = formato_precio(precio)

            base = moneda.replace(
                "USDT",
                ""
            )

            # =================================================
            # MENSAJE LONG
            # =================================================

            if tipo_final == "LONG":

                msg = (
                    f"🟢 SEÑAL LONG — {base}/USDT\n\n"

                    f"💰 Entrada: {precio:{fmt}}\n"
                    f"🎯 Take Profit: {tp:{fmt}}\n"
                    f"🛑 Stop Loss: {sl:{fmt}}\n\n"

                    f"⚡ Apalancamiento sugerido: x{lev}\n"
                    f"📊 Confianza: {confianza}/10\n\n"

                    f"📈 RSI: {rsi_prom:.1f}\n"
                    f"📊 ADX: {adx_prom:.1f}\n"
                    f"💪 Fuerza: {fuerza_prom:.2f}%\n\n"

                    f"📈 Movimiento TP: +{movimiento:.2f}%\n"
                    f"💵 Ganancia bruta estimada: "
                    f"+{ganancia_bruta:.2f}%\n"
                    f"💸 Comisiones estimadas: "
                    f"-{COMISION_POR_LADO * 2:.2f}%\n"
                    f"✅ Ganancia neta estimada: "
                    f"+{ganancia_neta:.2f}%\n\n"

                    f"⏱ TEMPORALIDADES\n"
                    + "\n".join(estados_tf)
                )

            # =================================================
            # MENSAJE SHORT
            # =================================================

            else:

                msg = (
                    f"🔴 SEÑAL SHORT — {base}/USDT\n\n"

                    f"💰 Entrada: {precio:{fmt}}\n"
                    f"🎯 Take Profit: {tp:{fmt}}\n"
                    f"🛑 Stop Loss: {sl:{fmt}}\n\n"

                    f"⚡ Apalancamiento sugerido: x{lev}\n"
                    f"📊 Confianza: {confianza}/10\n\n"

                    f"📉 RSI: {rsi_prom:.1f}\n"
                    f"📊 ADX: {adx_prom:.1f}\n"
                    f"💪 Fuerza: {fuerza_prom:.2f}%\n\n"

                    f"📉 Movimiento TP: +{movimiento:.2f}%\n"
                    f"💵 Ganancia bruta estimada: "
                    f"+{ganancia_bruta:.2f}%\n"
                    f"💸 Comisiones estimadas: "
                    f"-{COMISION_POR_LADO * 2:.2f}%\n"
                    f"✅ Ganancia neta estimada: "
                    f"+{ganancia_neta:.2f}%\n\n"

                    f"⏱ TEMPORALIDADES\n"
                    + "\n".join(estados_tf)
                )

            # =================================================
            # ENVIAR TELEGRAM
            # =================================================

            send(msg)

            # Guardamos la señal para no repetirla
            ultima_senal_enviada[moneda] = tipo_final

            print(
                f"SEÑAL ENVIADA: "
                f"{moneda} {tipo_final} "
                f"x{lev} "
                f"neto {ganancia_neta:.2f}%"
            )

        # =====================================================
        # ESPERAR 3 MINUTOS
        # =====================================================

        print("Esperando 3 minutos...")
        time.sleep(180)


# ============================================================
# PÁGINA PRINCIPAL
# ============================================================

@app.route("/")
def home():

    return (
        "BOT FUTUROS 40 MONEDAS ONLINE"
    )


# ============================================================
# TEST TELEGRAM
# ============================================================

@app.route("/test")
def test():

    send(
        "🟢 SEÑAL LONG — BTC/USDT\n\n"

        "💰 Entrada: 112000.00\n"
        "🎯 Take Profit: 113500.00\n"
        "🛑 Stop Loss: 111300.00\n\n"

        "⚡ Apalancamiento sugerido: x5\n"
        "📊 Confianza: 9/10\n\n"

        "📈 RSI: 57.8\n"
        "📊 ADX: 31.4\n"
        "💪 Fuerza: 0.62%\n\n"

        "📈 Movimiento TP: +1.34%\n"
        "💵 Ganancia bruta estimada: +6.70%\n"
        "💸 Comisiones estimadas: -0.10%\n"
        "✅ Ganancia neta estimada: +6.60%\n\n"

        "⏱ TEMPORALIDADES\n"
        "5m: 🟢 LONG\n"
        "15m: 🟢 LONG\n"
        "1h: 🟢 LONG\n"
        "4h: 🟢 LONG\n"
        "1d: 🟡"
    )

    return "TEST ENVIADO"


# ============================================================
# INICIAR BOT
# ============================================================

threading.Thread(
    target=bot_loop,
    daemon=True
).start()


if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=10000
    )
