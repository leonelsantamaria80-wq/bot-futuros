import os
import time
import threading
import requests
from flask import Flask

# ============================================================
# BOT FUTUROS CRYPTO - SEÑALES TELEGRAM
# ============================================================
#
# IMPORTANTE:
# Este bot NO ejecuta operaciones.
# Solamente analiza el mercado y manda señales a Telegram.
#
# Mercado:
# Binance USDⓈ-M Futures
#
# Temporalidades:
# 1D / 4H / 1H / 15M / 5M
#
# ============================================================


# ============================================================
# CONFIGURACION
# ============================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

BINANCE_URL = "https://fapi.binance.com"

app = Flask(__name__)

session = requests.Session()


# ============================================================
# PARAMETROS DEL BOT
# ============================================================

TOTAL_MONEDAS = 50

MONEDAS_BAJA_VOLATILIDAD = 40
MONEDAS_ALTA_VOLATILIDAD = 10

INTERVALO_ANALISIS = 180

MIN_SCORE = 7

MIN_LEVERAGE = 5
MAX_LEVERAGE = 10

MIN_GANANCIA_NETA = 1.50

# Estimación de comisión entrada + salida.
# Se puede modificar posteriormente según tu comisión real.
COMISION_TOTAL = 0.10

# Distancia máxima aproximada para enviar prealerta.
PREALERTA_ATR = 1.50

# ============================================================
# CONTROL DE SEÑALES REPETIDAS
# ============================================================

señales_activas = {}

señales_lock = threading.Lock()


# ============================================================
# TELEGRAM
# ============================================================

def enviar_telegram(mensaje):

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:

        print("Telegram no configurado.")

        return False

    try:

        url = (
            "https://api.telegram.org/bot"
            + TELEGRAM_TOKEN
            + "/sendMessage"
        )

        respuesta = session.post(
            url,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": mensaje
            },
            timeout=15
        )

        if respuesta.status_code != 200:

            print(
                "Error Telegram:",
                respuesta.text
            )

            return False

        return True

    except Exception as e:

        print(
            "Error enviando Telegram:",
            e
        )

        return False


# ============================================================
# BINANCE REQUEST
# ============================================================

def binance_get(endpoint, params=None):

    try:

        respuesta = session.get(
            BINANCE_URL + endpoint,
            params=params,
            timeout=10
        )

        if respuesta.status_code != 200:

            print(
                "Error Binance:",
                respuesta.status_code,
                endpoint
            )

            return None

        return respuesta.json()

    except Exception as e:

        print(
            "Error conexión Binance:",
            e
        )

        return None


# ============================================================
# CONTRATOS DISPONIBLES
# ============================================================

def obtener_contratos():

    data = binance_get(
        "/fapi/v1/exchangeInfo"
    )

    if not data:

        return []

    resultado = []

    for item in data.get("symbols", []):

        try:

            if item.get("status") != "TRADING":
                continue

            if item.get("contractType") != "PERPETUAL":
                continue

            if item.get("quoteAsset") != "USDT":
                continue

            symbol = item.get("symbol")

            if symbol and symbol.endswith("USDT"):

                resultado.append(symbol)

        except Exception:

            continue

    return resultado


# ============================================================
# VOLUMEN 24 HORAS
# ============================================================

def obtener_volumenes():

    data = binance_get(
        "/fapi/v1/ticker/24hr"
    )

    if not data:

        return {}

    resultado = {}

    for item in data:

        try:

            symbol = item.get("symbol")

            if not symbol:
                continue

            if not symbol.endswith("USDT"):
                continue

            volumen = float(
                item.get("quoteVolume", 0)
            )

            resultado[symbol] = volumen

        except Exception:

            continue

    return resultado


# ============================================================
# KLINES
# ============================================================

def obtener_velas(
    symbol,
    timeframe,
    limit=120
):

    data = binance_get(
        "/fapi/v1/klines",
        {
            "symbol": symbol,
            "interval": timeframe,
            "limit": limit
        }
    )

    if not data:
        return None

    velas = []

    try:

        for x in data:

            velas.append(
                {
                    "open": float(x[1]),
                    "high": float(x[2]),
                    "low": float(x[3]),
                    "close": float(x[4]),
                    "volume": float(x[5])
                }
            )

        return velas

    except Exception:

        return None


# ============================================================
# PRECIO MARK
# ============================================================

def obtener_mark_price(symbol):

    data = binance_get(
        "/fapi/v1/premiumIndex",
        {
            "symbol": symbol
        }
    )

    if not data:

        return None

    try:

        return float(
            data["markPrice"]
        )

    except Exception:

        return None


# ============================================================
# OPEN INTEREST
# ============================================================

def obtener_open_interest(symbol):

    data = binance_get(
        "/fapi/v1/openInterest",
        {
            "symbol": symbol
        }
    )

    if not data:

        return None

    try:

        return float(
            data["openInterest"]
        )

    except Exception:

        return None


# ============================================================
# FUNDING RATE
# ============================================================

def obtener_funding(symbol):

    data = binance_get(
        "/fapi/v1/premiumIndex",
        {
            "symbol": symbol
        }
    )

    if not data:

        return None

    try:

        return float(
            data["lastFundingRate"]
        ) * 100

    except Exception:

        return None


# ============================================================
# EMA
# ============================================================

def calcular_ema(valores, periodo):

    if len(valores) < periodo:

        return None

    k = 2 / (periodo + 1)

    resultado = valores[0]

    for valor in valores[1:]:

        resultado = (
            valor * k
            + resultado * (1 - k)
        )

    return resultado


def calcular_ema_series(valores, periodo):

    if not valores:

        return []

    k = 2 / (periodo + 1)

    resultado = [valores[0]]

    for valor in valores[1:]:

        resultado.append(
            valor * k
            + resultado[-1] * (1 - k)
        )

    return resultado


# ============================================================
# RSI
# ============================================================

def calcular_rsi(
    cierres,
    periodo=14
):

    if len(cierres) < periodo + 1:

        return 50

    ganancias = []
    perdidas = []

    for i in range(1, len(cierres)):

        cambio = (
            cierres[i]
            - cierres[i - 1]
        )

        if cambio > 0:

            ganancias.append(cambio)
            perdidas.append(0)

        else:

            ganancias.append(0)
            perdidas.append(abs(cambio))

    promedio_ganancia = (
        sum(ganancias[-periodo:])
        / periodo
    )

    promedio_perdida = (
        sum(perdidas[-periodo:])
        / periodo
    )

    if promedio_perdida == 0:

        return 100

    rs = (
        promedio_ganancia
        / promedio_perdida
    )

    return 100 - (
        100 / (1 + rs)
    )


# ============================================================
# MACD
# ============================================================

def calcular_macd(cierres):

    if len(cierres) < 35:

        return 0, 0

    ema12 = calcular_ema_series(
        cierres,
        12
    )

    ema26 = calcular_ema_series(
        cierres,
        26
    )

    linea_macd = []

    for a, b in zip(
        ema12,
        ema26
    ):

        linea_macd.append(
            a - b
        )

    señal = calcular_ema_series(
        linea_macd,
        9
    )

    return (
        linea_macd[-1],
        señal[-1]
    )


# ============================================================
# ATR
# ============================================================

def calcular_atr(
    velas,
    periodo=14
):

    if len(velas) < periodo + 2:

        return None

    tr = []

    for i in range(
        1,
        len(velas)
    ):

        actual = velas[i]

        anterior = velas[i - 1]

        rango = max(
            actual["high"]
            - actual["low"],

            abs(
                actual["high"]
                - anterior["close"]
            ),

            abs(
                actual["low"]
                - anterior["close"]
            )
        )

        tr.append(rango)

    return (
        sum(tr[-periodo:])
        / periodo
    )


# ============================================================
# ADX
# ============================================================

def calcular_adx(
    velas,
    periodo=14
):

    if len(velas) < periodo + 2:

        return 0, 0, 0

    trs = []
    positivos = []
    negativos = []

    for i in range(
        1,
        len(velas)
    ):

        actual = velas[i]
        anterior = velas[i - 1]

        tr = max(
            actual["high"]
            - actual["low"],

            abs(
                actual["high"]
                - anterior["close"]
            ),

            abs(
                actual["low"]
                - anterior["close"]
            )
        )

        movimiento_up = (
            actual["high"]
            - anterior["high"]
        )

        movimiento_down = (
            anterior["low"]
            - actual["low"]
        )

        plus_dm = (
            movimiento_up
            if (
                movimiento_up
                > movimiento_down
                and movimiento_up > 0
            )
            else 0
        )

        minus_dm = (
            movimiento_down
            if (
                movimiento_down
                > movimiento_up
                and movimiento_down > 0
            )
            else 0
        )

        trs.append(tr)
        positivos.append(plus_dm)
        negativos.append(minus_dm)

    tr14 = sum(
        trs[-periodo:]
    )

    plus14 = sum(
        positivos[-periodo:]
    )

    minus14 = sum(
        negativos[-periodo:]
    )

    if tr14 == 0:

        return 0, 0, 0

    plus_di = (
        100 * plus14 / tr14
    )

    minus_di = (
        100 * minus14 / tr14
    )

    suma = (
        plus_di
        + minus_di
    )

    if suma == 0:

        return 0, plus_di, minus_di

    dx = (
        100
        * abs(
            plus_di
            - minus_di
        )
        / suma
    )

    return (
        dx,
        plus_di,
        minus_di
    )


# ============================================================
# VWAP
# ============================================================

def calcular_vwap(velas):

    volumen_total = 0
    precio_volumen = 0

    for vela in velas[-50:]:

        precio_tipico = (
            vela["high"]
            + vela["low"]
            + vela["close"]
        ) / 3

        volumen = vela["volume"]

        volumen_total += volumen

        precio_volumen += (
            precio_tipico
            * volumen
        )

    if volumen_total == 0:

        return None

    return (
        precio_volumen
        / volumen_total
    )


# ============================================================
# SOPORTE Y RESISTENCIA
# ============================================================

def calcular_niveles(velas):

    if len(velas) < 30:

        return None, None

    ultimas = velas[-80:]

    soporte = min(
        x["low"]
        for x in ultimas
    )

    resistencia = max(
        x["high"]
        for x in ultimas
    )

    return (
        soporte,
        resistencia
    )


# ============================================================
# FIBONACCI
# ============================================================

def calcular_fibonacci(velas):

    if len(velas) < 50:

        return {}

    ultimas = velas[-100:]

    maximo = max(
        x["high"]
        for x in ultimas
    )

    minimo = min(
        x["low"]
        for x in ultimas
    )

    diferencia = (
        maximo
        - minimo
    )

    if diferencia <= 0:

        return {}

    return {

        "0.382":
            maximo
            - diferencia * 0.382,

        "0.500":
            maximo
            - diferencia * 0.500,

        "0.618":
            maximo
            - diferencia * 0.618,

        "0.786":
            maximo
            - diferencia * 0.786
    }


# ============================================================
# VOLUMEN
# ============================================================

def calcular_ratio_volumen(velas):

    if len(velas) < 25:

        return 1

    actual = velas[-1]["volume"]

    promedio = (
        sum(
            x["volume"]
            for x in velas[-21:-1]
        )
        / 20
    )

    if promedio == 0:

        return 1

    return (
        actual
        / promedio
    )


# ============================================================
# TENDENCIA
# ============================================================

def determinar_tendencia(velas):

    cierres = [
        x["close"]
        for x in velas
    ]

    ema20 = calcular_ema(
        cierres[-80:],
        20
    )

    ema50 = calcular_ema(
        cierres[-100:],
        50
    )

    precio = cierres[-1]

    if not ema20 or not ema50:

        return "NEUTRAL"

    if (
        precio > ema20
        and ema20 > ema50
    ):

        return "ALCISTA"

    if (
        precio < ema20
        and ema20 < ema50
    ):

        return "BAJISTA"

    return "NEUTRAL"


# ============================================================
# DIVERGENCIA RSI
# ============================================================

def detectar_divergencia(velas):

    if len(velas) < 40:

        return None

    cierres = [
        x["close"]
        for x in velas
    ]

    rsi_anterior = calcular_rsi(
        cierres[:-10]
    )

    rsi_actual = calcular_rsi(
        cierres
    )

    precio_anterior = (
        cierres[-10]
    )

    precio_actual = (
        cierres[-1]
    )

    if (
        precio_actual
        < precio_anterior
        and rsi_actual
        > rsi_anterior
    ):

        return "ALCISTA"

    if (
        precio_actual
        > precio_anterior
        and rsi_actual
        < rsi_anterior
    ):

        return "BAJISTA"

    return None


# ============================================================
# ANALISIS DE UNA TEMPORALIDAD
# ============================================================

def analizar_temporalidad(
    velas
):

    cierres = [
        x["close"]
        for x in velas
    ]

    macd_linea, macd_signal = (
        calcular_macd(cierres)
    )

    adx_value, plus_di, minus_di = (
        calcular_adx(velas)
    )

    soporte, resistencia = (
        calcular_niveles(velas)
    )

    atr_value = calcular_atr(
        velas
    )

    return {

        "precio":
            cierres[-1],

        "rsi":
            calcular_rsi(cierres),

        "ema20":
            calcular_ema(
                cierres,
                20
            ),

        "ema50":
            calcular_ema(
                cierres,
                50
            ),

        "macd":
            macd_linea,

        "macd_signal":
            macd_signal,

        "adx":
            adx_value,

        "plus_di":
            plus_di,

        "minus_di":
            minus_di,

        "atr":
            atr_value,

        "atr_pct":
            (
                atr_value
                / cierres[-1]
                * 100
            )
            if atr_value
            else 0,

        "vwap":
            calcular_vwap(velas),

        "soporte":
            soporte,

        "resistencia":
            resistencia,

        "fibonacci":
            calcular_fibonacci(velas),

        "volumen_ratio":
            calcular_ratio_volumen(
                velas
            ),

        "tendencia":
            determinar_tendencia(
                velas
            ),

        "divergencia":
            detectar_divergencia(
                velas
            )
    }


# ============================================================
# ANALIZAR MONEDA COMPLETA
# ============================================================

def analizar_moneda(symbol):

    resultado = {}

    temporalidades = [
        "1d",
        "4h",
        "1h",
        "15m",
        "5m"
    ]

    for tf in temporalidades:

        velas = obtener_velas(
            symbol,
            tf,
            120
        )

        if not velas:

            return None

        resultado[tf] = (
            analizar_temporalidad(
                velas
            )
        )

        time.sleep(0.05)

    resultado["futures"] = {

        "mark":
            obtener_mark_price(
                symbol
            ),

        "open_interest":
            obtener_open_interest(
                symbol
            ),

        "funding":
            obtener_funding(
                symbol
            )
    }

    return resultado


# ============================================================
# SELECCIONAR 50 MONEDAS
# ============================================================

def seleccionar_monedas():

    print(
        "Seleccionando monedas..."
    )

    contratos = obtener_contratos()

    if not contratos:

        return []

    volumenes = obtener_volumenes()

    candidatos = []

    for symbol in contratos:

        volumen = volumenes.get(
            symbol,
            0
        )

        # Evitar monedas con muy poca liquidez.
        if volumen < 20_000_000:

            continue

        velas = obtener_velas(
            symbol,
            "4h",
            80
        )

        if not velas:

            continue

        atr_value = calcular_atr(
            velas
        )

        if not atr_value:

            continue

        precio = velas[-1]["close"]

        if precio <= 0:

            continue

        volatilidad = (
            atr_value
            / precio
            * 100
        )

        candidatos.append(
            {
                "symbol": symbol,
                "volatilidad": volatilidad,
                "volumen": volumen
            }
        )

        time.sleep(0.03)

    if not candidatos:

        return []

    # Ordenar por volatilidad.
    candidatos.sort(
        key=lambda x:
        x["volatilidad"]
    )

    tranquilas = candidatos[
        :MONEDAS_BAJA_VOLATILIDAD
    ]

    volatiles = candidatos[
        -MONEDAS_ALTA_VOLATILIDAD:
    ]

    seleccionadas = (
        tranquilas
        + volatiles
    )

    # Quitar duplicados.
    resultado = []

    vistos = set()

    for x in seleccionadas:

        symbol = x["symbol"]

        if symbol not in vistos:

            vistos.add(symbol)

            resultado.append(symbol)

    print(
        "Monedas seleccionadas:",
        len(resultado)
    )

    return resultado[:TOTAL_MONEDAS]


# ============================================================
# CONFLUENCIA
# ============================================================

def calcular_confluencia(data):

    long_score = 0
    short_score = 0

    long_motivos = []
    short_motivos = []

    # --------------------------------------------------------
    # 1D
    # --------------------------------------------------------

    if data["1d"]["tendencia"] == "ALCISTA":

        long_score += 2

        long_motivos.append(
            "Tendencia 1D alcista"
        )

    elif data["1d"]["tendencia"] == "BAJISTA":

        short_score += 2

        short_motivos.append(
            "Tendencia 1D bajista"
        )

    # --------------------------------------------------------
    # 4H
    # --------------------------------------------------------

    if data["4h"]["tendencia"] == "ALCISTA":

        long_score += 2

        long_motivos.append(
            "Estructura 4H alcista"
        )

    elif data["4h"]["tendencia"] == "BAJISTA":

        short_score += 2

        short_motivos.append(
            "Estructura 4H bajista"
        )

    # --------------------------------------------------------
    # 1H RSI
    # --------------------------------------------------------

    rsi = data["1h"]["rsi"]

    if rsi <= 40:

        long_score += 1

        long_motivos.append(
            f"RSI 1H bajo ({rsi:.1f})"
        )

    elif rsi >= 60:

        short_score += 1

        short_motivos.append(
            f"RSI 1H alto ({rsi:.1f})"
        )

    # --------------------------------------------------------
    # Divergencia
    # --------------------------------------------------------

    divergencia = (
        data["1h"]["divergencia"]
    )

    if divergencia == "ALCISTA":

        long_score += 1

        long_motivos.append(
            "Divergencia RSI alcista"
        )

    elif divergencia == "BAJISTA":

        short_score += 1

        short_motivos.append(
            "Divergencia RSI bajista"
        )

    # --------------------------------------------------------
    # MACD
    # --------------------------------------------------------

    macd = data["1h"]["macd"]

    macd_signal = (
        data["1h"]["macd_signal"]
    )

    if macd > macd_signal:

        long_score += 1

        long_motivos.append(
            "MACD 1H alcista"
        )

    elif macd < macd_signal:

        short_score += 1

        short_motivos.append(
            "MACD 1H bajista"
        )

    # --------------------------------------------------------
    # ADX
    # --------------------------------------------------------

    adx = data["1h"]["adx"]

    plus_di = data["1h"]["plus_di"]

    minus_di = data["1h"]["minus_di"]

    if adx >= 20:

        if plus_di > minus_di:

            long_score += 1

            long_motivos.append(
                f"ADX fuerte ({adx:.1f})"
            )

        elif minus_di > plus_di:

            short_score += 1

            short_motivos.append(
                f"ADX fuerte ({adx:.1f})"
            )

    # --------------------------------------------------------
    # Volumen 15M
    # --------------------------------------------------------

    volumen = (
        data["15m"]["volumen_ratio"]
    )

    if volumen >= 1.30:

        if long_score > short_score:

            long_score += 1

            long_motivos.append(
                f"Volumen elevado ({volumen:.1f}x)"
            )

        elif short_score > long_score:

            short_score += 1

            short_motivos.append(
                f"Volumen elevado ({volumen:.1f}x)"
            )

    # --------------------------------------------------------
    # VWAP
    # --------------------------------------------------------

    precio = data["1h"]["precio"]

    vwap = data["1h"]["vwap"]

    if vwap:

        if precio > vwap:

            long_score += 1

            long_motivos.append(
                "Precio sobre VWAP"
            )

        elif precio < vwap:

            short_score += 1

            short_motivos.append(
                "Precio bajo VWAP"
            )

    # --------------------------------------------------------
    # Decisión
    # --------------------------------------------------------

    if (
        long_score >= MIN_SCORE
        and long_score > short_score
    ):

        return (
            "LONG",
            long_score,
            long_motivos
        )

    if (
        short_score >= MIN_SCORE
        and short_score > long_score
    ):

        return (
            "SHORT",
            short_score,
            short_motivos
        )

    return (
        None,
        max(
            long_score,
            short_score
        ),
        []
    )


# ============================================================
# BUSCAR ZONA DE ENTRADA
# ============================================================

def buscar_zona(
    data,
    direccion
):

    precio = data["1h"]["precio"]

    soporte = data["4h"]["soporte"]

    resistencia = data["4h"]["resistencia"]

    fibonacci = (
        data["4h"]["fibonacci"]
    )

    atr_value = data["4h"]["atr"]

    if not atr_value:

        return None

    niveles = []

    if direccion == "LONG":

        if soporte and soporte < precio:

            niveles.append(soporte)

        for nombre in [
            "0.618",
            "0.786"
        ]:

            nivel = fibonacci.get(
                nombre
            )

            if (
                nivel
                and nivel < precio
            ):

                niveles.append(nivel)

        vwap = data["4h"]["vwap"]

        if vwap and vwap < precio:

            niveles.append(vwap)

        if not niveles:

            return None

        nivel = max(niveles)

    else:

        if (
            resistencia
            and resistencia > precio
        ):

            niveles.append(
                resistencia
            )

        for nombre in [
            "0.382",
            "0.500",
            "0.618"
        ]:

            nivel = fibonacci.get(
                nombre
            )

            if (
                nivel
                and nivel > precio
            ):

                niveles.append(nivel)

        vwap = data["4h"]["vwap"]

        if vwap and vwap > precio:

            niveles.append(vwap)

        if not niveles:

            return None

        nivel = min(niveles)

    ancho = atr_value * 0.30

    zona_min = (
        nivel - ancho
    )

    zona_max = (
        nivel + ancho
    )

    return (
        zona_min,
        zona_max,
        nivel
    )


# ============================================================
# ¿ESTA CERCA DE LA ZONA?
# ============================================================

def esta_cerca(
    precio,
    zona_min,
    zona_max,
    atr_value
):

    if (
        zona_min
        <= precio
        <= zona_max
    ):

        return True

    if precio > zona_max:

        distancia = (
            precio
            - zona_max
        )

    else:

        distancia = (
            zona_min
            - precio
        )

    return (
        abs(distancia)
        <= atr_value * PREALERTA_ATR
    )


# ============================================================
# CONFIRMACION 5M
# ============================================================

def confirmar_entrada(
    data,
    direccion,
    zona_min,
    zona_max
):

    precio = data["5m"]["precio"]

    atr_value = data["5m"]["atr"]

    if not atr_value:

        return False

    margen = atr_value * 0.50

    cerca = (
        zona_min - margen
        <= precio
        <= zona_max + margen
    )

    if not cerca:

        return False

    rsi = data["5m"]["rsi"]

    macd = data["5m"]["macd"]

    macd_signal = (
        data["5m"]["macd_signal"]
    )

    adx = data["5m"]["adx"]

    plus_di = data["5m"]["plus_di"]

    minus_di = data["5m"]["minus_di"]

    volumen = (
        data["5m"]["volumen_ratio"]
    )

    if adx < 18:

        return False

    if volumen < 0.90:

        return False

    if direccion == "LONG":

        if rsi > 68:

            return False

        if macd <= macd_signal:

            return False

        if plus_di <= minus_di:

            return False

        return True

    else:

        if rsi < 32:

            return False

        if macd >= macd_signal:

            return False

        if minus_di <= plus_di:

            return False

        return True


# ============================================================
# APALANCAMIENTO
# ============================================================

def calcular_apalancamiento(
    score,
    adx,
    volatilidad
):

    # Base
    leverage = 5

    # Mejor confluencia
    if score >= 8:

        leverage = 6

    if score >= 9:

        leverage = 7

    # Mucha fuerza y volatilidad moderada
    if (
        score >= 9
        and adx >= 28
        and volatilidad < 2.0
    ):

        leverage = 8

    # x9/x10 solamente en señales
    # excepcionalmente fuertes.
    if (
        score >= 10
        and adx >= 30
        and volatilidad < 1.50
    ):

        leverage = 10

    return max(
        MIN_LEVERAGE,
        min(
            leverage,
            MAX_LEVERAGE
        )
    )


# ============================================================
# TP Y STOP
# ============================================================

def calcular_tp_sl(
    precio,
    atr_value,
    direccion
):

    stop_distancia = (
        atr_value * 1.35
    )

    tp1_distancia = (
        atr_value * 1.50
    )

    tp2_distancia = (
        atr_value * 2.50
    )

    if direccion == "LONG":

        stop = (
            precio
            - stop_distancia
        )

        tp1 = (
            precio
            + tp1_distancia
        )

        tp2 = (
            precio
            + tp2_distancia
        )

    else:

        stop = (
            precio
            + stop_distancia
        )

        tp1 = (
            precio
            - tp1_distancia
        )

        tp2 = (
            precio
            - tp2_distancia
        )

    return (
        stop,
        tp1,
        tp2
    )


# ============================================================
# RESULTADO ESTIMADO
# ============================================================

def calcular_ganancia(
    entrada,
    objetivo,
    leverage
):

    movimiento = (
        abs(
            objetivo
            - entrada
        )
        / entrada
    ) * 100

    resultado_bruto = (
        movimiento
        * leverage
    )

    resultado_neto = (
        resultado_bruto
        - COMISION_TOTAL
    )

    return (
        movimiento,
        resultado_bruto,
        resultado_neto
    )


# ============================================================
# FORMATO DE PRECIO
# ============================================================

def precio_texto(precio):

    if precio >= 1000:

        return f"{precio:,.2f}"

    if precio >= 1:

        return f"{precio:,.4f}"

    if precio >= 0.01:

        return f"{precio:.6f}"

    return f"{precio:.10f}"


# ============================================================
# PREALERTA
# ============================================================

def mandar_prealerta(
    symbol,
    data,
    direccion,
    score,
    motivos,
    zona
):

    zona_min, zona_max, nivel = zona

    precio = data["5m"]["precio"]

    distancia = (
        abs(
            precio
            - nivel
        )
        / precio
    ) * 100

    if direccion == "LONG":

        titulo = (
            "🟡 PREALERTA LONG"
        )

    else:

        titulo = (
            "🟠 PREALERTA SHORT"
        )

    mensaje = (
        f"{titulo}\n"
        f"{symbol}\n\n"

        f"💰 Precio actual: "
        f"{precio_texto(precio)}\n"

        f"🎯 Zona: "
        f"{precio_texto(zona_min)}"
        f" — "
        f"{precio_texto(zona_max)}\n"

        f"📍 Nivel: "
        f"{precio_texto(nivel)}\n"

        f"📏 Distancia: "
        f"{distancia:.2f}%\n\n"

        f"⭐ Confluencia: "
        f"{score}/10\n\n"

        "📊 Motivos:\n"
        + "\n".join(
            "• " + motivo
            for motivo in motivos
        )
        + "\n\n"

        "⚠️ PREPARÁ LA OPERACIÓN.\n"
        "Todavía NO entrar.\n"
        "Esperar confirmación."
    )

    enviar_telegram(
        mensaje
    )


# ============================================================
# CONFIRMACION
# ============================================================

def mandar_confirmacion(
    symbol,
    data,
    direccion,
    score,
    motivos
):

    precio = data["5m"]["precio"]

    atr_value = data["1h"]["atr"]

    if not atr_value:

        return

    stop, tp1, tp2 = (
        calcular_tp_sl(
            precio,
            atr_value,
            direccion
        )
    )

    adx = data["1h"]["adx"]

    volatilidad = (
        data["1h"]["atr_pct"]
    )

    leverage = (
        calcular_apalancamiento(
            score,
            adx,
            volatilidad
        )
    )

    movimiento, bruto, neto = (
        calcular_ganancia(
            precio,
            tp2,
            leverage
        )
    )

    # No mandar si no alcanza el
    # mínimo que configuramos.
    if neto < MIN_GANANCIA_NETA:

        print(
            symbol,
            "descartado:",
            "ganancia neta",
            neto
        )

        return

    funding = (
        data["futures"]["funding"]
    )

    open_interest = (
        data["futures"]["open_interest"]
    )

    if funding is None:

        funding_text = "N/D"

    else:

        funding_text = (
            f"{funding:.4f}%"
        )

    if open_interest is None:

        oi_text = "N/D"

    else:

        oi_text = (
            f"{open_interest:,.2f}"
        )

    if direccion == "LONG":

        titulo = (
            "🟢 LONG CONFIRMADO"
        )

    else:

        titulo = (
            "🔴 SHORT CONFIRMADO"
        )

    mensaje = (
        f"{titulo}\n"
        f"{symbol}\n\n"

        f"💰 ENTRADA: "
        f"{precio_texto(precio)}\n\n"

        f"🎯 TP1: "
        f"{precio_texto(tp1)}\n"

        f"🎯 TP2: "
        f"{precio_texto(tp2)}\n"

        f"🛑 STOP: "
        f"{precio_texto(stop)}\n\n"

        f"⚡ APALANCAMIENTO: "
        f"x{leverage}\n"

        f"⭐ CONFLUENCIA: "
        f"{score}/10\n\n"

        f"📊 RSI 1H: "
        f"{data['1h']['rsi']:.1f}\n"

        f"📊 ADX 1H: "
        f"{adx:.1f}\n"

        f"📈 Volatilidad ATR 1H: "
        f"{volatilidad:.2f}%\n"

        f"💵 Funding: "
        f"{funding_text}\n"

        f"📊 Open Interest: "
        f"{oi_text}\n\n"

        f"📈 Movimiento TP2: "
        f"{movimiento:.2f}%\n"

        f"💰 Resultado bruto: "
        f"+{bruto:.2f}%\n"

        f"💸 Comisión estimada: "
        f"-{COMISION_TOTAL:.2f}%\n"

        f"✅ Resultado neto: "
        f"+{neto:.2f}%\n\n"

        "👤 OPERACIÓN MANUAL\n"
        "El bot NO compra ni vende.\n\n"

        "📊 Motivos:\n"
        + "\n".join(
            "• " + motivo
            for motivo in motivos
        )
    )

    enviar_telegram(
        mensaje
    )


# ============================================================
# PROCESAR MONEDA
# ============================================================

def procesar_moneda(symbol):

    try:

        data = analizar_moneda(
            symbol
        )

        if not data:

            return

        direccion, score, motivos = (
            calcular_confluencia(
                data
            )
        )

        if not direccion:

            return

        zona = buscar_zona(
            data,
            direccion
        )

        if not zona:

            return

        zona_min, zona_max, nivel = (
            zona
        )

        precio = data["5m"]["precio"]

        atr_value = data["4h"]["atr"]

        if not atr_value:

            return

        # ----------------------------------------------------
        # COMPROBAR SI YA HAY UNA SEÑAL ACTIVA
        # ----------------------------------------------------

        with señales_lock:

            señal_actual = (
                señales_activas.get(
                    symbol
                )
            )

        # ----------------------------------------------------
        # PREALERTA
        # ----------------------------------------------------

        cerca = esta_cerca(
            precio,
            zona_min,
            zona_max,
            atr_value
        )

        if cerca:

            # Si no existe señal activa,
            # mandamos prealerta.

            if not señal_actual:

                mandar_prealerta(
                    symbol,
                    data,
                    direccion,
                    score,
                    motivos,
                    zona
                )

                with señales_lock:

                    señales_activas[
                        symbol
                    ] = {
                        "direccion":
                            direccion,

                        "prealerta":
                            True,

                        "confirmada":
                            False,

                        "zona":
                            zona,

                        "timestamp":
                            time.time()
                    }

                return

            # Si existe pero es dirección
            # diferente, nueva oportunidad.

            if (
                señal_actual["direccion"]
                != direccion
            ):

                mandar_prealerta(
                    symbol,
                    data,
                    direccion,
                    score,
                    motivos,
                    zona
                )

                with señales_lock:

                    señales_activas[
                        symbol
                    ] = {
                        "direccion":
                            direccion,

                        "prealerta":
                            True,

                        "confirmada":
                            False,

                        "zona":
                            zona,

                        "timestamp":
                            time.time()
                    }

                return

        # ----------------------------------------------------
        # CONFIRMACION
        # ----------------------------------------------------

        if not confirmar_entrada(
            data,
            direccion,
            zona_min,
            zona_max
        ):

            return

        # ----------------------------------------------------
        # NO REPETIR CONFIRMACION
        # ----------------------------------------------------

        with señales_lock:

            señal_actual = (
                señales_activas.get(
                    symbol
                )
            )

            if (
                señal_actual
                and señal_actual.get(
                    "confirmada"
                )
                and señal_actual.get(
                    "direccion"
                ) == direccion
            ):

                return

        # ----------------------------------------------------
        # ENVIAR CONFIRMACION
        # ----------------------------------------------------

        mandar_confirmacion(
            symbol,
            data,
            direccion,
            score,
            motivos
        )

        with señales_lock:

            señales_activas[
                symbol
            ] = {
                "direccion":
                    direccion,

                "prealerta":
                    True,

                "confirmada":
                    True,

                "zona":
                    zona,

                "timestamp":
                    time.time()
            }

    except Exception as e:

        print(
            "Error procesando",
            symbol,
            ":",
            e
        )


# ============================================================
# LIMPIAR SEÑALES ANTIGUAS
# ============================================================

def limpiar_señales():

    ahora = time.time()

    with señales_lock:

        borrar = []

        for symbol, señal in (
            señales_activas.items()
        ):

            timestamp = (
                señal.get(
                    "timestamp",
                    ahora
                )
            )

            # Después de 6 horas
            # permitimos una nueva señal.

            if (
                ahora
                - timestamp
                > 21600
            ):

                borrar.append(
                    symbol
                )

        for symbol in borrar:

            del señales_activas[
                symbol
            ]


# ============================================================
# BOT PRINCIPAL
# ============================================================

def ejecutar_bot():

    print(
        "===================================="
    )

    print(
        " BOT FUTUROS CRYPTO INICIADO"
    )

    print(
        "===================================="
    )

    enviar_telegram(
        "🤖 BOT FUTUROS ONLINE\n\n"
        "Sistema de análisis activado.\n\n"
        "📊 50 monedas dinámicas\n"
        "🟢 LONG\n"
        "🔴 SHORT\n"
        "🟡 PREALERTA\n\n"
        "⏱️ 1D / 4H / 1H / 15M / 5M\n\n"
        "👤 Operaciones manuales."
    )

    monedas = []

    ultima_seleccion = 0

    while True:

        try:

            ahora = time.time()

            # Recalcular las 50 monedas
            # cada 6 horas.

            if (
                not monedas
                or ahora
                - ultima_seleccion
                > 21600
            ):

                nuevas = (
                    seleccionar_monedas()
                )

                if nuevas:

                    monedas = nuevas

                    ultima_seleccion = (
                        ahora
                    )

                    print(
                        "Universo actualizado:",
                        len(monedas)
                    )

            if not monedas:

                print(
                    "No hay monedas disponibles."
                )

                time.sleep(60)

                continue

            print(
                "--------------------------------"
            )

            print(
                "Nuevo ciclo:",
                len(monedas),
                "monedas"
            )

            print(
                "--------------------------------"
            )

            for symbol in monedas:

                print(
                    "Analizando:",
                    symbol
                )

                procesar_moneda(
                    symbol
                )

                # Pequeña pausa para
                # reducir presión sobre API.

                time.sleep(0.15)

            limpiar_señales()

            print(
                "Ciclo terminado."
            )

            print(
                "Esperando",
                INTERVALO_ANALISIS,
                "segundos."
            )

            time.sleep(
                INTERVALO_ANALISIS
            )

        except Exception as e:

            print(
                "Error general:",
                e
            )

            time.sleep(30)


# ============================================================
# RUTA PRINCIPAL
# ============================================================

@app.route("/")
def inicio():

    return (
        "BOT FUTUROS "
        "CONFLUENCIAS ONLINE"
    )


# ============================================================
# TEST TELEGRAM
# ============================================================

@app.route("/test")
def test():

    enviar_telegram(
        "🟢 TEST TELEGRAM\n\n"
        "El bot está conectado correctamente."
    )

    return (
        "TEST TELEGRAM ENVIADO"
    )


# ============================================================
# INICIAR
# ============================================================

if __name__ == "__main__":

    hilo = threading.Thread(
        target=ejecutar_bot,
        daemon=True
    )

    hilo.start()

    app.run(
        host="0.0.0.0",
        port=10000
    )
