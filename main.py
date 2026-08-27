import os
import time
import json
import threading
import datetime
import numpy as np
import requests

from zoneinfo import ZoneInfo
from flask import Flask, request


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

# Token para proteger /test y /estado
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")

BINANCE_URL = "https://fapi.binance.com"


# ============================================================
# ARCHIVO DE ESTADO
# ============================================================

ARCHIVO_ESTADO = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "estado_señales.json"
)


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# SESSION HTTP
# ============================================================

session = requests.Session()

session.headers.update({
    "User-Agent": "bot-senales-futuros/2.0"
})


# ============================================================
# PARAMETROS DEL BOT
# ============================================================

TOTAL_MONEDAS = 50

MONEDAS_BAJA_VOLATILIDAD = 40
MONEDAS_ALTA_VOLATILIDAD = 10

# Cada cuánto analiza nuevamente las monedas
INTERVALO_ANALISIS = 180


# ============================================================
# CONFLUENCIA
# ============================================================

MIN_SCORE = 7


# ============================================================
# APALANCAMIENTO
# ============================================================

MIN_LEVERAGE = 5
MAX_LEVERAGE = 10


# ============================================================
# GANANCIA MINIMA
# ============================================================

MIN_GANANCIA_NETA = 1.50


# ============================================================
# FILTRO DE MOMENTUM 5M
# ============================================================

UMBRAL_ADX_MINIMO_5M = 18

UMBRAL_VOLUMEN_MINIMO_5M = 0.90


# ============================================================
# COMISION BINANCE
# ============================================================

# 0.04% por lado
# Entrada + salida = 0.08%
#
# Si tu comisión real es diferente, podés modificarlo.

COMISION_TAKER_POR_LADO = 0.04


# ============================================================
# PREALERTA
# ============================================================

PREALERTA_ATR = 1.50


# ============================================================
# CONTROL DE RATE LIMIT
# ============================================================

PAUSA_ENTRE_VELAS = 0.12

PAUSA_ENTRE_SELECCION = 0.15

PAUSA_ENTRE_MONEDAS_CICLO = 0.25

ESPERA_MINIMA_RATE_LIMIT = 60


# ============================================================
# ALERTA DE ERRORES BINANCE
# ============================================================

UMBRAL_ERRORES_ALERTA = 5

COOLDOWN_ALERTA_SEGUNDOS = 3600

_errores_consecutivos = 0

_ultimo_codigo_error = None

_ultima_alerta_error = 0

_lock_errores = threading.Lock()


# ============================================================
# REGISTRO DE ERRORES BINANCE
# ============================================================

def _registrar_fallo_binance(status_code, endpoint):

    global _errores_consecutivos
    global _ultimo_codigo_error
    global _ultima_alerta_error

    with _lock_errores:

        _errores_consecutivos += 1

        _ultimo_codigo_error = status_code

        contador = _errores_consecutivos

        ahora = time.time()

        debe_avisar = (
            contador >= UMBRAL_ERRORES_ALERTA
            and ahora - _ultima_alerta_error
            > COOLDOWN_ALERTA_SEGUNDOS
        )

        if debe_avisar:
            _ultima_alerta_error = ahora

    if debe_avisar:

        explicacion = ""

        if status_code == 451:

            explicacion = (
                "\n\n⚠️ 451 = Binance está bloqueando las requests "
                "que llegan desde la IP/región del servidor."
            )

        elif status_code == 403:

            explicacion = (
                "\n\n⚠️ 403 = Binance rechazó el acceso. "
                "Puede tratarse de un bloqueo de IP o región."
            )

        elif status_code is None:

            explicacion = (
                "\n\n⚠️ No está llegando respuesta de Binance "
                "(timeout o error de conexión)."
            )

        mensaje = (
            "🔴 BOT SIN DATOS DE BINANCE\n\n"
            f"Últimos {contador} intentos a {endpoint} fallaron.\n"
            f"Código: {status_code}\n\n"
            "El bot NO está pudiendo analizar el mercado ahora mismo."
            f"{explicacion}"
        )

        enviar_telegram(mensaje)


# ============================================================
# REGISTRO DE REQUEST EXITOSA
# ============================================================

def _registrar_ok_binance():

    global _errores_consecutivos

    with _lock_errores:

        _errores_consecutivos = 0


# ============================================================
# PERSISTENCIA DE SEÑALES
# ============================================================

señales_lock = threading.Lock()

señales_activas = {}


def cargar_estado():

    global señales_activas

    if not os.path.exists(ARCHIVO_ESTADO):

        return

    try:

        with open(
            ARCHIVO_ESTADO,
            "r",
            encoding="utf-8"
        ) as f:

            señales_activas = json.load(f)

        print(
            "Estado cargado:",
            len(señales_activas),
            "señales activas"
        )

    except Exception as e:

        print(
            "No se pudo cargar el estado previo:",
            e
        )

        señales_activas = {}


def guardar_estado():

    try:

        with señales_lock:

            copia = dict(señales_activas)

        with open(
            ARCHIVO_ESTADO,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                copia,
                f,
                ensure_ascii=False,
                indent=2
            )

    except Exception as e:

        print(
            "No se pudo guardar el estado:",
            e
        )


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
# REQUEST BINANCE
# ============================================================

def binance_get(
    endpoint,
    params=None,
    reintentos=2
):

    for intento in range(reintentos + 1):

        try:

            respuesta = session.get(
                BINANCE_URL + endpoint,
                params=params,
                timeout=10
            )


            # ------------------------------------------------
            # RATE LIMIT
            # ------------------------------------------------

            if respuesta.status_code in (429, 418):

                retry_after = respuesta.headers.get(
                    "Retry-After"
                )

                try:

                    espera = max(
                        int(retry_after),
                        ESPERA_MINIMA_RATE_LIMIT
                    )

                except (
                    TypeError,
                    ValueError
                ):

                    espera = ESPERA_MINIMA_RATE_LIMIT


                print(
                    f"Rate limit Binance "
                    f"({respuesta.status_code}). "
                    f"Esperando {espera}s."
                )

                time.sleep(espera)

                continue


            # ------------------------------------------------
            # BLOQUEO
            # ------------------------------------------------

            if respuesta.status_code in (451, 403):

                print(
                    "Error Binance:",
                    respuesta.status_code,
                    endpoint
                )

                _registrar_fallo_binance(
                    respuesta.status_code,
                    endpoint
                )

                return None


            # ------------------------------------------------
            # OTROS ERRORES
            # ------------------------------------------------

            if respuesta.status_code != 200:

                print(
                    "Error Binance:",
                    respuesta.status_code,
                    endpoint
                )

                _registrar_fallo_binance(
                    respuesta.status_code,
                    endpoint
                )

                return None


            # ------------------------------------------------
            # OK
            # ------------------------------------------------

            _registrar_ok_binance()

            return respuesta.json()


        except Exception as e:

            print(
                "Error conexión Binance:",
                e
            )

            time.sleep(2)


    _registrar_fallo_binance(
        None,
        endpoint
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


    for item in data.get(
        "symbols",
        []
    ):

        try:

            if item.get("status") != "TRADING":

                continue

            if item.get("contractType") != "PERPETUAL":

                continue

            if item.get("quoteAsset") != "USDT":

                continue

            symbol = item.get("symbol")


            if (
                symbol
                and symbol.endswith("USDT")
            ):

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

            if (
                not symbol
                or not symbol.endswith("USDT")
            ):

                continue


            resultado[symbol] = float(
                item.get(
                    "quoteVolume",
                    0
                )
            )


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
# MARK PRICE + FUNDING
# ============================================================

def obtener_mark_y_funding(symbol):

    data = binance_get(
        "/fapi/v1/premiumIndex",
        {
            "symbol": symbol
        }
    )

    if not data:

        return None, None


    try:

        mark = float(
            data["markPrice"]
        )

        funding = (
            float(
                data["lastFundingRate"]
            )
            * 100
        )

        return mark, funding


    except Exception:

        return None, None


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
# EMA
# ============================================================

def calcular_ema(
    valores,
    periodo
):

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


def calcular_ema_series(
    valores,
    periodo
):

    if not valores:

        return []


    k = 2 / (periodo + 1)

    resultado = [
        valores[0]
    ]


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


    for i in range(
        1,
        len(cierres)
    ):

        cambio = (
            cierres[i]
            - cierres[i - 1]
        )


        if cambio > 0:

            ganancias.append(cambio)

            perdidas.append(0)

        else:

            ganancias.append(0)

            perdidas.append(
                abs(cambio)
            )


    promedio_ganancia = (
        sum(
            ganancias[-periodo:]
        )
        / periodo
    )

    promedio_perdida = (
        sum(
            perdidas[-periodo:]
        )
        / periodo
    )


    if promedio_perdida == 0:

        return 100


    rs = (
        promedio_ganancia
        / promedio_perdida
    )


    return (
        100
        - (
            100
            / (1 + rs)
        )
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


    linea_macd = [
        a - b
        for a, b
        in zip(
            ema12,
            ema26
        )
    ]


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
        sum(
            tr[-periodo:]
        )
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

        positivos.append(
            plus_dm
        )

        negativos.append(
            minus_dm
        )


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
        100
        * plus14
        / tr14
    )


    minus_di = (
        100
        * minus14
        / tr14
    )


    suma = (
        plus_di
        + minus_di
    )


    if suma == 0:

        return (
            0,
            plus_di,
            minus_di
        )


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
# BOLLINGER
# ============================================================

def calcular_bollinger(
    cierres,
    periodo=20,
    desviaciones=2
):

    if len(cierres) < periodo:

        return None, None, None


    sma = (
        sum(
            cierres[-periodo:]
        )
        / periodo
    )


    std = np.std(
        cierres[-periodo:]
    )


    banda_superior = (
        sma
        + (
            std
            * desviaciones
        )
    )


    banda_inferior = (
        sma
        - (
            std
            * desviaciones
        )
    )


    return (
        banda_superior,
        sma,
        banda_inferior
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


    precio_anterior = cierres[-10]

    precio_actual = cierres[-1]


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
# ANALISIS TEMPORALIDAD
# ============================================================

def analizar_temporalidad(velas):

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


    bb_superior, bb_sma, bb_inferior = (
        calcular_bollinger(cierres)
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
            calcular_vwap(
                velas
            ),

        "soporte":
            soporte,

        "resistencia":
            resistencia,

        "fibonacci":
            calcular_fibonacci(
                velas
            ),

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
            ),

        "bb_superior":
            bb_superior,

        "bb_sma":
            bb_sma,

        "bb_inferior":
            bb_inferior
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


        time.sleep(
            PAUSA_ENTRE_VELAS
        )


    mark, funding = (
        obtener_mark_y_funding(
            symbol
        )
    )


    resultado["futures"] = {

        "mark":
            mark,

        "open_interest":
            obtener_open_interest(
                symbol
            ),

        "funding":
            funding
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


        candidatos.append({

            "symbol":
                symbol,

            "volatilidad":
                volatilidad,

            "volumen":
                volumen
        })


        time.sleep(
            PAUSA_ENTRE_SELECCION
        )


    if not candidatos:

        return []


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


    resultado = []

    vistos = set()


    for x in seleccionadas:

        symbol = x["symbol"]


        if symbol not in vistos:

            vistos.add(symbol)

            resultado.append(
                symbol
            )


    print(
        "Monedas seleccionadas:",
        len(resultado)
    )


    return resultado[
        :TOTAL_MONEDAS
    ]


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

    if (
        data["1d"]["tendencia"]
        == "ALCISTA"
    ):

        long_score += 2

        long_motivos.append(
            "Tendencia 1D alcista"
        )


    elif (
        data["1d"]["tendencia"]
        == "BAJISTA"
    ):

        short_score += 2

        short_motivos.append(
            "Tendencia 1D bajista"
        )


    # --------------------------------------------------------
    # 4H
    # --------------------------------------------------------

    if (
        data["4h"]["tendencia"]
        == "ALCISTA"
    ):

        long_score += 2

        long_motivos.append(
            "Estructura 4H alcista"
        )


    elif (
        data["4h"]["tendencia"]
        == "BAJISTA"
    ):

        short_score += 2

        short_motivos.append(
            "Estructura 4H bajista"
        )


    # --------------------------------------------------------
    # RSI 1H
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
    # DIVERGENCIA
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
    # VOLUMEN
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
    # RESULTADO
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

    fibonacci = data["4h"]["fibonacci"]

    atr_value = data["4h"]["atr"]


    if not atr_value:

        return None


    niveles = []


    if direccion == "LONG":

        if (
            soporte
            and soporte < precio
        ):

            niveles.append(
                soporte
            )


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

                niveles.append(
                    nivel
                )


        vwap = data["4h"]["vwap"]


        if (
            vwap
            and vwap < precio
        ):

            niveles.append(
                vwap
            )


        if not niveles:

            return None


        nivel = max(
            niveles
        )


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

                niveles.append(
                    nivel
                )


        vwap = data["4h"]["vwap"]


        if (
            vwap
            and vwap > precio
        ):

            niveles.append(
                vwap
            )


        if not niveles:

            return None


        nivel = min(
            niveles
        )


    # --------------------------------------------------------
    # ANCHO DE LA ZONA
    # --------------------------------------------------------

    ancho = (
        atr_value
        * 0.30
    )


    return (
        nivel - ancho,
        nivel + ancho,
        nivel
    )


# ============================================================
# ¿ESTÁ CERCA DE LA ZONA?
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
        <= atr_value
        * PREALERTA_ATR
    )


# ============================================================
# MOMENTUM MINIMO
# ============================================================

def hay_momentum_minimo(data):

    adx = data["5m"]["adx"]

    volumen = (
        data["5m"]["volumen_ratio"]
    )


    if adx < UMBRAL_ADX_MINIMO_5M:

        return False


    if volumen < UMBRAL_VOLUMEN_MINIMO_5M:

        return False


    return True


# ============================================================
# CONFIRMACION EXTRA 5M
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


    margen = (
        atr_value
        * 0.50
    )


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

    leverage = 5


    if score >= 8:

        leverage = 6


    if score >= 9:

        leverage = 7


    if (
        score >= 9
        and adx >= 28
        and volatilidad < 2.0
    ):

        leverage = 8


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
# NUEVO STOP LOSS / TP
# ============================================================
#
# ESTA ES LA PARTE PRINCIPAL CORREGIDA.
#
# El Stop ya NO queda pegado a la entrada.
#
# LONG:
#   - mínimo 1.80 ATR
#   - intenta colocarse debajo del soporte
#   - deja un pequeño margen debajo del soporte
#   - máximo aproximado 3 ATR
#
# SHORT:
#   - mínimo 1.80 ATR
#   - intenta colocarse encima de resistencia
#   - deja un pequeño margen encima de resistencia
#   - máximo aproximado 3 ATR
#
# ============================================================

def calcular_tp_sl(
    precio,
    atr_value,
    direccion,
    data=None,
    zona=None
):

    if (
        not atr_value
        or precio <= 0
    ):

        return (
            None,
            None,
            None
        )


    # --------------------------------------------------------
    # DISTANCIAS DEL STOP
    # --------------------------------------------------------

    STOP_MIN_ATR = 1.80

    STOP_MAX_ATR = 3.00

    STOP_BUFFER_ATR = 0.20


    stop_minimo = (
        atr_value
        * STOP_MIN_ATR
    )


    stop_maximo = (
        atr_value
        * STOP_MAX_ATR
    )


    stop_buffer = (
        atr_value
        * STOP_BUFFER_ATR
    )


    # --------------------------------------------------------
    # NIVELES
    # --------------------------------------------------------

    soporte = None

    resistencia = None


    if data:

        try:

            soporte = (
                data["4h"]
                .get("soporte")
            )

            resistencia = (
                data["4h"]
                .get("resistencia")
            )

        except Exception:

            pass


    # ========================================================
    # LONG
    # ========================================================

    if direccion == "LONG":

        stop_tecnico = None


        # -----------------------------------------------
        # Intentar colocar el stop debajo del soporte
        # -----------------------------------------------

        if (
            soporte
            and soporte < precio
        ):

            candidato = (
                soporte
                - stop_buffer
            )


            distancia = (
                precio
                - candidato
            )


            # Solo usamos el soporte si no queda
            # demasiado lejos.
            if (
                distancia >= stop_minimo
                and distancia <= stop_maximo
            ):

                stop_tecnico = candidato


        # -----------------------------------------------
        # Si soporte está demasiado lejos,
        # usamos un Stop por ATR.
        # -----------------------------------------------

        if stop_tecnico is None:

            stop_tecnico = (
                precio
                - (
                    atr_value
                    * 2.20
                )
            )


        # -----------------------------------------------
        # Seguridad mínima
        # -----------------------------------------------

        if (
            precio - stop_tecnico
            < stop_minimo
        ):

            stop_tecnico = (
                precio
                - stop_minimo
            )


        # -----------------------------------------------
        # Seguridad máxima
        # -----------------------------------------------

        if (
            precio - stop_tecnico
            > stop_maximo
        ):

            stop_tecnico = (
                precio
                - stop_maximo
            )


        stop = stop_tecnico


        # -----------------------------------------------
        # TAKE PROFITS
        # -----------------------------------------------

        tp1 = (
            precio
            + atr_value * 1.50
        )


        tp2 = (
            precio
            + atr_value * 2.80
        )


    # ========================================================
    # SHORT
    # ========================================================

    else:

        stop_tecnico = None


        # -----------------------------------------------
        # Intentar colocar el stop encima de resistencia
        # -----------------------------------------------

        if (
            resistencia
            and resistencia > precio
        ):

            candidato = (
                resistencia
                + stop_buffer
            )


            distancia = (
                candidato
                - precio
            )


            if (
                distancia >= stop_minimo
                and distancia <= stop_maximo
            ):

                stop_tecnico = candidato


        # -----------------------------------------------
        # Si resistencia está demasiado lejos,
        # usamos ATR.
        # -----------------------------------------------

        if stop_tecnico is None:

            stop_tecnico = (
                precio
                + (
                    atr_value
                    * 2.20
                )
            )


        # -----------------------------------------------
        # Seguridad mínima
        # -----------------------------------------------

        if (
            stop_tecnico - precio
            < stop_minimo
        ):

            stop_tecnico = (
                precio
                + stop_minimo
            )


        # -----------------------------------------------
        # Seguridad máxima
        # -----------------------------------------------

        if (
            stop_tecnico - precio
            > stop_maximo
        ):

            stop_tecnico = (
                precio
                + stop_maximo
            )


        stop = stop_tecnico


        # -----------------------------------------------
        # TAKE PROFITS
        # -----------------------------------------------

        tp1 = (
            precio
            - atr_value * 1.50
        )


        tp2 = (
            precio
            - atr_value * 2.80
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
        * 100
    )


    resultado_bruto = (
        movimiento
        * leverage
    )


    comision_total = (
        COMISION_TAKER_POR_LADO
        * 2
        * leverage
    )


    resultado_neto = (
        resultado_bruto
        - comision_total
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
# MANDAR ORDEN
# ============================================================

def mandar_orden_compra(
    symbol,
    data,
    direccion,
    score,
    motivos,
    zona
):

    zona_min, zona_max, nivel = zona


    precio = data["5m"]["precio"]


    # Usamos ATR 1H para calcular riesgo
    atr_value = data["1h"]["atr"]


    if not atr_value:

        return None


    # --------------------------------------------------------
    # STOP / TP CORREGIDOS
    # --------------------------------------------------------

    stop, tp1, tp2 = calcular_tp_sl(
        precio,
        atr_value,
        direccion,
        data,
        zona
    )


    if (
        stop is None
        or tp1 is None
        or tp2 is None
    ):

        return None


    # --------------------------------------------------------
    # APALANCAMIENTO
    # --------------------------------------------------------

    adx = data["1h"]["adx"]

    volatilidad = (
        data["1h"]["atr_pct"]
    )


    leverage = calcular_apalancamiento(
        score,
        adx,
        volatilidad
    )


    # --------------------------------------------------------
    # GANANCIA
    # --------------------------------------------------------

    movimiento, bruto, neto = (
        calcular_ganancia(
            precio,
            tp2,
            leverage
        )
    )


    if neto < MIN_GANANCIA_NETA:

        print(
            symbol,
            "descartado: ganancia neta",
            round(neto, 2)
        )

        return None


    # --------------------------------------------------------
    # DATOS FUTUROS
    # --------------------------------------------------------

    funding = (
        data["futures"]["funding"]
    )


    open_interest = (
        data["futures"]["open_interest"]
    )


    funding_text = (
        "N/D"
        if funding is None
        else f"{funding:.4f}%"
    )


    oi_text = (
        "N/D"
        if open_interest is None
        else f"{open_interest:,.2f}"
    )


    # ========================================================
    # URGENCIA
    # ========================================================

    dentro_zona = (
        zona_min
        <= precio
        <= zona_max
    )


    atr_5m = data["5m"]["atr"]


    if dentro_zona:

        urgencia_texto = (
            "⏱️ YA ESTÁ DENTRO de la zona."
        )


    elif atr_5m:

        distancia_precio = (
            precio - zona_max
            if precio > zona_max
            else zona_min - precio
        )


        velas_estimadas = (
            distancia_precio
            / atr_5m
        )


        minutos_estimados = (
            velas_estimadas
            * 5
        )


        urgencia_texto = (
            "⏱️ Todavía se está acercando — "
            f"~{minutos_estimados:.0f} min "
            "estimados hasta tocar la zona. "
            "No es una garantía."
        )


    else:

        urgencia_texto = (
            "⏱️ Todavía se está acercando "
            "a la zona."
        )


    # ========================================================
    # TÍTULO
    # ========================================================

    if direccion == "LONG":

        titulo = (
            "🟢 ORDEN DE COMPRA (LONG)"
        )

    else:

        titulo = (
            "🔴 ORDEN DE VENTA (SHORT)"
        )


    # ========================================================
    # DISTANCIA STOP
    # ========================================================

    distancia_stop = (
        abs(
            precio
            - stop
        )
        / precio
        * 100
    )


    # ========================================================
    # MENSAJE
    # ========================================================

    mensaje = (

        f"{titulo}\n"
        f"{symbol}\n\n"

        f"💰 PRECIO ACTUAL: "
        f"{precio_texto(precio)}\n"

        f"🎯 ZONA DE ENTRADA: "
        f"{precio_texto(zona_min)} — "
        f"{precio_texto(zona_max)}\n"

        f"{urgencia_texto}\n\n"

        f"🎯 TP1: "
        f"{precio_texto(tp1)}\n"

        f"🎯 TP2: "
        f"{precio_texto(tp2)}\n"

        f"🛑 STOP: "
        f"{precio_texto(stop)}\n"

        f"📏 Distancia Stop: "
        f"{distancia_stop:.2f}%\n\n"

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

        f"💸 Comisión estimada "
        f"(x{leverage}): "
        f"-{COMISION_TAKER_POR_LADO * 2 * leverage:.2f}%\n"

        f"✅ Resultado neto: "
        f"+{neto:.2f}%\n\n"

        "👤 OPERACIÓN MANUAL — "
        "el bot NO compra ni vende.\n"

        "⚠️ La señal llega con anticipación. "
        "El precio puede moverse antes de ejecutar.\n\n"

        "📊 Motivos:\n"

        + "\n".join(
            "• " + m
            for m in motivos
        )
    )


    enviar_telegram(
        mensaje
    )


    return (
        precio,
        stop,
        tp1,
        tp2
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


        zona_min, zona_max, nivel = zona


        precio = data["5m"]["precio"]

        atr_value = data["4h"]["atr"]


        if not atr_value:

            return


        cerca = esta_cerca(
            precio,
            zona_min,
            zona_max,
            atr_value
        )


        if not cerca:

            return


        # ----------------------------------------------------
        # MOMENTUM
        # ----------------------------------------------------

        if not hay_momentum_minimo(
            data
        ):

            return


        # ----------------------------------------------------
        # EVITAR REPETIR SEÑAL
        # ----------------------------------------------------

        with señales_lock:

            señal_actual = (
                señales_activas.get(
                    symbol
                )
            )


        debe_enviar = (

            not señal_actual

            or señal_actual.get(
                "direccion"
            )
            != direccion
        )


        if not debe_enviar:

            return


        # ----------------------------------------------------
        # MANDAR SEÑAL
        # ----------------------------------------------------

        resultado = mandar_orden_compra(

            symbol,

            data,

            direccion,

            score,

            motivos,

            zona
        )


        if not resultado:

            return


        (
            precio_entrada,
            stop,
            tp1,
            tp2
        ) = resultado


        # ----------------------------------------------------
        # GUARDAR SEÑAL
        # ----------------------------------------------------

        with señales_lock:

            señales_activas[symbol] = {

                "direccion":
                    direccion,

                "timestamp":
                    time.time(),

                "precio_entrada":
                    precio_entrada,

                "stop":
                    stop,

                "tp1":
                    tp1,

                "tp2":
                    tp2,

                "estado":
                    "esperando_tp1"
            }


        guardar_estado()


    except Exception as e:

        print(
            "Error procesando",
            symbol,
            ":",
            e
        )


# ============================================================
# VIGILAR TP1 / TP2
# ============================================================

def vigilar_señales_activas():

    with señales_lock:

        copia = {

            symbol:
                dict(señal)

            for symbol, señal
            in señales_activas.items()

            if señal.get("estado")
        }


    for symbol, señal in copia.items():

        velas = obtener_velas(
            symbol,
            "5m",
            1
        )


        if not velas:

            continue


        precio_actual = (
            velas[-1]["close"]
        )


        direccion = (
            señal.get(
                "direccion"
            )
        )


        tp1 = señal.get(
            "tp1"
        )


        tp2 = señal.get(
            "tp2"
        )


        precio_entrada = (
            señal.get(
                "precio_entrada"
            )
        )


        estado = (
            señal.get(
                "estado"
            )
        )


        if (
            tp1 is None
            or tp2 is None
            or precio_entrada is None
        ):

            continue


        # ----------------------------------------------------
        # TP1
        # ----------------------------------------------------

        alcanzo_tp1 = (

            (
                direccion == "LONG"
                and precio_actual >= tp1
            )

            or

            (
                direccion == "SHORT"
                and precio_actual <= tp1
            )
        )


        # ----------------------------------------------------
        # TP2
        # ----------------------------------------------------

        alcanzo_tp2 = (

            (
                direccion == "LONG"
                and precio_actual >= tp2
            )

            or

            (
                direccion == "SHORT"
                and precio_actual <= tp2
            )
        )


        # ----------------------------------------------------
        # AVISO TP1
        # ----------------------------------------------------

        if (
            estado == "esperando_tp1"
            and alcanzo_tp1
        ):

            enviar_telegram(

                f"🔥 {symbol} alcanzó TP1 "
                f"({precio_texto(tp1)}).\n\n"

                f"🛡️ Sugerencia: mové tu "
                f"Stop Loss al precio de entrada "
                f"({precio_texto(precio_entrada)})."
            )


            with señales_lock:

                if symbol in señales_activas:

                    señales_activas[
                        symbol
                    ]["estado"] = (
                        "esperando_tp2"
                    )


            guardar_estado()


        # ----------------------------------------------------
        # AVISO TP2
        # ----------------------------------------------------

        elif (
            estado == "esperando_tp2"
            and alcanzo_tp2
        ):

            enviar_telegram(

                f"🚀 {symbol} alcanzó TP2 "
                f"({precio_texto(tp2)}).\n\n"

                "✅ Operación completada."
            )


            with señales_lock:

                señales_activas.pop(
                    symbol,
                    None
                )


            guardar_estado()


# ============================================================
# REPORTE DIARIO 09:30 ARGENTINA
# ============================================================

HORA_REPORTE_DIARIO = 9

MINUTO_REPORTE_DIARIO = 30


def reporte_diario_estado():

    ya_enviado_hoy = False


    zona_argentina = ZoneInfo(
        "America/Argentina/Buenos_Aires"
    )


    while True:

        try:

            ahora = (
                datetime.datetime.now(
                    zona_argentina
                )
            )


            if (
                ahora.hour
                == HORA_REPORTE_DIARIO

                and

                ahora.minute
                == MINUTO_REPORTE_DIARIO
            ):

                if not ya_enviado_hoy:

                    enviar_telegram(

                        "🟢 TU BOT ESTÁ FUNCIONANDO\n\n"

                        "🤖 El bot de futuros está "
                        "activo y analizando el mercado.\n\n"

                        "📊 Binance Futures\n"
                        "⏱️ 1D / 4H / 1H / 15M / 5M\n"
                        "🟢 LONG / 🔴 SHORT\n\n"

                        "🇦🇷 Reporte diario — 09:30"
                    )


                    ya_enviado_hoy = True


            else:

                ya_enviado_hoy = False


        except Exception as e:

            print(
                "Error reporte diario:",
                e
            )


        time.sleep(20)


# ============================================================
# LIMPIAR SEÑALES ANTIGUAS
# ============================================================

def limpiar_señales():

    ahora = time.time()

    hubo_cambios = False


    with señales_lock:

        borrar = [

            symbol

            for symbol, señal
            in señales_activas.items()

            if (
                ahora
                - señal.get(
                    "timestamp",
                    ahora
                )
                > 21600
            )
        ]


        for symbol in borrar:

            del señales_activas[
                symbol
            ]

            hubo_cambios = True


    if hubo_cambios:

        guardar_estado()


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


    cargar_estado()


    # --------------------------------------------------------
    # MENSAJE AL INICIAR
    # --------------------------------------------------------

    enviar_telegram(

        "🤖 BOT FUTUROS ONLINE\n\n"

        "Sistema de análisis activado.\n\n"

        "📊 50 monedas dinámicas\n"

        "🟢 LONG\n"
        "🔴 SHORT\n\n"

        "⏱️ 1D / 4H / 1H / 15M / 5M\n\n"

        "🛑 Stop Loss técnico "
        "con protección contra stops demasiado cercanos.\n\n"

        "👤 Operaciones manuales."
    )


    monedas = []

    ultima_seleccion = 0


    while True:

        try:

            ahora = time.time()


            # ------------------------------------------------
            # ACTUALIZAR UNIVERSO CADA 6 HORAS
            # ------------------------------------------------

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

                    ultima_seleccion = ahora


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


            # ------------------------------------------------
            # ANALIZAR MONEDAS
            # ------------------------------------------------

            for symbol in monedas:

                print(
                    "Analizando:",
                    symbol
                )


                procesar_moneda(
                    symbol
                )


                time.sleep(
                    PAUSA_ENTRE_MONEDAS_CICLO
                )


            # ------------------------------------------------
            # VIGILAR SEÑALES
            # ------------------------------------------------

            vigilar_señales_activas()


            # ------------------------------------------------
            # LIMPIAR ANTIGUAS
            # ------------------------------------------------

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
        "BOT FUTUROS CONFLUENCIAS ONLINE"
    )


# ============================================================
# TEST TELEGRAM
# ============================================================

@app.route("/test")
def test():

    if (
        not ADMIN_TOKEN
        or request.args.get("token")
        != ADMIN_TOKEN
    ):

        return (
            "No autorizado",
            403
        )


    enviar_telegram(

        "🟢 TEST TELEGRAM\n\n"

        "El bot está conectado correctamente."
    )


    return (
        "TEST TELEGRAM ENVIADO"
    )


# ============================================================
# ESTADO
# ============================================================

@app.route("/estado")
def estado():

    if (
        not ADMIN_TOKEN
        or request.args.get("token")
        != ADMIN_TOKEN
    ):

        return (
            "No autorizado",
            403
        )


    with _lock_errores:

        errores = (
            _errores_consecutivos
        )

        ultimo_codigo = (
            _ultimo_codigo_error
        )


    return {

        "errores_consecutivos_binance":
            errores,

        "ultimo_codigo_error":
            ultimo_codigo,

        "señales_activas":
            list(
                señales_activas.keys()
            )
    }


# ============================================================
# INICIAR
# ============================================================

if __name__ == "__main__":

    # --------------------------------------------------------
    # HILO PRINCIPAL DEL BOT
    # --------------------------------------------------------

    hilo_bot = threading.Thread(
        target=ejecutar_bot,
        daemon=True
    )

    hilo_bot.start()


    # --------------------------------------------------------
    # HILO DEL REPORTE DIARIO
    # --------------------------------------------------------

    hilo_reporte = threading.Thread(
        target=reporte_diario_estado,
        daemon=True
    )

    hilo_reporte.start()


    # --------------------------------------------------------
    # SERVIDOR FLASK
    # --------------------------------------------------------

    app.run(
        host="0.0.0.0",
        port=int(
            os.getenv(
                "PORT",
                10000
            )
        )
    )
