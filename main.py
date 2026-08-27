import os
import time
import json
import threading
from flask import Flask
import requests
import datetime
import math

# ============================================================
# CONFIGURACIÓN GLOBAL
# ============================================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
BINANCE_URL = "https://fapi.binance.com"

señales_activas = {}

def cargar_estado():
    global señales_activas
    if os.path.exists("estado.json"):
        with open("estado.json", "r") as f:
            señales_activas = json.load(f)

def guardar_estado():
    with open("estado.json", "w") as f:
        json.dump(señales_activas, f)

def enviar_telegram(mensaje):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": mensaje}
    try:
        requests.post(url, json=payload)
    except Exception as e:
        print(f"Error enviando Telegram: {e}")

def precio_texto(precio):
    return f"{precio:.4f}" if precio < 10 else f"{precio:.2f}"

def obtener_velas(symbol, intervalo, limite=100):
    url = f"{BINANCE_URL}/fapi/v1/klines"
    params = {"symbol": symbol, "interval": intervalo, "limit": limite}
    try:
        res = requests.get(url, params=params)
        data = res.json()
        if isinstance(data, list):
            return [{"close": float(x[4]), "high": float(x[2]), "low": float(x[3])} for x in data]
    except:
        pass
    return []

# ============================================================
# TAREAS EN SEGUNDO PLANO (Aviso Diario)
# ============================================================
def reporte_diario_estado():
    ya_enviado_hoy = False
    while True:
        ahora = datetime.datetime.now()
        if ahora.hour == 15 and ahora.minute == 16:
            if not ya_enviado_hoy:
                enviar_telegram("✅ El bot de futuros está activo y escaneando el mercado.")
                ya_enviado_hoy = True
        else:
            ya_enviado_hoy = False
        time.sleep(30)
# ============================================================
# INDICADORES TÉCNICOS
# ============================================================
def calcular_atr(velas, periodos=14):
    if len(velas) < periodos: return 0
    tr_list = [max(v['high'] - v['low'], abs(v['high'] - velas[i-1]['close']), abs(v['low'] - velas[i-1]['close'])) 
               for i, v in enumerate(velas) if i > 0]
    return sum(tr_list[-periodos:]) / periodos if tr_list else 0

def calcular_bollinger(cierres, periodo=20, desviaciones=2):
    if len(cierres) < periodo:
        return None, None, None
    sma = sum(cierres[-periodo:]) / periodo
    varianza = sum((x - sma) ** 2 for x in cierres[-periodo:]) / periodo
    std = math.sqrt(varianza)
    return sma + (std * desviaciones), sma, sma - (std * desviaciones)

def calcular_tp_sl(precio, atr, direccion):
    if direccion == "LONG":
        stop = precio - (atr * 1.5)
        tp1 = precio + (atr * 1.5)
        tp2 = precio + (atr * 3.0)
    else:
        stop = precio + (atr * 1.5)
        tp1 = precio - (atr * 1.5)
        tp2 = precio - (atr * 3.0)
    return stop, tp1, tp2

def analizar_temporalidad(velas):
    if not velas: return {}
    cierres = [x["close"] for x in velas]
    atr_val = calcular_atr(velas)
    bb_upper, bb_sma, bb_lower = calcular_bollinger(cierres)
    
    return {
        "precio": cierres[-1],
        "atr": atr_val,
        "bb_upper": bb_upper,
        "bb_lower": bb_lower
    }
# ============================================================
# ALERTAS Y SEGUIMIENTO DE OPERACIONES
# ============================================================
def mandar_prealerta(symbol, data, direccion, score, motivos, zona):
    precio = data["5m"]["precio"]
    atr_value = data["4h"]["atr"] if "4h" in data and data["4h"].get("atr") else (precio * 0.02)
    
    stop, tp1, tp2 = calcular_tp_sl(precio, atr_value, direccion)
    
    titulo = "🟡 PREALERTA LONG" if direccion == "LONG" else "🟠 PREALERTA SHORT"
    mensaje = (f"{titulo}\n{symbol}\n\n"
               f"💰 Precio actual: {precio_texto(precio)}\n"
               f"🔎 Zona detectada: {zona}\n\n"
               f"🛑 Stop Loss sugerido: {precio_texto(stop)}\n"
               f"🎯 Take Profit 1: {precio_texto(tp1)}\n"
               f"🎯 Take Profit 2: {precio_texto(tp2)}\n\n"
               f"📊 Score: {score}\n"
               f"Motivos: {', '.join(motivos)}")
    
    enviar_telegram(mensaje)
    
    # Registramos la operación para hacerle seguimiento de TP
    señales_activas[symbol] = {
        "direccion": direccion,
        "precio_entrada": precio,
        "tp1": tp1,
        "tp2": tp2,
        "estado": "esperando_tp1"
    }
    guardar_estado()

def vigilar_señales_activas():
    global señales_activas
    for symbol, datos_senal in list(señales_activas.items()):
        velas_actuales = obtener_velas(symbol, "5m", 1)
        if not velas_actuales: continue
            
        precio_actual = velas_actuales[-1]["close"]
        direccion = datos_senal.get("direccion")
        tp1 = datos_senal.get("tp1")
        tp2 = datos_senal.get("tp2")
        precio_entrada = datos_senal.get("precio_entrada")
        estado = datos_senal.get("estado")
        
        if direccion == "LONG":
            if estado == "esperando_tp1" and precio_actual >= tp1:
                enviar_telegram(f"🔥 {symbol} alcanzó TP1 ({precio_texto(tp1)}).\n🛡️ Mueve Stop Loss a {precio_texto(precio_entrada)}.")
                señales_activas[symbol]["estado"] = "esperando_tp2"
                guardar_estado()
            elif estado == "esperando_tp2" and precio_actual >= tp2:
                enviar_telegram(f"🚀 {symbol} alcanzó TP2 ({precio_texto(tp2)}).\n✅ Operación completada.")
                del señales_activas[symbol]
                guardar_estado()
        elif direccion == "SHORT":
            if estado == "esperando_tp1" and precio_actual <= tp1:
                enviar_telegram(f"🔥 {symbol} alcanzó TP1 ({precio_texto(tp1)}).\n🛡️ Mueve Stop Loss a {precio_texto(precio_entrada)}.")
                señales_activas[symbol]["estado"] = "esperando_tp2"
                guardar_estado()
            elif estado == "esperando_tp2" and precio_actual <= tp2:
                enviar_telegram(f"🚀 {symbol} alcanzó TP2 ({precio_texto(tp2)}).\n✅ Operación completada.")
                del señales_activas[symbol]
                guardar_estado()

# ============================================================
# BUCLE PRINCIPAL (MAIN)
# ============================================================
def ejecutar_bot():
    cargar_estado()
    print("Bot iniciado...")
    # Aquí defines tu lista de monedas (ejemplo simplificado)
    monedas = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    
    while True:
        try:
            for symbol in monedas:
                # 1. Obtener datos
                velas_4h = obtener_velas(symbol, "4h", 50)
                velas_5m = obtener_velas(symbol, "5m", 50)
                
                if not velas_4h or not velas_5m: continue
                
                data = {
                    "4h": analizar_temporalidad(velas_4h),
                    "5m": analizar_temporalidad(velas_5m)
                }
                
                # 2. Lógica de entrada básica con Bollinger (Ejemplo)
                precio_actual = data["5m"]["precio"]
                bb_lower = data["5m"].get("bb_lower")
                
                # Si el precio toca la banda inferior y no hay señal activa
                if bb_lower and precio_actual <= bb_lower and symbol not in señales_activas:
                    mandar_prealerta(symbol, data, "LONG", 10, ["Precio en Banda Bollinger Inferior"], "Soporte Dinámico")
            
            # 3. Vigilar operaciones en curso
            vigilar_señales_activas()
            
        except Exception as e:
            print(f"Error en el ciclo principal: {e}")
            
        time.sleep(60) # Espera 1 minuto antes del próximo escaneo
            # Servidor para Render Free
app = Flask(__name__)
@app.route('/')
def home(): return "Bot futuros OK"
def run_flask():
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 10000)))
threading.Thread(target=run_flask, daemon=True).start()

if __name__ == "__main__":
    ejecutar_bot()
    

    
        
        

threading.Thread(target=reporte_diario_estado, daemon=True).start()
