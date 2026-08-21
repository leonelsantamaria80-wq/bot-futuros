from flask import Flask
import threading
import os
import ccxt
import pandas as pd
import ta
import time
import requests
from datetime import datetime

# --- NO BORRAR ESTO - ES PARA QUE FUNCIONE EN RENDER FREE ---
app = Flask(__name__)
@app.route('/')
def home(): 
    return "Bot activo"
def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
threading.Thread(target=run_flask, daemon=True).start()
# --- FIN ---

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
exchange = ccxt.binance()

def send_telegram(mensaje):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": mensaje, "parse_mode": "Markdown"}
    try:
        requests.post(url, data=data)
    except:
        pass

def analizar_moneda(par):
    try:
        ohlcv = exchange.fetch_ohlcv(par, '15m', limit=100)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['rsi'] = ta.momentum.RSIIndicator(df['close']).rsi()
        df['ema20'] = ta.trend.EMAIndicator(df['close'], window=20).ema_indicator()
        df['ema50'] = ta.trend.EMAIndicator(df['close'], window=50).ema_indicator()
        rsi = df['rsi'].iloc[-1]
        close = df['close'].iloc[-1]
        ema20 = df['ema20'].iloc[-1]
        ema50 = df['ema50'].iloc[-1]

        if rsi < 35 and close > ema20 and ema20 > ema50:
            if rsi < 25:
                apal = "x10"
                tp = "8% - 12%"
            elif rsi < 30:
                apal = "x7"
                tp = "5% - 8%"
            else:
                apal = "x5"
                tp = "3% - 5%"
            mensaje = f"🚀 *SEÑAL LONG {par}*\n\n💰 Precio: {close}\n📊 RSI: {round(rsi,2)}\n⚙️ Apalancamiento: {apal}\n🎯 TP Dinámico: {tp}\n⏰ {datetime.now().strftime('%H:%M:%S')}"
            send_telegram(mensaje)
    except Exception as e:
        print(f"Error {par}: {e}")

print("Bot iniciado")
send_telegram("✅ Bot conectado - 50 criptos activo 24/7")

pares = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT', 'XRP/USDT', 'DOGE/USDT', 'ADA/USDT', 'AVAX/USDT', 'LINK/USDT', 'DOT/USDT', 'MATIC/USDT', 'LTC/USDT', 'TRX/USDT', 'SHIB/USDT', 'UNI/USDT', 'ATOM/USDT', 'ETC/USDT', 'XLM/USDT', 'FIL/USDT', 'HBAR/USDT', 'ARB/USDT', 'OP/USDT', 'NEAR/USDT', 'APT/USDT', 'SUI/USDT', 'PEPE/USDT', 'RENDER/USDT', 'INJ/USDT', 'TIA/USDT', 'SEI/USDT', 'WLD/USDT', 'FET/USDT', 'AGIX/USDT', 'GRT/USDT', 'AAVE/USDT', 'MKR/USDT', 'RNDR/USDT', 'STX/USDT', 'IMX/USDT', 'FLOW/USDT', 'SAND/USDT', 'MANA/USDT', 'AXS/USDT', 'GALA/USDT', 'CHZ/USDT', 'ENJ/USDT', 'CRV/USDT', 'COMP/USDT', 'SNX/USDT', 'LDO/USDT']

while True:
    for par in pares:
        analizar_moneda(par)
        time.sleep(1)
    time.sleep(60)
