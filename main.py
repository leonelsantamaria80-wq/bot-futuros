import requests
import time
from datetime import datetime

TOKEN = "ACA VA TU TOKEN DE TELEGRAM"
CHAT_ID = "ACA VA TU CHAT_ID"
MIN_GANANCIA = 1.5
TIMEFRAMES = ["5m","15m","1h","4h","1d"]

MONEDAS = ["BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT","ADAUSDT","DOGEUSDT","SHIBUSDT","AVAXUSDT","DOTUSDT","LINKUSDT","LTCUSDT","MATICUSDT","TRXUSDT","UNIUSDT","ATOMUSDT","ETCUSDT","FILUSDT","XLMUSDT","HBARUSDT","NEARUSDT","APTUSDT","ARBUSDT","OPUSDT","INJUSDT","RNDRUSDT","PEPEUSDT","SUIUSDT","TIAUSDT","SEIUSDT","WIFUSDT","BONKUSDT","FLOKIUSDT","JUPUSDT","ENAUSDT","WUSDT","PYTHUSDT","STRKUSDT","LDOUSDT","STXUSDT"]

def send(msg):
    try: requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage", json={"chat_id": CHAT_ID, "text": msg}, timeout=15)
    except: pass

def get_data(symbol, interval):
    try:
        url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit=100"
        d = requests.get(url, timeout=10).json()
        closes = [float(x[4]) for x in d]
        highs = [float(x[2]) for x in d]
        lows = [float(x[3]) for x in d]
        vols = [float(x[5]) for x in d]
        return closes, highs, lows, vols
    except: return None, None, None, None

def ema(arr, p):
    k=2/(p+1); e=arr[0]
    for x in arr[1:]: e=x*k+e*(1-k)
    return e

def calc_ema_list(arr, p):
    k=2/(p+1); l=[arr[0]]
    for x in arr[1:]: l.append(x*k+l[-1]*(1-k))
    return l

def calc_rsi(closes, period=14):
    gains=losses=0
    for i in range(-period,0):
        d=closes[i]-closes[i-1]
        if d>0: gains+=d
        else: losses-=d
    if losses==0: return 100
    rs=gains/losses
    return 100-(100/(1+rs))

def calc_macd(closes):
    e12=calc_ema_list(closes,12)
    e26=calc_ema_list(closes,26)
    macd=[a-b for a,b in zip(e12,e26)]
    sig=calc_ema_list(macd,9)
    return macd[-1], sig[-1]

def calc_atr(highs, lows, closes, period=14):
    trs=[]
    for i in range(1,len(closes)):
        tr=max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
        trs.append(tr)
    return sum(trs[-period:])/period

def calc_soporte_resistencia(highs, lows):
    return min(lows[-20:]), max(highs[-20:])

def calc_adx(highs, lows, closes, period=14):
    try:
        tr_list=[]; plus_dm_list=[]; minus_dm_list=[]
        for i in range(1,len(closes)):
            tr = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
            up = highs[i]-highs[i-1]
            down = lows[i-1]-lows[i]
            plus_dm = up if up>down and up>0 else 0
            minus_dm = down if down>up and down>0 else 0
            tr_list.append(tr); plus_dm_list.append(plus_dm); minus_dm_list.append(minus_dm)
        tr14=sum(tr_list[-period:]); plus14=sum(plus_dm_list[-period:]); minus14=sum(minus_dm_list[-period:])
        if tr14==0: return 0,0,0
        plus_di=100*plus14/tr14
        minus_di=100*minus14/tr14
        dx=100*abs(plus_di-minus_di)/(plus_di+minus_di) if (plus_di+minus_di)!=0 else 0
        return dx, plus_di, minus_di
    except: return 0,0,0

def check_signal_pro(closes, highs, lows, vols):
    if not closes or len(closes)<50: return None
    e9_now=ema(closes[-9:],9); e21_now=ema(closes[-21:],21)
    e9_prev=ema(closes[-10:-1],9); e21_prev=ema(closes[-22:-2],21)
    rsi=calc_rsi(closes)
    macd, sig=calc_macd(closes)
    atr=calc_atr(highs,lows,closes)
    sop,res=calc_soporte_resistencia(highs,lows)
    adx, plus_di, minus_di = calc_adx(highs,lows,closes)
    vol_prom=sum(vols[-20:])/20
    vol_ok=vols[-1] > vol_prom*1.2
    fuerza=abs(e9_now-e21_now)/closes[-1]*100
    if e9_prev<e21_prev and e9_now>e21_now and 35<rsi<68 and macd>sig and vol_ok and adx>20 and plus_di>minus_di:
        return "LONG", rsi, fuerza, atr, sop, res, "TENDENCIA FUERTE", adx
    if e9_prev>e21_prev and e9_now<e21_now and rsi>35 and macd<sig and vol_ok and adx>20 and minus_di>plus_di:
        return "SHORT", rsi, fuerza, atr, sop, res, "TENDENCIA FUERTE", adx
    return None

def get_leverage(fuerza, conf, adx):
    # CORREGIDO: MINIMO x5
    if conf>=5 and adx>28: return 10
    if conf>=4 and adx>25: return 9
    if conf>=4: return 8
    if conf==3 and adx>23: return 7
    if conf==3 and fuerza>0.4: return 6
    return 5

print("BOT PRO + ADX x5 MIN INICIADO")
send("🤖 BOT PRO + ADX x5 MIN ONLINE")

while True:
    for moneda in MONEDAS:
        long_c=0; short_c=0; señales=[]; precio_actual=None
        for tf in TIMEFRAMES:
            closes, highs, lows, vols = get_data(moneda, tf)
            if not closes: continue
            if precio_actual is None: precio_actual=closes[-1]
            res=check_signal_pro(closes, highs, lows, vols)
            if res:
                tipo,rsi,fuerza,atr,sop,resis,extra,adx=res
                señales.append((tf,tipo,rsi,fuerza,atr,sop,resis,extra,adx))
                if tipo=="LONG": long_c+=1
                else: short_c+=1
            time.sleep(0.15)
        if long_c<3 and short_c<3: continue
        tipo_final="LONG" if long_c>=3 else "SHORT"
        conf=long_c if tipo_final=="LONG" else short_c
        señales_final=[s for s in señales if s[1]==tipo_final]
        fuerza_prom=sum(s[3] for s in señales_final)/len(señales_final)
        atr_prom=sum(s[4] for s in señales_final)/len(señales_final)
        adx_prom=sum(s[8] for s in señales_final)/len(señales_final)
        precio=precio_actual
        lev=get_leverage(fuerza_prom, conf, adx_prom)
        if tipo_final=="LONG":
            sl=precio-(atr_prom*1.5)
            tp=precio+(atr_prom*2.5)
        else:
            sl=precio+(atr_prom*1.5)
            tp=precio-(atr_prom*2.5)
        ganancia=abs(tp-precio)/precio*100*lev
        if ganancia<MIN_GANANCIA: continue
        fmt=".8f" if precio<0.01 else ".2f"
        base=moneda.replace("USDT","")
        if tipo_final=="LONG":
            msg=f"Compra LONG {base}/USDT\nEntrada {precio:{fmt}}\nTP {tp:{fmt}}\nStop Loss {sl:{fmt}}\nApalancamiento sugerido x{lev}"
        else:
            msg=f"Compra SHORT {base}/USDT\nEntrada {precio:{fmt}}\nTP {tp:{fmt}}\nStop Loss {sl:{fmt}}\nApalancamiento sugerido x{lev}"
        send(msg)
    time.sleep(180)
