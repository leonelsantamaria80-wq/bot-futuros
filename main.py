import os, time, requests
TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
def send(t):
    try:
        requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage", json={"chat_id": CHAT_ID, "text": t}, timeout=10)
    except: pass
print("Bot iniciado")
offset=0
while True:
    try:
        r=requests.get(f"https://api.telegram.org/bot{TOKEN}/getUpdates", params={"offset": offset, "timeout":20}, timeout=25).json()
        for u in r.get("result", []):
            offset=u["update_id"]+1
            txt=u.get("message",{}).get("text","")
            cid=u.get("message",{}).get("chat",{}).get("id")
            if "/start" in txt.lower(): send(f"Bot activo! ID: {cid}")
    except: time.sleep(5)
    time.sleep(2)
