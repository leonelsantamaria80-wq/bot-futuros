import os, telebot
from flask import Flask
from threading import Thread

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = "8898482159"

bot = telebot.TeleBot(BOT_TOKEN)
app = Flask('')

productos = {}

@bot.message_handler(commands=['start'])
def start(m):
    uid = str(m.from_user.id)
    if uid == ADMIN_ID:
        bot.reply_to(m, "✅ BIENVENIDO ADMIN LEO!\n\nYa sos admin.\n\nComandos:\n/agregar Nombre | Precio | Stock\nEjemplo: /agregar iPhone 13 | 500000 | 5\n\n/stock para ver stock")
    else:
        bot.reply_to(m, f"Bot activo! ID: {uid}")

@bot.message_handler(commands=['agregar'])
def agregar(m):
    if str(m.from_user.id)!= ADMIN_ID: return
    try:
        txt = m.text.replace("/agregar","").strip()
        nom, pre, stk = [x.strip() for x in txt.split("|")]
        productos[nom] = {"precio":pre,"stock":stk}
        bot.reply_to(m, f"✅ Agregado: {nom} - ${pre} - Stock {stk}")
    except:
        bot.reply_to(m, "Usa: /agregar Nombre | Precio | Stock")

@bot.message_handler(commands=['stock'])
def ver(m):
    if not productos:
        bot.reply_to(m, "Vacio")
        return
    t="📦 STOCK:\n\n"
    for k,v in productos.items():
        t+=f"• {k} - ${v['precio']} - {v['stock']}u\n"
    bot.reply_to(m,t)

@app.route('/')
def home(): return "OK"

def run(): bot.infinity_polling()
Thread(target=run).start()
app.run(host="0.0.0.0", port=10000)
