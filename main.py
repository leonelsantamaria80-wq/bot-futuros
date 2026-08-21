import os
import threading
import asyncio
from flask import Flask
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

TOKEN = os.environ.get("BOT_TOKEN")

app = Flask(__name__)
@app.route('/')
def home():
    return "Bot Leo esta vivo!"

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Bot activo! Tu ID es: {update.effective_user.id}")

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

def run_bot():
    # Esto arregla el error de MainThread
    asyncio.set_event_loop(asyncio.new_event_loop())
    application = Application.builder().token(TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.run_polling()

if __name__ == "__main__":
    # Flask va en hilo secundario
    threading.Thread(target=run_flask, daemon=True).start()
    run_bot()
