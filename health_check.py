"""
Health Check Server — Koyeb Free Tier Keep-Alive
=================================================
Yeh ek halka Flask server chalata hai jo:
  1. "/" aur "/health" endpoint pe 200 OK deta hai (Koyeb health check ke liye)
  2. Khud ko har 10 minute mein ping karta hai (sleep avoid karne ke liye)

bot.py ke saath ek hi thread mein run hota hai.
"""

import os
import time
import logging
import threading
import requests
from flask import Flask

logger = logging.getLogger(__name__)

app = Flask(__name__)

# Koyeb deployment ka public URL — agar set ho to self-ping karega
SELF_URL = os.getenv("KOYEB_PUBLIC_URL", "").strip().rstrip("/")
PING_INTERVAL = int(os.getenv("PING_INTERVAL_SECONDS", "600"))  # 10 min default


@app.route("/")
def home():
    return {"status": "alive", "service": "r2-renamer-bot"}, 200


@app.route("/health")
def health():
    return {"status": "ok"}, 200


def self_ping_loop():
    """Background thread — khud ko ping karte rehna taaki Koyeb sleep na kare."""
    if not SELF_URL:
        logger.warning(
            "KOYEB_PUBLIC_URL set nahi hai — self-ping disabled. "
            "Koyeb dashboard → Environment Variables mein apna public URL daalo."
        )
        return

    logger.info(f"Self-ping shuru: {SELF_URL}/health (har {PING_INTERVAL}s)")
    time.sleep(30)  # server ready hone do

    while True:
        try:
            resp = requests.get(f"{SELF_URL}/health", timeout=10)
            if resp.status_code == 200:
                logger.info(f"Self-ping OK (200)")
            else:
                logger.warning(f"Self-ping unexpected status: {resp.status_code} — check KOYEB_PUBLIC_URL")
        except Exception as e:
            logger.warning(f"Self-ping failed: {e}")

        time.sleep(PING_INTERVAL)


def run_health_server(port: int = None):
    """Flask server ko background thread mein chalao."""
    port = port or int(os.getenv("PORT", "8000"))

    flask_thread = threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False),
        daemon=True,
    )
    flask_thread.start()
    logger.info(f"Health check server running on port {port}")

    ping_thread = threading.Thread(target=self_ping_loop, daemon=True)
    ping_thread.start()


if __name__ == "__main__":
    # Standalone test ke liye
    logging.basicConfig(level=logging.INFO)
    run_health_server()
    while True:
        time.sleep(60)
