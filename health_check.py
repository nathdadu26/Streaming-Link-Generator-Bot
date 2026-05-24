import os
import asyncio
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SELF_URL      = os.getenv("KOYEB_PUBLIC_DOMAIN", "")   # set this in Koyeb env vars
PING_INTERVAL = 840  # 14 minutes — before Koyeb free tier sleeps at 15min

# =========================
# HEALTH CHECK SERVER
# =========================

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, format, *args):
        pass  # suppress default access logs


def run_server():
    server = HTTPServer(("0.0.0.0", 8000), HealthHandler)
    logger.info("Health check server running on :8000")
    server.serve_forever()


# =========================
# SELF PING
# =========================

async def self_ping():
    if not SELF_URL:
        logger.warning("KOYEB_PUBLIC_DOMAIN not set — self-ping disabled.")
        return

    url = SELF_URL if SELF_URL.startswith("http") else f"https://{SELF_URL}"
    logger.info(f"Self-ping started → {url} every {PING_INTERVAL}s")

    async with httpx.AsyncClient(timeout=10) as client:
        while True:
            await asyncio.sleep(PING_INTERVAL)
            try:
                r = await client.get(url)
                logger.info(f"Self-ping OK ({r.status_code})")
            except Exception as e:
                logger.warning(f"Self-ping failed: {e}")


# =========================
# ENTRY POINT
# =========================

if __name__ == "__main__":
    # Start HTTP server in background thread
    Thread(target=run_server, daemon=True).start()

    # Run self-ping loop
    asyncio.run(self_ping())
