"""
health_check.py
---------------
Lightweight HTTP server jo Koyeb ke health-check endpoint ko handle karta hai.
Bot ke saath parallel thread mein chalta hai.

Koyeb free tier mein agar koi HTTP port expose nahi hota
to service "unhealthy" mark ho ke sleep ho jaati hai.
Yeh file usi problem ka solution hai.

Self-ping feature: har 20 minute mein bot khud apne /health endpoint ko
hit karta hai taaki Koyeb service kabhi sleep na ho.
"""
import os
import time
import threading
import logging
import urllib.request
from http.server import HTTPServer, BaseHTTPRequestHandler

log = logging.getLogger(__name__)

PORT      = int(os.getenv("PORT", 8000))
PING_URL  = os.getenv("KOYEB_PUBLIC_URL", f"http://localhost:{PORT}") + "/health"
PING_INTERVAL = 20 * 60  # 20 minutes


# ─────────────────────────────────────────────
#  HTTP HANDLER
# ─────────────────────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/health"):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            self.send_response(404)
            self.end_headers()

    # Access log spam band karo
    def log_message(self, format, *args):
        pass


# ─────────────────────────────────────────────
#  SELF-PING LOOP
# ─────────────────────────────────────────────

def _ping_loop():
    """
    Har 20 minute mein apne /health endpoint ko ping karo.
    Pehli baar 30 second baad — server ko start hone ka time dene ke liye.
    """
    time.sleep(30)
    while True:
        try:
            with urllib.request.urlopen(PING_URL, timeout=10) as resp:
                log.info("Self-ping OK: %s → %d", PING_URL, resp.status)
        except Exception as e:
            log.warning("Self-ping failed: %s", e)
        time.sleep(PING_INTERVAL)


# ─────────────────────────────────────────────
#  PUBLIC API
# ─────────────────────────────────────────────

def start_health_server():
    """Background threads mein HTTP server + self-ping loop start karo."""

    # 1. HTTP server
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    log.info("Health-check server started on port %d", PORT)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    # 2. Self-ping loop
    ping_thread = threading.Thread(target=_ping_loop, daemon=True, name="self-ping")
    ping_thread.start()
    log.info("Self-ping loop started (interval=%ds, url=%s)", PING_INTERVAL, PING_URL)

    return server
