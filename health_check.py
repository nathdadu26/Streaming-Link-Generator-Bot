"""
health_check.py
---------------
Lightweight HTTP server jo Koyeb ke health-check endpoint ko handle karta hai.
Bot ke saath parallel thread mein chalta hai.

Koyeb free tier mein agar koi HTTP port expose nahi hota
to service "unhealthy" mark ho ke sleep ho jaati hai.
Yeh file usi problem ka solution hai.
"""

import os
import threading
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler

log = logging.getLogger(__name__)

PORT = int(os.getenv("PORT", 8000))


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


def start_health_server():
    """Background thread mein HTTP server start karo."""
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    log.info("Health-check server started on port %d", PORT)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
