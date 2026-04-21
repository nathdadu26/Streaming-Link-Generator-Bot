# ─────────────────────────────────────────────
#  Dockerfile — Bunny Stream Telegram Bot
#  Base: Python 3.11 slim
# ─────────────────────────────────────────────

FROM python:3.11-slim

# System deps (ffmpeg optional, sirf agar needed ho)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Working directory
WORKDIR /app

# Requirements pehle copy karo (Docker cache optimize hoga)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Baaki files copy karo
COPY . .

# Downloads folder banana (runtime mein bhi banega, yahan sirf safety ke liye)
RUN mkdir -p downloads

# Koyeb health check port
EXPOSE 8000

# Bot start karo
CMD ["python", "bot.py"]
