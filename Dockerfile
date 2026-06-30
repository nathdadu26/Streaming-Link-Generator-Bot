FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# bot.py + health_check.py dono copy karo
COPY bot.py .
COPY health_check.py .

# Koyeb yeh port expose karega health check ke liye
EXPOSE 8000

CMD ["python", "bot.py"]
