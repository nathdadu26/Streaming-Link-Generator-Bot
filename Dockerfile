FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py .
COPY health_check.py .
COPY mega_downloader.py .

EXPOSE 8000

CMD ["python", "bot.py"]
