FROM python:3.11-slim

# Working directory
WORKDIR /app

# Dependencies pehle copy karo (caching ke liye)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Sirf bot.py copy karo (renamer local pe chalega)
COPY bot.py .

# Koyeb env vars se config lega, .env ki zaroorat nahi
CMD ["python", "bot.py"]
