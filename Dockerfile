# Офіційний Python
FROM python:3.11-slim

# Робоча директорія
WORKDIR /app

# 1) Спочатку копіюємо ТІЛЬКИ requirements і ставимо залежності
COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# 2) Потім копіюємо весь код
COPY . .

# Cloud Run слухає цей порт
ENV PORT=8080

# Старт бота
CMD ["python", "bot.py"]
