# Використовуємо офіційний Python-образ
FROM python:3.11-slim

# Встановлюємо робочу директорію
WORKDIR /app

# Копіюємо всі файли у контейнер
COPY . .

# Встановлюємо залежності
RUN pip install --no-cache-dir -r requirements.txt

# Вказуємо порт (Cloud Run використовує $PORT)
ENV PORT=8080

# Запускаємо бота
CMD exec python bot.py
