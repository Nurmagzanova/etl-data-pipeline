FROM python:3.9-slim

WORKDIR /app

# Копирование файла зависимостей
COPY requirements.txt .

# Установка Python зависимостей
RUN pip install --no-cache-dir -r requirements.txt

# Копирование исходного кода
COPY data-pipeline/ ./data-pipeline/
COPY sql/ ./sql/

# Команда запуска
CMD ["python", "data-pipeline/src/main.py"]