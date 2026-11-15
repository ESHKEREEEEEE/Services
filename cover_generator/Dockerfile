# Образ с питоном
FROM python:3.11-slim

# Рабочая директория контейнера
WORKDIR /app

# Установка системных зависимостей для llama-cpp-python
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    git \
    libgomp1 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Копируем requirements.txt в контейнер
COPY requirements.txt .

# Устанавливаем все из requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Создаем директорию для моделей
RUN mkdir -p /app/models

# Копируем скрипт для скачивания модели
COPY download_model.py /app/download_model.py

# Скачиваем модель
RUN python /app/download_model.py

# Копируем остальные файлы
COPY . .

# Запускаем
CMD ["python", "-u", "cover_generator.py"]
