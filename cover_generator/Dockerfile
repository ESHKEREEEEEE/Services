#Образ с питоном
FROM python:3.11-slim

#Рабочая директория контейнера
WORKDIR /app

#Скопировать requirements.txt в контейнер
COPY requirements.txt .

#Установить все из requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

#Скопировать все остальное
COPY . .

#Запустить
CMD ["python", "-u", "cover_generator.py"]
