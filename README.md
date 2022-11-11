# Проектная работа 3 спринта

## Запуск ETL
Для запуска ETL необходимо:
1. Создать файл `postgres_to_es/config/.env` с параметрами:
```dotenv
DB_NAME - название базы данных
DB_USER - user базы данных
DB_PASSWORD - пароль от базы данных
DB_HOST - хост базы данных
DB_PORT - порт базы данных

ES_HOST - хост Elastic search
ES_PORT - порт Elastic search
```

2. Выполнить команды:
```shell
pip install poetry
poetry config virtualenvs.create false
poetry install

cd postgres_to_es
python main.py
```
