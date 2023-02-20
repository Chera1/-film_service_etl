# ETL service from Postgres to Elasticsearch

## Local run

Firstly create env file `postgres_to_es/config/.env` with following parameters:

```dotenv
DB_HOST - Postgres host
DB_PORT - Postgres port
DB_NAME - Postgres database name
DB_USER - Postgres user
DB_PASSWORD - Postgres password

ES_HOST - ElasticSearch host
ES_PORT - ElasticSearch port
```

To run etl process execute following command:
```shell
pip install poetry
poetry install

cd postgres_to_es
python main.py
```
