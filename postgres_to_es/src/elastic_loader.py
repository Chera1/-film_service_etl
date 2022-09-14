import json
import os
import time
from functools import wraps

from dotenv import load_dotenv
from elasticsearch import Elasticsearch, exceptions

from .models import FilmWork


load_dotenv()
ES_CONFIG = {
    "host": os.environ.get("ES_HOST"),
    "port": os.environ.get("ES_PORT"),
}
ES_CONNECTION_WAIT_TIME = 1


def retry_es(func):
    """
    Декоратор для инициализации повторного подключения.

    :return: результат выполнения функции
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        cls = args[0]
        while True:
            try:
                return func(*args, **kwargs)
            except exceptions.ConnectionError:
                print("ES request error!")
                cls._connect()

    return wrapper


class ElasticLoader:
    """Класс загрузки данных в Elasticsearch."""

    def __init__(self):
        self.__es = None
        self._connect()
        self.__create_index()

    def _connect(self) -> None:
        """Метод для инициализации подключения к Elasticsearch."""

        self.__es = Elasticsearch([ES_CONFIG], timeout=300)
        while not self.__es.ping():
            self.__es = Elasticsearch([ES_CONFIG], timeout=300)
            time.sleep(ES_CONNECTION_WAIT_TIME)
            print("Waiting connecting to elasticsearch...")
        print("Elasticsearch is connected!")

    @retry_es
    def __create_index(self) -> None:
        """Метод создания индекса в Elasticsearch."""

        if self.__es.indices.exists(index="movies"):
            print('Index "movies" already exists')
        else:
            with open("es_index.json", "r") as f:
                es_index = json.load(f)
            self.__es.indices.create(
                index="movies",
                mappings=es_index["mappings"],
                settings=es_index["settings"],
            )
            print('Index "movies" was created')

    @retry_es
    def upload_filmworks(self, filmworks: list[FilmWork]) -> None:
        """
        Метод загрузки данных в Elasticsearch.

        :param filmworks: список фильмов для загрузки
        """

        body = []
        for filmwork in filmworks:
            body += filmwork.to_es_type()
        self.__es.bulk(filter_path="items.*.error", body=body)
        print("Index was loaded to ES!")
