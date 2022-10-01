import json
import time
from functools import wraps

from elasticsearch import Elasticsearch, exceptions

from postgres_to_es.config.settings import EsSettings, MainTimingSettings
from .log_writer import logger
from .models import Genre, FilmWork


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
                logger.error("ES request error!")
                cls._connect()

    return wrapper


class ElasticLoader:
    """Класс загрузки данных в Elasticsearch."""

    def __init__(self):
        self.__es_config = EsSettings().dict()
        self.__connect_wait_time = MainTimingSettings().es_connect_wait_time
        self.__es = None
        self._connect()
        self.__create_filmwork_index()
        self.__create_genre_index()

    def _connect(self) -> None:
        """Метод для инициализации подключения к Elasticsearch."""

        self.__es = Elasticsearch([self.__es_config], timeout=300)
        while not self.__es.ping():
            self.__es = Elasticsearch([self.__es_config], timeout=300)
            time.sleep(self.__connect_wait_time)
            logger.error("Waiting connecting to elasticsearch...")
        logger.info("Elasticsearch is connected!")

    def __create_filmwork_index(self):
        self.__create_index('movies', "es_index/filmwork_es_index.json")

    def __create_genre_index(self):
        self.__create_index('genres', "es_index/genre_es_index.json")

    @retry_es
    def __create_index(self, index_name: str, index_filepath: str) -> None:
        """Метод создания индекса в Elasticsearch."""

        if self.__es.indices.exists(index=index_name):
            logger.warning(f"Index '{index_name}' already exists")
        else:
            with open(index_filepath, "r") as f:
                es_index = json.load(f)
            self.__es.indices.create(
                index=index_name,
                mappings=es_index["mappings"],
                settings=es_index["settings"],
            )
            logger.info(f"Index '{index_name}' was created")

    @retry_es
    def upload_filmworks(self, filmworks: list[FilmWork]) -> None:
        """
        Метод загрузки фильмов в Elasticsearch.

        :param filmworks: список фильмов для загрузки
        """

        body = []
        for filmwork in filmworks:
            body += filmwork.to_es_type()
        self.__es.bulk(filter_path="items.*.error", body=body)
        logger.info("Index was loaded to ES!")

    @retry_es
    def upload_genres(self, genres: list[Genre]) -> None:
        """
        Метод загрузки жанров в Elasticsearch.

        :param genres: список жанров для загрузки
        """

        body = []
        for genre in genres:
            body += genre.to_es_type()
        self.__es.bulk(filter_path="items.*.error", body=body)
        logger.info("Index was loaded to ES!")
