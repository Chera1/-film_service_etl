import json
import time
from functools import wraps

from elasticsearch import Elasticsearch, exceptions

from config.settings import EsSettings, MainTimingSettings
from .log_writer import logger
from .models import FilmWork, Genre, Person


def retry_es(func):
    """
    Reconnection decorator

    :return: function
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
    """Elasticsearch loader class"""

    def __init__(self):
        self.__es_config = EsSettings().dict()
        self.__connect_wait_time = MainTimingSettings().es_connect_wait_time
        self.__es = None
        self._connect()
        self.__create_indexes()

    def _connect(self) -> None:
        """Connects to Elasticsearch"""

        self.__es = Elasticsearch([self.__es_config], timeout=300)
        while not self.__es.ping():
            self.__es = Elasticsearch([self.__es_config], timeout=300)
            time.sleep(self.__connect_wait_time)
            logger.error("Waiting connecting to elasticsearch...")
        logger.info("Elasticsearch is connected!")

    def __create_indexes(self):
        """Creates one index in Elasticsearch"""

        self.__create_index('movies', "es_index/filmwork_es_index.json")
        self.__create_index('genres', "es_index/genre_es_index.json")
        self.__create_index('persons', "es_index/person_es_index.json")

    @retry_es
    def __create_index(self, index_name: str, index_filepath: str) -> None:
        """Creates all indexes in Elasticsearch"""

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
        Uploads films to Elasticsearch

        :param filmworks: films list to upload
        """

        body = []
        for filmwork in filmworks:
            body += filmwork.to_es_type()
        self.__es.bulk(filter_path="items.*.error", body=body)
        logger.info("Index was loaded to ES!")

    @retry_es
    def upload_genres(self, genres: list[Genre]) -> None:
        """
        Uploads genres to Elasticsearch

        :param genres: genres list to upload
        """

        body = []
        for genre in genres:
            body += genre.to_es_type()
        self.__es.bulk(filter_path="items.*.error", body=body)
        logger.info("Index was loaded to ES!")

    @retry_es
    def upload_persons(self, persons: list[Person]) -> None:
        """
        Uploads persons to Elasticsearch

        :param persons: persons list to upload
        """

        body = []
        for person in persons:
            body += person.to_es_type()
        self.__es.bulk(filter_path="items.*.error", body=body)
        logger.info("Index was loaded to ES!")
