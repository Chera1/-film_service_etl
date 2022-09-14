import os

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import DictCursor

from .models import FilmWork

from .backoff import backoff


load_dotenv()
DSN = {
    "dbname": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("DB_HOST", "127.0.0.1"),
    "port": os.environ.get("DB_PORT", 5432),
}


class PgExtractor:
    """Класс для выгрузки данных из Postgres."""

    __start_sleep_time = 0.1
    __factor = 2
    __border_sleep_time = 10

    def __init__(self):
        self.__connection = None
        self.__cursor = None
        self.__connect()

    @backoff(__start_sleep_time, __factor, __border_sleep_time)
    def __connect(self) -> None:
        """Метод инициализирующий подключение к Postgres."""

        self.__connection = psycopg2.connect(**DSN)
        self.__cursor = self.__connection.cursor(cursor_factory=DictCursor)
        print("Postgres connected!")

    def load_filmworks(self, query: str, modified: str) -> list[FilmWork]:
        """
        Метод (генератор) выгрузки данных из Postgres.

        :param query: SQL запрос
        :param modified: Дата и время модификации записи в таблице
        :return: Список фильмов
        """

        try:
            self.__cursor.execute(query, (modified,))
        except psycopg2.OperationalError:
            self.__connect()
            self.__cursor.execute(query, (modified,))
        print("Data extract from PG")
        while rows := self.__cursor.fetchmany(100):
            yield [FilmWork(**row) for row in rows]

    def __del__(self):
        for c in (self.__cursor, self.__connection):
            try:
                c.close()
            except:
                pass
