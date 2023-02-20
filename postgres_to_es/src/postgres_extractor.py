import psycopg2
from psycopg2.extras import DictCursor, register_uuid

from config.settings import MainTimingSettings, PgSettings
from .backoff import backoff
from .log_writer import logger
from .models import FilmWork, Genre, Person


class PgExtractor:
    """Class to extract data from Postgres"""

    timing_settings = MainTimingSettings()
    __start_sleep_time = timing_settings.backoff_start_sleep_time
    __factor = timing_settings.backoff_factor
    __border_sleep_time = timing_settings.backoff_border_sleep_time

    def __init__(self):
        self.__dsn = PgSettings().dict()
        self.__connection = None
        self.__cursor = None
        self.__connect()

    @backoff(__start_sleep_time, __factor, __border_sleep_time)
    def __connect(self) -> None:
        """Initialize connection to Postgres"""

        register_uuid()
        self.__connection = psycopg2.connect(**self.__dsn)
        self.__cursor = self.__connection.cursor(cursor_factory=DictCursor)
        logger.info("Postgres connected!")

    def __execute(self, query, modified):
        try:
            self.__cursor.execute(query, {'modified': modified})
        except psycopg2.OperationalError:
            self.__connect()
            self.__cursor.execute(query, {'modified': modified})
        return self.__cursor

    def load_filmworks(self, query: str, modified: str) -> list[FilmWork]:
        """
        Yields films from Postgres

        :param query: SQL query
        :param modified: modification datetime
        :return: films list
        """

        cursor = self.__execute(query, modified)
        logger.info("Data extract filmworks from PG")
        while rows := cursor.fetchmany(100):
            yield [FilmWork(**row) for row in rows]

    def load_genres(self, query: str, modified: str) -> list[Genre]:
        """
        Yields genres from Postgres

        :param query: SQL query
        :param modified: modification datetime
        :return: genres list
        """

        cursor = self.__execute(query, modified)
        logger.info("Data extract genres from PG")
        while rows := cursor.fetchmany(100):
            yield [Genre(**row) for row in rows]

    def load_persons(self, query: str, modified: str) -> list[Person]:
        """
        Yields persons from Postgres

        :param query: SQL query
        :param modified: modification datetime
        :return: persons list
        """

        cursor = self.__execute(query, modified)
        while rows := cursor.fetchmany(100):
            yield [Person(**row) for row in rows]

    def __del__(self):
        for c in (self.__cursor, self.__connection):
            try:
                c.close()
            except:
                pass
