import time

from src.elastic_loader import ElasticLoader
from src.postgres_extractor import PgExtractor
from src.sql_queries import filmwork_query, genre_query, person_query
from src.state_storage import JsonFileStorage, State


REFRESH_TIME = 5


def run_es_loader(
    pg_extractor: PgExtractor,
    es_loader: ElasticLoader,
    state_store: State,
    queries_dict: dict,
) -> None:
    """
    Процедура запуска перекладки данных из Postgres в Elasticsearch.

    :param pg_extractor: Экземпляр класса PgExtractor
    :param es_loader: Экземпляр класса ElasticLoader
    :param state_store: Хранилище состояний
    :param queries_dict: Словарь сущность: sql-запрос
    """

    while True:
        for entity, query in queries_dict.items():
            batch = pg_extractor.load_filmworks(
                query, state_store.get_state(entity, "2020-01-01")
            )
            for filmworks in batch:
                es_loader.upload_filmworks(filmworks)
                state_store.set_state(
                    entity, filmworks[-1].modified.strftime("%Y-%m-%d %H:%M:%S.%f")
                )
        time.sleep(REFRESH_TIME)


if __name__ == "__main__":
    pg = PgExtractor()
    es = ElasticLoader()
    json_storage = JsonFileStorage("storage.json")
    state = State(json_storage)
    queries = {
        "filmworks": filmwork_query,
        "genres": genre_query,
        "persons": person_query,
    }
    run_es_loader(pg, es, state, queries)
