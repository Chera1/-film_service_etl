import time

from config.settings import MainTimingSettings
from src.elastic_loader import ElasticLoader
from src.postgres_extractor import PgExtractor
from src.sql_queries import filmwork_fw_query, filmwork_g_query, filmwork_p_query, genre_query
from src.state_storage import JsonFileStorage, State


REFRESH_TIME = MainTimingSettings().etl_refresh_time

# def __run_es_loader(
#     pg_extractor: PgExtractor,
#     es_loader: ElasticLoader,
#     state_store: State,
#     queries_dict: dict,
# ) -> None:
#     """
#     Процедура запуска перекладки данных из Postgres в Elasticsearch.
#
#     :param pg_extractor: Экземпляр класса PgExtractor
#     :param es_loader: Экземпляр класса ElasticLoader
#     :param state_store: Хранилище состояний
#     :param queries_dict: Словарь сущность: sql-запрос
#     """
#
#     while True:
#         for entity, query in queries_dict.items():
#             batch = pg_extractor.load_filmworks(
#                 query, state_store.get_state(entity, "2020-01-01")
#             )
#             for filmworks in batch:
#                 es_loader.upload_filmworks(filmworks)
#                 state_store.set_state(
#                     entity, filmworks[-1].modified.strftime("%Y-%m-%d %H:%M:%S.%f")
#                 )
#         time.sleep(REFRESH_TIME)


def run_es_loader(state_store: State, etl_steps: dict) -> None:
    while True:
        for step in etl_steps.values():
            queries_dict = step['queries']
            for query_key, query in queries_dict.items():
                batch = step['load_func'](
                    query, state_store.get_state(query_key, "2020-01-01")
                )
                for entity in batch:
                    step['upload_func'](entity)
                    state_store.set_state(
                        query_key, entity[-1].modified.strftime("%Y-%m-%d %H:%M:%S.%f")
                    )
        time.sleep(REFRESH_TIME)


if __name__ == "__main__":
    pg = PgExtractor()
    es = ElasticLoader()
    json_storage = JsonFileStorage("storage.json")
    state = State(json_storage)
    steps = {
        "filmworks": {
            "queries": {
                "filmwork_fw": filmwork_fw_query,
                "filmwork_g": filmwork_g_query,
                "filmwork_p": filmwork_p_query,
            },
            "load_func": pg.load_filmworks,
            "upload_func": es.upload_filmworks
        },
        "genre": {
            "queries": {
                "genres": genre_query
            },
            "load_func": pg.load_genres,
            "upload_func": es.upload_genres
        }
    }
    run_es_loader(state, steps)
