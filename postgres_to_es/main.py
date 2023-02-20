import time

from config.settings import MainTimingSettings
from src.elastic_loader import ElasticLoader
from src.postgres_extractor import PgExtractor
from src.sql_queries import (filmwork_fw_query, filmwork_g_query,
                             filmwork_p_query, genre_query, person_query)
from src.state_storage import JsonFileStorage, State

REFRESH_TIME = MainTimingSettings().etl_refresh_time


def run_es_loader(state_store: State, etl_steps: dict) -> None:
    """
    Procedure for start etl process

    :param state_store: state store
    :param etl_steps: etl steps
    """

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
        "genres": {
            "queries": {
                "genre": genre_query
            },
            "load_func": pg.load_genres,
            "upload_func": es.upload_genres
        },
        "persons": {
            "queries": {
                "person": person_query
            },
            "load_func": pg.load_persons,
            "upload_func": es.upload_persons
        }
    }
    run_es_loader(state, steps)
