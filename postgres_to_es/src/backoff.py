import time
from functools import wraps

import psycopg2


def backoff(start_sleep_time: float = 0.1, factor: int = 2, border_sleep_time: int = 10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            n = 1
            t = start_sleep_time
            while True:
                try:
                    return func(*args, **kwargs)
                except psycopg2.OperationalError:
                    t = (
                        start_sleep_time * factor**n
                        if t < border_sleep_time
                        else border_sleep_time
                    )
                    n += 1
                    time.sleep(t)
                    print("Waiting time is ", t)

        return inner

    return func_wrapper
