import time
from functools import wraps

import psycopg2

from .log_writer import logger


def backoff(start_sleep_time: float = 0.1, factor: int = 2, border_sleep_time: int = 10):
    """
    Backoff decorator for repeating a function after some time if a error raised
    Uses naive exponential growth of the repeat time (factor) up to the boundary sleep time (border_sleep_time)

    :param start_sleep_time: starting sleep time
    :param factor: factor to increase waiting time
    :param border_sleep_time: border sleep time
    :return: function
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
                    logger.error("PG connection is broken. Reconnection after %s seconds", t)

        return inner

    return func_wrapper
