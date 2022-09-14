import abc
import json
from typing import Any


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    """Класс для работы с локальным json хранилищем."""

    def __init__(self, file_path: str = None):
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        """
        Метод сохранения состояний.

        :param state: Состояние
        """

        with open(self.file_path, "w") as f:
            json.dump(state, f, indent=4)

    def retrieve_state(self) -> dict:
        """
        Метод получения состояний из локального хранилища.
        Если такого json файла нет, возвращаем пустой словарь

        :return: Словарь состояний
        """

        try:
            with open(self.file_path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return {}


class State:
    """Класс для хранения состояния при работе с данными."""

    def __init__(self, storage: BaseStorage):
        self.storage = storage
        self.state = self.storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        """
        Метод установки состояния для определённого ключа.

        :param key: Ключ состояния
        :param value: Значение состояния
        """

        self.state[key] = value
        self.storage.save_state(self.state)

    def get_state(self, key: str, alternative_state: str) -> Any:
        """
        Метод получения состояния по определённому ключу.

        :param key: Ключ состояния
        :param alternative_state: Альтернативное значение, если нет значения по искомому ключу
        :return: Значение состояния
        """

        return self.state.get(key, alternative_state)
