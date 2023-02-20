import abc
import json
from typing import Any


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Save state"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Retrieve state from local storage"""
        pass


class JsonFileStorage(BaseStorage):
    """Class to work with local JSON store"""

    def __init__(self, file_path: str = None):
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        """
        Saves state

        :param state: current state
        """

        with open(self.file_path, "w") as f:
            json.dump(state, f, indent=4)

    def retrieve_state(self) -> dict:
        """
        Method for getting states from local storage.
        If there is no such json file, we return an empty dict

        :return: state dict
        """

        try:
            with open(self.file_path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return {}


class State:
    """Class to store state"""

    def __init__(self, storage: BaseStorage):
        self.storage = storage
        self.state = self.storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        """
        Sets state by key

        :param key: state key
        :param value: state value
        """

        self.state[key] = value
        self.storage.save_state(self.state)

    def get_state(self, key: str, alternative_state: str) -> Any:
        """
        Retrieves state by key

        :param key: state key
        :param alternative_state: alternative state
        :return: state value
        """

        return self.state.get(key, alternative_state)
