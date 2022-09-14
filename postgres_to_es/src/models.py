import uuid
from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class FilmWork(BaseModel):
    """Модель для фильма."""

    id: uuid.UUID
    title: str
    description: Optional[str]
    rating: Optional[float]
    type: str
    created: datetime
    modified: datetime
    actors: list[Optional[dict]]
    writers: list[Optional[dict]]
    directors: list[Optional[dict]]
    genres: list[Optional[str]]

    def to_es_type(self) -> list[dict]:
        """
        Метод для преобразования экземпляра в список, пригодный для загрузки в Elasticsearch.

        :return: Список для загрузки в Elasticsearch
        """

        first_row = {"index": {"_index": "movies", "_id": self.id}}
        second_row = {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "imdb_rating": self.rating,
            "actors": self.actors,
            "actors_names": [actor.get("name") for actor in self.actors],
            "director": [director.get("name") for director in self.directors],
            "genre": self.genres,
            "writers": self.writers,
            "writers_names": [writer.get("name") for writer in self.writers],
        }
        return [first_row, second_row]
