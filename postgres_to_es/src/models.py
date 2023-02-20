import uuid
from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel


class FilmWork(BaseModel):
    """Film model"""

    id: uuid.UUID
    title: str
    description: Optional[str]
    rating: Optional[float]
    type: str
    created: datetime
    modified: datetime
    creation_date: Optional[date]
    actors: list[Optional[dict]]
    writers: list[Optional[dict]]
    directors: list[Optional[dict]]
    genres: list[Optional[dict]]
    tag: Optional[str]
    price: float

    def to_es_type(self) -> list[dict]:
        """
        Method for converting film object to a list suitable for loading into Elasticsearch

        :return: list to upload to Elasticsearch
        """

        first_row = {"index": {"_index": "movies", "_id": self.id}}
        second_row = {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "imdb_rating": self.rating,
            "creation_date": self.creation_date,
            "actors": self.actors,
            "actors_names": [actor.get("name") for actor in self.actors],
            "director": [director.get("name") for director in self.directors],
            "genre": self.genres,
            "writers": self.writers,
            "writers_names": [writer.get("name") for writer in self.writers],
            "tag": self.tag,
            "price": self.price,
        }
        return [first_row, second_row]


class Genre(BaseModel):
    """Genre model"""

    id: uuid.UUID
    name: str
    description: Optional[str]
    created: datetime
    modified: datetime

    def to_es_type(self) -> list[dict]:
        """
        Method for converting genre object to a list suitable for loading into Elasticsearch

        :return: list to upload to Elasticsearch
        """

        first_row = {"index": {"_index": "genres", "_id": self.id}}
        second_row = {
            "id": self.id,
            "name": self.name,
            "description": self.description
        }
        return [first_row, second_row]


class Person(BaseModel):
    """Person model"""

    id: uuid.UUID
    name: str
    created: datetime
    modified: datetime
    role: list[str]
    film_ids: list[uuid.UUID]

    def to_es_type(self) -> list[dict]:
        """
        Method for converting person object to a list suitable for loading into Elasticsearch

        :return: list to upload to Elasticsearch
        """

        first_row = {"index": {"_index": "persons", "_id": self.id}}
        second_row = {
            "id": self.id,
            "name": self.name,
            "role": self.role,
            "film_ids": self.film_ids
        }
        return [first_row, second_row]
