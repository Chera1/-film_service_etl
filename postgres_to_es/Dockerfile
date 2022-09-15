FROM python:3.10.6-slim

WORKDIR /opt/app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY pyproject.toml pyproject.toml
COPY poetry.lock poetry.lock

RUN  apt-get update \
     && pip install --upgrade pip \
     && pip install poetry \
     && poetry config virtualenvs.create false \
     && poetry install --no-dev --no-interaction --no-ansi

COPY . .

ENTRYPOINT ["python", "main.py"]
