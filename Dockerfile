FROM --platform=linux/amd64 python:3.9-slim as base

COPY ./vegaorm /app/vegaorm
COPY ./Pipfile /app
COPY ./Pipfile.lock /app

WORKDIR /app

FROM base as dev

COPY ./test /app/test

RUN pip install pipenv
RUN pipenv uninstall psycopg2
RUN pipenv install --dev
