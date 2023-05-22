FROM python:3.11.2-slim-bullseye as python-build-stage

ARG BUILD_ENVIRONMENT=production
ARG APP_HOME=/app

RUN apt-get update && apt-get install --no-install-recommends -y \
    build-essential \
    default-libmysqlclient-dev \
    libcurl4-openssl-dev libssl-dev \
    openssh-client
RUN pip install poetry

WORKDIR ${APP_HOME}

COPY pyproject.toml ${APP_HOME}
RUN poetry config virtualenvs.create false
RUN poetry install --no-root $(test "$BUILD_ENVIRONMENT" == production && echo "--no-dev")

COPY . ${APP_HOME}
WORKDIR ${APP_HOME}