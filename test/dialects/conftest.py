# pylint: disable=W0212, W0621
from __future__ import annotations

import test.dialects as test
from typing import Type

import pytest
from _pytest.python import Metafunc

import src


def pytest_generate_tests(metafunc: Metafunc) -> None:
    if not metafunc.cls:
        return
    databases: list[tuple[Type[src.Database], str]] = metafunc.cls.databases
    arg_names = ["req_db_type", "req_host_name"]
    arg_values = [[db_type, hostname] for db_type, hostname in databases]
    id_list = [db_type.__name__ for db_type, _ in databases]
    metafunc.parametrize(arg_names, arg_values, ids=id_list, scope="class")


@pytest.fixture
def db(req_db_type: Type[src.Database], req_host_name: str) -> src.Database:
    conn_details = src.DatabaseConfig(
        host=req_host_name, user=test.USER, password=test.PASSWORD, database=test.DATABASE
    )
    database_type: Type[src.Database] = req_db_type
    _db = database_type(conn_details)
    return _db


@pytest.fixture
def db_type(req_db_type: Type[src.Database]) -> Type[src.Database]:
    return req_db_type


@pytest.fixture
def db_hostname(req_host_name: str) -> str:
    return req_host_name


@pytest.fixture(autouse=True)
def clean_up(db: src.Database) -> None:
    db._drop_tables(database=test.DATABASE)
