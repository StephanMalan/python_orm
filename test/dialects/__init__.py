from os import getenv

import pytest

import src

USER = getenv("USER", "")
PASSWORD = getenv("PASSWORD", "")
DATABASE = getenv("DATABASE", "")
POSTGRESS_HOSTNAME = getenv("POSTGRES_HOSTNAME", "")
MYSQL_HOSTNAME = getenv("MYSQL_HOSTNAME", "")
assert USER
assert PASSWORD
assert DATABASE
assert POSTGRESS_HOSTNAME
assert MYSQL_HOSTNAME

POSTGRESS_CONFIG = (src.PostgresDatabase, POSTGRESS_HOSTNAME)
MYSQL_CONFIG = (src.MySQLDatabase, MYSQL_HOSTNAME)
SQLITE_CONFIG = (src.SQLiteDatabase, "test.db")

pytest.register_assert_rewrite("conftest")
