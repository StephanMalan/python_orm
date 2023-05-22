# pylint: disable=W0212
from test.dialects import DATABASE, MYSQL_CONFIG, PASSWORD, POSTGRESS_CONFIG, USER
from threading import Thread
from typing import Type

import pytest

import src


class TestPostgresMultithreading:
    databases = [POSTGRESS_CONFIG, MYSQL_CONFIG]

    def _get_database_sleep_sql(self, db_type: Type[src.Database]) -> str:
        if db_type == src.PostgresDatabase:
            return "UPDATE book set name = (SELECT pg_sleep(1));"
        elif db_type == src.MySQLDatabase:
            return "UPDATE book set name = (SELECT sleep(1));"
        raise ValueError(f"Invalid db type provided: {db_type}.")

    def test_multithreading(self, db_type: Type[src.Database], db_hostname: str) -> None:
        conn_details = src.DatabaseConfig(host=db_hostname, user=USER, password=PASSWORD, database=DATABASE, maxconn=2)
        db = db_type(conn_details)

        class Book(src.BaseModel):
            name: str = src.CharField(max_length=32)

        db.create_table(Book)

        book = Book(name="1984")
        db.save(book)

        # Start new thread with db connection that will sleep for 1 second then update the table
        def run_sleep_sql() -> None:
            db._execute_update(self._get_database_sleep_sql(db_type))

        sleep_thread = Thread(target=run_sleep_sql)
        sleep_thread.start()

        # Value returned before thread can modify the value
        books = db.query(Book)
        assert len(books) == 1
        assert books.first().to_dict() == {"id": 1, "name": "1984"}
        sleep_thread.join()

    def test_multithreading_no_connection_available(self, db_type: Type[src.Database], db_hostname: str) -> None:
        conn_details = src.DatabaseConfig(host=db_hostname, user=USER, password=PASSWORD, database=DATABASE, maxconn=1)
        db = db_type(conn_details)

        class Book(src.BaseModel):
            name: str = src.CharField(max_length=32)

        db.create_table(Book)

        book = Book(name="1984")
        db.save(book)

        # Start new thread with db connection that will wait for 3 seconds then update the table
        def run_sleep_sql() -> None:
            db._execute_update(self._get_database_sleep_sql(db_type))

        sleep_thread = Thread(target=run_sleep_sql)
        sleep_thread.start()

        # Database connection started with a max of 1 connection, and it's being used by the other thread
        with pytest.raises(src.NoConnectionError):
            db.create_table(Book)
        sleep_thread.join()
