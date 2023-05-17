from __future__ import annotations

import time
from collections import ChainMap
from dataclasses import dataclass, fields
from typing import Any, Type, TypeVar

from psycopg2 import DatabaseError, errors
from psycopg2._psycopg import connection
from psycopg2.pool import PoolError, ThreadedConnectionPool
from psycopg2.sql import SQL, Identifier

import src

CREATE_TABLE_SQL = "CREATE TABLE {} (id SERIAL PRIMARY KEY, {});"
INSERT_SQL = "INSERT INTO {} ({}) VALUES ({}) RETURNING id;"
UPDATE_SQL = "UPDATE {} SET {} WHERE id = %s;"
SELECT_SQL = "SELECT {} FROM {}{} ORDER BY id {};"
DESCRIBE_TABLE_SQL = (
    "SELECT column_name, data_type, character_maximum_length "
    "FROM information_schema.columns WHERE table_name = %s;"
)
ALTER_TABLE_SQL = "ALTER TABLE {} {}"


@dataclass
class DatabaseConfig:
    host: str
    user: str
    password: str
    database: str = "postgress"
    minconn: int = 1
    maxconn: int = 1


T = TypeVar("T", bound="src.Model")


class Database:
    def __init__(self, conn_details: DatabaseConfig) -> None:
        self.conn_details = conn_details
        self.conn_pool = self._create_conn_pool(conn_details)

    def _create_conn_pool(self, conn_details: DatabaseConfig) -> ThreadedConnectionPool:
        return ThreadedConnectionPool(**conn_details.__dict__)

    def _get_connection(self) -> connection:
        for _ in range(10):
            try:
                conn: connection = self.conn_pool.getconn()
                conn.autocommit = True
                return conn
            except PoolError:
                time.sleep(0.1)
        raise PoolError()

    def _execute(self, query: any, query_vars=None):
        conn = None
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            cur.execute(query, query_vars)
            print(cur.query)
            return cur.fetchall() if cur.description else []
        except DatabaseError as ex:
            self.conn_pool.putconn(conn, close=True)
            raise ex
        finally:
            if conn and not conn.closed:
                conn.cursor().close()
                self.conn_pool.putconn(conn)

    def create_table(self, model: Type[src.Model]):
        fields = model.get_field_definitions()
        sql_fields = [
            f"{k} {v.sql_type}{v.get_max_length()}" for k, v in fields.items()
        ]
        # Create table in database
        try:
            sql = SQL(CREATE_TABLE_SQL).format(
                SQL(model.__name__), SQL(", ".join(sql_fields))
            )
            print(str(sql))
            self._execute(sql)
        except errors.DuplicateTable:
            self._modify_table(model)

    def _modify_table(self, model: Type[src.Model]):
        results = self._execute(DESCRIBE_TABLE_SQL, (model.__name__.lower(),))
        lst = [src.FieldFactory.create_field(*field) for field in results]
        o_fields = dict(ChainMap(*lst))
        o_fields.pop("id")
        n_fields = model.get_field_definitions()
        n_keys, o_keys = set(n_fields.keys()), set(o_fields.keys())
        actions = []
        actions.extend([f"DROP COLUMN {name}" for name in o_keys.difference(n_keys)])
        to_add = dict(
            ChainMap(*[{key: n_fields[key]} for key in n_keys.difference(o_keys)])
        )
        actions.extend(
            [
                f"ADD COLUMN {k} {v.sql_type}{v.get_max_length()}"
                for k, v in to_add.items()
            ]
        )
        to_upd = dict(
            ChainMap(
                *[
                    {k: n_fields[k]}
                    for k in n_keys.intersection(o_keys)
                    if n_fields[k] != o_fields[k]
                ]
            )
        )
        actions.extend(
            [
                f"ALTER COLUMN {k} TYPE {v.sql_type}{v.get_max_length()}"
                for k, v in to_upd.items()
            ]
        )
        self._execute(
            SQL(ALTER_TABLE_SQL).format(SQL(model.__name__), SQL(", ".join(actions)))
        )

    def save(self, model: src.Model):
        table_name = SQL(model.__class__.__name__)
        field_dict = model.get_field_values()
        field_values = list(field_dict.values())
        if model.id:
            assignments = SQL(
                ", ".join("{} = %s".format(field) for field in field_dict.keys())
            )
            field_values.append(model.id)
            self._execute(SQL(UPDATE_SQL).format(table_name, assignments), field_values)
        else:
            field_name_lst = field_dict.keys()
            field_names = SQL(", ".join(field_name_lst))
            field_placeholders = SQL(",".join(["%s"] * len(field_name_lst)))
            ret = self._execute(
                SQL(INSERT_SQL).format(table_name, field_names, field_placeholders),
                field_values,
            )
            model.id = ret[0][0]

    def query(self, model: Type[T]) -> src.Query[T]:
        query = src.Query(model, self)
        return query

    def fetch_results(
        self, model: Type[T], criterion: dict[str, Any], limit: int = 0
    ) -> list[T]:
        _fields = ["id"] + model.get_field_names()
        where_clause = SQL("")
        field_values = []
        limit_clause = SQL("")
        if criterion:
            where_clause = SQL(
                " WHERE "
                + " AND ".join(["{} = %s".format(f) for f in criterion.keys()])
            )
            field_values = [value for value in criterion.values()]
        if limit > 0:
            limit_clause = SQL(f"LIMIT {limit}")
        sel_fields = SQL(", ".join(_fields))
        tbl_name = SQL(model.__name__.lower())
        ret = self._execute(
            SQL(SELECT_SQL).format(sel_fields, tbl_name, where_clause, limit_clause),
            field_values,
        )
        return [model(**dict(zip(_fields, row))) for row in ret]
