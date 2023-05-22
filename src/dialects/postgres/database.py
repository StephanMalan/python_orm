from __future__ import annotations

import time
from collections import ChainMap
from typing import Any, Callable, Type

from psycopg2._psycopg import connection, cursor
from psycopg2.pool import PoolError, ThreadedConnectionPool
from psycopg2.sql import SQL

import src


class PostgresDatabase(src.Database):
    def _init_connection(self, conn_details: src.DatabaseConfig) -> ThreadedConnectionPool:
        return ThreadedConnectionPool(**conn_details.__dict__)

    def _get_connection(self) -> connection:
        for _retry in range(10):
            try:
                conn: connection = self.conn.getconn()
                conn.autocommit = True
                return conn
            except PoolError:
                time.sleep(0.1)
        raise src.NoConnectionError(PoolError())

    def _execute_query(self, sql_query: Any, query_vars: tuple[Any, ...] | None = None) -> list[tuple[Any, ...]]:
        query_vars = query_vars or ()
        conn = self._get_connection()
        try:
            cur: cursor = conn.cursor()
            cur.execute(sql_query, query_vars)
            results: list[tuple[Any, ...]] = cur.fetchall()
            return results
        finally:
            self.conn.putconn(conn)

    def _execute_update(
        self, sql_query: Any, query_vars: tuple[Any, ...] | None = None, insert_id: bool = False
    ) -> int:
        query_vars = query_vars or ()
        conn = self._get_connection()
        try:
            cur: cursor = conn.cursor()
            cur.execute(sql_query, query_vars)
            result: int = cur.fetchone()[0] if insert_id else cur.rowcount  # type: ignore
            return result
        finally:
            self.conn.putconn(conn)

    def _get_table_schema(self, model: Type[src.BaseModel]) -> dict[str, src.Field]:
        describe_table_sql_template = (
            "SELECT column_name, data_type, character_maximum_length "
            "FROM information_schema.columns WHERE table_name = %s;"
        )
        results = self._execute_query(describe_table_sql_template, (model.__name__.lower(),))
        return dict(ChainMap(*[self._create_field(*col) for col in results]))

    def _get_create_table_sql(self, model: Type[src.BaseModel]) -> Any:
        create_table_sql_template = "CREATE TABLE {} (id SERIAL PRIMARY KEY, {});"
        _fields = model.get_configured_field_defs()
        sql_fields = [f"{k} {self.get_sql_type(v)}{v.get_max_length()}" for k, v in _fields.items()]
        query = SQL(create_table_sql_template).format(SQL(model.__name__), SQL(", ".join(sql_fields)))
        return query

    def _get_alter_table_sql(self, model: Type[src.BaseModel], schema: dict[str, src.Field]) -> Any:
        alter_table_sql_template = "ALTER TABLE {} {}"
        orgnl_fields = schema
        orgnl_fields.pop("id")
        new_fields = model.get_configured_field_defs()
        new_set, orgnl_set = set(new_fields.keys()), set(orgnl_fields.keys())
        actions = [f"DROP COLUMN {name}" for name in orgnl_set.difference(new_set)]
        to_add = [(key, new_fields[key]) for key in new_set.difference(orgnl_set)]
        actions.extend([f"ADD COLUMN {k} {self.get_sql_type(v)}{v.get_max_length()}" for k, v in to_add])
        to_upd = [(k, new_fields[k]) for k in new_set.intersection(orgnl_set) if new_fields[k] != orgnl_fields[k]]
        actions.extend([f"ALTER COLUMN {k} TYPE {self.get_sql_type(v)}{v.get_max_length()}" for k, v in to_upd])
        query = SQL(alter_table_sql_template).format(SQL(model.__name__), SQL(", ".join(actions)))
        return query

    @classmethod
    def _create_field(cls, name: str, f_type: str, max_length: int) -> dict[str, src.Field]:
        field_mapping: dict[str, Callable[..., Any]] = {
            "integer": src.IntField,
            "boolean": src.BoolField,
            "character varying": src.CharField,
        }
        field_func = field_mapping[f_type]
        return {name: field_func(max_length) if max_length else field_func()}

    def _get_insert_table_sql(self, model: src.BaseModel) -> tuple[Any, tuple[Any, ...]]:
        insert_sql_template = "INSERT INTO {} ({}) VALUES ({}) RETURNING id;"
        table_name = SQL(model.__class__.__name__)
        field_dict = model.get_field_values()
        field_values = list(field_dict.values())
        field_name_lst = field_dict.keys()
        field_names = SQL(", ".join(field_name_lst))
        field_placeholders = SQL(",".join(["%s"] * len(field_name_lst)))
        query = SQL(insert_sql_template).format(table_name, field_names, field_placeholders)
        return query, tuple(field_values)

    def _get_update_table_sql(self, model: src.BaseModel) -> tuple[Any, tuple[Any, ...]]:
        update_sql_template = "UPDATE {} SET {} WHERE id = %s;"
        table_name = SQL(model.__class__.__name__)
        field_dict = model.get_field_values()
        field_values = list(field_dict.values())
        assignments = SQL(", ".join(f"{field} = %s" for field in field_dict))
        field_values.append(model.id)
        query = SQL(update_sql_template).format(table_name, assignments)
        return query, tuple(field_values)

    def fetch_results(self, model: Type[src.T], criterion: dict[str, Any], limit: int = 0) -> list[src.T]:
        select_sql_template = "SELECT {} FROM {}{} ORDER BY id {};"
        _fields = ["id"] + model.get_field_names()
        where_clause = SQL(" WHERE " + " AND ".join([f"{f} = %s" for f in criterion.keys()])) if criterion else SQL("")
        field_values = list(criterion.values()) if criterion else []
        limit_clause = SQL(f"LIMIT {limit}") if limit else SQL("")
        sel_fields = SQL(", ".join(_fields))
        tbl_name = SQL(model.__name__.lower())
        ret = self._execute_query(
            SQL(select_sql_template).format(sel_fields, tbl_name, where_clause, limit_clause),
            tuple(field_values),
        )
        return [model(**dict(zip(_fields, row))) for row in ret]

    def get_sql_type(self, field: src.Field) -> str:
        _types = {str: "varchar", int: "integer", bool: "boolean"}
        return _types[field.native_type]

    def _drop_tables(self, **kwargs: Any) -> None:
        sql = "SELECT 'DROP TABLE IF EXISTS ' || tablename || ';' FROM pg_tables WHERE schemaname = 'public';"
        drop_tables_sql = self._execute_query(sql)
        _ = [self._execute_update(sql[0]) for sql in drop_tables_sql]
        assert self._execute_query("SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public';") == [(0,)]
