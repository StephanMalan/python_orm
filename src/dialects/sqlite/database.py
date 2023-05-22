from __future__ import annotations

import re
import sqlite3
from collections import ChainMap
from sqlite3 import Connection, Cursor
from typing import Any, Callable, Type

import src


class SQLiteDatabase(src.Database):
    def _init_connection(self, conn_details: src.DatabaseConfig) -> Any:
        return None

    def _get_connection(self) -> Connection:
        return sqlite3.connect(self.conn_details.host)

    def _execute_query(self, sql_query: Any, query_vars: tuple[Any, ...] | None = None) -> list[tuple[Any, ...]]:
        query_vars = query_vars or ()
        with self._get_connection() as conn:
            print(sql_query)
            cur: Cursor = conn.execute(sql_query, query_vars)
            results: list[Any] = cur.fetchall()
            return results

    def _execute_update(
        self, sql_query: Any, query_vars: tuple[Any, ...] | None = None, insert_id: bool = False
    ) -> int:
        query_vars = query_vars or ()
        with self._get_connection() as conn:
            print(sql_query)
            cur: Cursor = conn.execute(sql_query, query_vars)
            result: int = cur.lastrowid if insert_id else cur.rowcount  # type: ignore
            return result

    def _get_table_schema(self, model: Type[src.BaseModel]) -> dict[str, src.Field]:
        describe_table_sql_template = "SELECT name, type as tpe FROM pragma_table_info(?)"
        results = self._execute_query(describe_table_sql_template, (model.__name__.lower(),))
        return dict(ChainMap(*[self._create_field(*col) for col in results]))

    def _create_field(self, name: str, tpe: str) -> dict[str, src.Field]:
        field_mapping: dict[str, Callable[..., Any]] = {
            "INTEGER": src.IntField,
            "BOOLEAN": src.BoolField,
            "VARCHAR": src.CharField,
        }
        f_type, max_length = self._get_field_length(tpe)
        field_func = field_mapping[f_type]
        return {name: field_func(max_length) if max_length else field_func()}

    def _get_field_length(self, tpe: str) -> tuple[str, int]:
        result = re.search(r"^(VARCHAR|BOOLEAN|INTEGER)(\(([0-9]+)\))?", tpe)
        return result.group(1), int(result.group(3)) if result.group(3) else 0  # type: ignore

    def _get_create_table_sql(self, model: Type[src.BaseModel]) -> Any:
        _fields = model.get_configured_field_defs()
        sql_fields = [f"{k} {self.get_sql_type(v)}{v.get_max_length()}" for k, v in _fields.items()]
        tbl_name = model.__name__.lower()
        return f"CREATE TABLE {tbl_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, {', '.join(sql_fields)});"

    def _get_alter_table_sql(self, model: Type[src.BaseModel], schema: dict[str, src.Field]) -> Any:
        raise src.FeatureNotImplementedError("Modify table")

    def _get_insert_table_sql(self, model: src.BaseModel) -> tuple[Any, tuple[Any, ...]]:
        table_name = model.__class__.__name__.lower()
        field_dict = model.get_field_values()
        field_values = list(field_dict.values())
        field_name_lst = field_dict.keys()
        field_names = ", ".join(field_name_lst)
        field_placeholders = ",".join(["?"] * len(field_name_lst))
        return f"INSERT INTO {table_name} ({field_names}) VALUES ({field_placeholders});", tuple(field_values)

    def _get_update_table_sql(self, model: src.BaseModel) -> tuple[Any, tuple[Any, ...]]:
        table_name = model.__class__.__name__.lower()
        field_dict = model.get_field_values()
        field_values = list(field_dict.values())
        assignments = ", ".join(f"{field} = ?" for field in field_dict)
        field_values.append(model.id)
        return f"UPDATE {table_name} SET {assignments} WHERE id = ?;", tuple(field_values)

    def fetch_results(self, model: Type[src.T], criterion: dict[str, Any], limit: int = 0) -> list[src.T]:
        _fields = model.get_all_field_defs()
        where_clause = " WHERE " + " AND ".join([f"{f} = ?" for f in criterion.keys()]) if criterion else ""
        field_values = list(criterion.values()) if criterion else []
        limit_clause = f"LIMIT {limit}" if limit else ""
        sel_fields = ", ".join(_fields.keys())
        tbl_name = model.__name__.lower()
        sql = f"SELECT {sel_fields} FROM {tbl_name}{where_clause} ORDER BY id {limit_clause};"
        ret = self._execute_query(sql, tuple(field_values))
        return [
            model(
                **{
                    k: bool(row[idx]) if val == src.BoolField() else row[idx]
                    for idx, (k, val) in enumerate(_fields.items())
                }
            )
            for row in ret
        ]
        # return [model(**dict(zip(_fields, row))) for row in ret]

    def get_sql_type(self, field: src.Field) -> str:
        _types = {str: "VARCHAR", int: "INTEGER", bool: "BOOLEAN"}
        return _types[field.native_type]

    def _drop_tables(self, **kwargs: Any) -> None:
        sql = (
            "SELECT 'DROP TABLE ' || name || ';' from sqlite_master WHERE type = 'table' and name != 'sqlite_sequence';"
        )
        drop_tables_sql = self._execute_query(sql)
        _ = [self._execute_update(sql[0]) for sql in drop_tables_sql]
        assert self._execute_query(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' and name != 'sqlite_sequence';"
        ) == [(0,)]
