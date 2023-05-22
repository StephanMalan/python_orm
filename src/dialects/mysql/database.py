from __future__ import annotations

import time
from collections import ChainMap
from typing import Any, Callable, Type

from mysql.connector.cursor import MySQLCursor
from mysql.connector.errors import PoolError
from mysql.connector.pooling import MySQLConnectionPool, PooledMySQLConnection

import src


class MySQLDatabase(src.Database):
    def _init_connection(self, conn_details: src.DatabaseConfig) -> MySQLConnectionPool:
        return MySQLConnectionPool(
            pool_name="mypool",
            pool_size=conn_details.maxconn,
            host=conn_details.host,
            user="root",
            password=conn_details.password,
            database=conn_details.database,
        )

    def _get_connection(self) -> PooledMySQLConnection:
        for _retry in range(10):
            try:
                conn_pool: PooledMySQLConnection = self.conn.get_connection()
                return conn_pool
            except PoolError:
                time.sleep(0.1)
        raise src.NoConnectionError(PoolError)

    def _execute_query(self, sql_query: Any, query_vars: tuple[Any, ...] | None = None) -> list[tuple[Any, ...]]:
        query_vars = query_vars or ()
        with self._get_connection() as conn:
            cur: MySQLCursor = conn.cursor()
            cur.execute(sql_query, query_vars)
            print(cur.statement)
            results: list[tuple[Any, ...]] = cur.fetchall()
            conn.commit()
            cur.close()
            return results

    def _execute_update(
        self, sql_query: Any, query_vars: tuple[Any, ...] | None = None, insert_id: bool = False
    ) -> int:
        query_vars = query_vars or ()
        with self._get_connection() as conn:
            cur: MySQLCursor = conn.cursor()
            cur.execute(sql_query, query_vars)
            print(cur.statement)
            result: int = cur._last_insert_id if insert_id else cur.rowcount  # type: ignore # pylint: disable=W0212
            conn.commit()
            cur.close()
            return result

    def _get_table_schema(self, model: Type[src.BaseModel]) -> dict[str, src.Field]:
        describe_table_sql_template = (
            "SELECT COLUMN_NAME, DATA_TYPE,CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE table_name = %s"
        )
        results = self._execute_query(describe_table_sql_template, (model.__name__.lower(),))
        return dict(ChainMap(*[self._create_field(*col) for col in results]))

    @classmethod
    def _create_field(cls, name: str, f_type: str, max_length: int) -> dict[str, src.Field]:
        field_mapping: dict[str, Callable[..., Any]] = {
            "int": src.IntField,
            "tinyint": src.BoolField,
            "varchar": src.CharField,
        }
        field_func = field_mapping[f_type]
        return {name: field_func(max_length) if max_length else field_func()}

    def _get_create_table_sql(self, model: Type[src.BaseModel]) -> Any:
        _fields = model.get_configured_field_defs()
        sql_fields = [f"{k} {self.get_sql_type(v)}{self.get_field_max_len(v)}" for k, v in _fields.items()]
        return f"CREATE TABLE {model.__name__.lower()} (id INT AUTO_INCREMENT PRIMARY KEY, {', '.join(sql_fields)});"

    def _get_alter_table_sql(self, model: Type[src.BaseModel], schema: dict[str, src.Field]) -> Any:
        orgnl_fields = schema
        orgnl_fields.pop("id")
        new_fields = model.get_configured_field_defs()
        new_set, orgnl_set = set(new_fields.keys()), set(orgnl_fields.keys())
        actions = [f"DROP COLUMN {name}" for name in orgnl_set.difference(new_set)]
        to_add = [(key, new_fields[key]) for key in new_set.difference(orgnl_set)]
        actions.extend([f"ADD COLUMN {k} {self.get_sql_type(v)}{self.get_field_max_len(v)}" for k, v in to_add])
        to_upd = [(k, new_fields[k]) for k in new_set.intersection(orgnl_set) if new_fields[k] != orgnl_fields[k]]
        actions.extend([f"MODIFY COLUMN {k} {self.get_sql_type(v)}{self.get_field_max_len(v)}" for k, v in to_upd])
        return f"ALTER TABLE {model.__name__.lower()} {', '.join(actions)}"

    def _get_insert_table_sql(self, model: src.BaseModel) -> tuple[Any, tuple[Any, ...]]:
        tbl_name = model.__class__.__name__.lower()
        field_dict = model.get_field_values()
        field_vals = list(field_dict.values())
        field_name_lst = field_dict.keys()
        field_names = ", ".join(field_name_lst)
        field_placehldrs = ",".join(["%s"] * len(field_name_lst))
        return f"INSERT INTO {tbl_name} ({field_names}) VALUES ({field_placehldrs});", tuple(field_vals)

    def _get_update_table_sql(self, model: src.BaseModel) -> tuple[Any, tuple[Any, ...]]:
        tbl_name = model.__class__.__name__.lower()
        field_dict = model.get_field_values()
        field_values = list(field_dict.values())
        assignments = ", ".join(f"{field} = %s" for field in field_dict)
        field_values.append(model.id)
        return f"UPDATE {tbl_name} SET {assignments} WHERE id = %s;", tuple(field_values)

    def fetch_results(self, model: Type[src.T], criterion: dict[str, Any], limit: int = 0) -> list[src.T]:
        _flds = model.get_all_field_defs()
        where_clause = " WHERE " + " AND ".join([f"{f} = %s" for f in criterion.keys()]) if criterion else ""
        field_values = [int(v) if _flds[k] == src.BoolField() else v for k, v in criterion.items()] if criterion else []
        limit_clause = f"LIMIT {limit}" if limit else ""
        sel_fields = ", ".join(_flds.keys())
        tbl_name = model.__name__.lower()
        sql = f"SELECT {sel_fields} FROM {tbl_name}{where_clause} ORDER BY id {limit_clause};"
        ret = self._execute_query(sql, tuple(field_values))
        return [
            model(
                **{
                    k: bool(row[idx]) if val == src.BoolField() else row[idx]
                    for idx, (k, val) in enumerate(_flds.items())
                }
            )
            for row in ret
        ]

    def get_field_max_len(self, field: src.Field) -> str:
        if field.native_type == str:
            return f"({field.max_length})" if field.max_length else "(4096)"
        return f"({field.max_length})" if field.max_length else ""

    def get_sql_type(self, field: src.Field) -> str:
        _types = {str: "varchar", int: "int", bool: "tinyint"}
        return _types[field.native_type]

    def _drop_tables(self, **kwargs: Any) -> None:
        sql = (
            "SELECT CONCAT('DROP TABLE IF EXISTS ', table_name, ';')"
            f"FROM information_schema.tables WHERE table_schema = '{kwargs['database']}';"
        )
        drop_tables_sql = self._execute_query(sql)
        _ = [self._execute_update(sql[0]) for sql in drop_tables_sql]
        sql = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{kwargs['database']}';"
        assert self._execute_query(sql) == [(0,)]
