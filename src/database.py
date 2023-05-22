from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Type

import src


@dataclass
class DatabaseConfig:
    host: str
    user: str
    password: str
    database: str
    minconn: int = 1
    maxconn: int = 1


class Database(ABC):
    def __init__(self, conn_details: DatabaseConfig) -> None:
        self.conn_details = conn_details
        self.conn = self._init_connection(conn_details)

    @abstractmethod
    def _init_connection(self, conn_details: DatabaseConfig) -> Any:
        """Initialize database connection(s)"""

    @abstractmethod
    def _execute_query(self, sql_query: Any, query_vars: tuple[Any, ...] | None = None) -> list[tuple[Any, ...]]:
        """Execute SQL on the database and return the resulting rows"""

    @abstractmethod
    def _execute_update(
        self, sql_query: Any, query_vars: tuple[Any, ...] | None = None, insert_id: bool = False
    ) -> int:
        """Execute SQL on the database and return either the id of from the last insert or the row count"""

    def create_table(self, model: Type[src.BaseModel]) -> None:
        """Create table from a model. If table exists and is differs from model, the table is altered"""
        table_schema = self._get_table_schema(model)
        if not table_schema:
            create_table_sql = self._get_create_table_sql(model)
            self._execute_update(create_table_sql)
            return

        if table_schema == model.get_all_field_defs():
            return

        alter_table_sql = self._get_alter_table_sql(model, table_schema)
        self._execute_update(alter_table_sql)

    @abstractmethod
    def _get_table_schema(self, model: Type[src.BaseModel]) -> dict[str, src.Field]:
        """Returns the table schema"""

    @abstractmethod
    def _get_create_table_sql(self, model: Type[src.BaseModel]) -> Any:
        """Returns the SQL required to create a new table in the database"""

    @abstractmethod
    def _get_alter_table_sql(self, model: Type[src.BaseModel], schema: dict[str, src.Field]) -> Any:
        """Returns the SQL required to alter an existing table in the database"""

    def save(self, model: src.BaseModel) -> None:
        """Save model data to database. If the model is new, the value is added; otherwise it's updated"""
        if model.id:
            update_sql, query_vars = self._get_update_table_sql(model)
            self._execute_update(update_sql, query_vars)
        else:
            insert_sql, query_vars = self._get_insert_table_sql(model)
            result = self._execute_update(insert_sql, query_vars, insert_id=True)
            model.id = result

    @abstractmethod
    def _get_update_table_sql(self, model: src.BaseModel) -> tuple[Any, tuple[Any, ...]]:
        """Returns the SQL required to update an existing model's row in a table in the database"""

    @abstractmethod
    def _get_insert_table_sql(self, model: src.BaseModel) -> tuple[Any, tuple[Any, ...]]:
        """Returns the SQL required to insert a model's data into a table in the database"""

    def query(self, model: Type[src.T]) -> src.Query[src.T]:
        query = src.Query(model, self)
        return query

    @abstractmethod
    def fetch_results(self, model: Type[src.T], criterion: dict[str, Any], limit: int = 0) -> list[src.T]:
        """Retrieve data from database. Can be filtered and limited"""

    @abstractmethod
    def _drop_tables(self, **kwargs: Any) -> None:
        """Test fuction that is used to clean up database"""
