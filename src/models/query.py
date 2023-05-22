from __future__ import annotations

from typing import Any, Generic, Iterator, Type

from src import Database
from src.models.model import T


class Query(Generic[T]):
    def __init__(self, model: Type[T], db: Database):
        self.model = model
        self.db = db
        self._result_cache: list[T] = []
        self._criteria: dict[str, Any] = {}

    def __len__(self) -> int:
        if not self._result_cache:
            self._result_cache = self.db.fetch_results(self.model, self._criteria)
        return len(self._result_cache)

    def __iter__(self) -> Iterator[T]:
        if not self._result_cache:
            self._result_cache = self.db.fetch_results(self.model, self._criteria)
        return self._result_cache.__iter__()

    def __contains__(self, val: object) -> bool:
        if not isinstance(val, self.model):
            return False
        if not self._result_cache:
            self._result_cache = self.db.fetch_results(self.model, self._criteria)
        return any(m.id == val.id for m in self._result_cache)

    def __getitem__(self, k: int) -> T:
        if not self._result_cache:
            self._result_cache = self.db.fetch_results(self.model, self._criteria)
        return self._result_cache[k]

    def filter(self, **criteria: Any) -> "Query[T]":
        self.model.validate_field_types(criteria)
        if self._result_cache:
            self._result_cache = [
                result for result in self._result_cache if all(getattr(result, k) == v for k, v in criteria.items())
            ]
        else:
            self._criteria.update(criteria)
        return self

    def first(self) -> T:
        if not self._result_cache:
            self._result_cache = self.db.fetch_results(self.model, self._criteria, limit=1)
        return self._result_cache[0]

    def all(self) -> list[T]:
        if not self._result_cache:
            self._result_cache = self.db.fetch_results(self.model, self._criteria)
        return self._result_cache
