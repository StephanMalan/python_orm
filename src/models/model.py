from __future__ import annotations

from typing import Any, Dict, TypeVar

# from src import Field, IntField, InvalidField, InvalidFieldValue, ValueNotInitialized
import src

T = TypeVar("T", bound="BaseModel")


class BaseModel:
    id: int = src.IntField()

    def __init__(self, **kwargs: Any):
        self._data: dict[str, Any] = {"id": None} | kwargs
        self.validate_field_types(kwargs)
        self._validate_fields()

    def __getattribute__(self, name: str) -> Any:
        try:
            _data = object.__getattribute__(self, "_data")
            if name in _data:
                return _data[name]
        except AttributeError:
            return {}

        return object.__getattribute__(self, name)

    def __setattr__(self, k: str, value: Any) -> None:
        if k in self._data:
            self._data[k] = value
        else:
            super().__setattr__(k, value)

    def __str__(self) -> str:
        return str(self._data)

    def __repr__(self) -> str:
        items = [item for item in self._data.items() if item[0] != "id" or item[1]]
        values_str = ", ".join(f"{k}={v!r}" for k, v in sorted(items))
        return f"{self.__class__.__name__}({values_str})"

    def to_dict(self) -> dict[str, Any]:
        return self._data

    @classmethod
    def get_configured_field_defs(cls) -> Dict[str, src.Field]:
        return {k: v for k, v in vars(cls).items() if issubclass(type(v), src.Field)}

    @classmethod
    def get_all_field_defs(cls) -> Dict[str, src.Field]:
        return {k: v for k, v in vars(cls).items() if issubclass(type(v), src.Field)} | {"id": src.IntField()}

    @classmethod
    def get_field_names(cls) -> list[str]:
        return [k for k, v in vars(cls).items() if issubclass(type(v), src.Field)]

    @classmethod
    def validate_field_types(cls, fields: Dict[str, Any]) -> None:
        field_defs = cls.get_all_field_defs()
        for field, value in fields.items():
            if field not in field_defs.keys():
                raise src.InvalidFieldError(field, cls.__name__)
            field_type = field_defs[field]
            if not field_type.validate_value(value):
                raise src.InvalidFieldValueError(field, field_type.native_type.__name__, type(value).__name__)

    def _validate_fields(self) -> None:
        for field in self.get_field_names():
            if field != "id" and field not in self._data:
                raise src.ValueNotInitializedError(field)

    def get_field_values(self) -> dict[str, Any]:
        return {name: self._data[name] for name in self.get_field_names()}
