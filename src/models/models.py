import traceback
from functools import reduce
from typing import Any, Dict, Type, TypeVar

from src.exceptions import InvalidField, InvalidFieldValue, ValueNotInitialized

SUPPORTED_TYPES = Type[str | int | bool]


class Field:
    def __init__(self, native_type: SUPPORTED_TYPES, sql_type: str, max_length: int = 0):
        self.native_type = native_type
        self.sql_type = sql_type
        self.max_length = max_length

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Field):
            return False
        return self.sql_type == other.sql_type and self.max_length == other.max_length

    def get_max_length(self) -> str:
        return f"({self.max_length})" if self.max_length else ""

    def validate_value(self, value: Any) -> bool:
        return value is None or isinstance(value, self.native_type)


class CharField(Field):
    def __init__(self, max_length: int = 0) -> None:
        super().__init__(str, "VARCHAR")
        self.max_length = max_length


class IntField(Field):
    def __init__(self) -> None:
        super().__init__(int, "INTEGER")


class BoolField(Field):
    def __init__(self) -> None:
        super().__init__(bool, "BOOLEAN")


class FieldFactory:
    _mapping = {
        "integer": IntField,
        "boolean": BoolField,
        "character varying": CharField,
    }

    @classmethod
    def create_field(cls, name: str, f_type: str, max_length: int) -> Dict[str, Field]:
        f_class = cls._mapping[f_type]
        return {name: f_class(max_length) if max_length else f_class()}


class Model:
    id: int = IntField()

    def __init__(self, **kwargs: Any):
        self._data: dict[str, Any] = {"id": None} | kwargs
        self.validate_field_types(kwargs)
        self._validate_fields()

    def __getattribute__(self, name: str) -> Any:
        print("Looking up", name)
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
        values_str = ", ".join(f"{k}={v!r}" for k, v in sorted(self._data.items()))
        return f"{self.__class__.__name__}({values_str})"

    @classmethod
    def get_field_definitions(cls) -> Dict[str, Field]:
        return {k: v for k, v in vars(cls).items() if issubclass(type(v), Field)}

    @classmethod
    def get_field_names(cls) -> list[str]:
        return [k for k, v in vars(cls).items() if issubclass(type(v), Field)]

    @classmethod
    def validate_field_types(cls, fields: Dict[str, Any]) -> None:
        field_defs: dict[str, Field] = cls.get_field_definitions() | {"id": IntField()}
        print(field_defs)
        for field, value in fields.items():
            if field not in field_defs.keys():
                raise InvalidField(field, cls.__name__)
            field_type = field_defs[field]
            if not field_type.validate_value(value):
                raise InvalidFieldValue(field, field_type.native_type.__name__, type(value).__name__)

    def _validate_fields(self) -> None:
        print(self.get_field_names(), self._data)
        for field in self.get_field_names():
            if field != "id" and field not in self._data:
                raise ValueNotInitialized(field)

    def get_field_values(self) -> Dict[str, Any]:
        return {name: self._data[name] for name in self.get_field_names()}
