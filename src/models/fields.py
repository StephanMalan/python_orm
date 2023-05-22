from typing import Any, Type

SupportedTypes = Type[str | int | bool]


class Field:
    def __init__(self, native_type: SupportedTypes, max_length: int = 0):
        self.native_type = native_type
        self.max_length = max_length

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Field):
            return False
        return self.native_type == other.native_type and self.max_length == other.max_length

    def get_max_length(self) -> str:
        return f"({self.max_length})" if self.max_length else ""

    def validate_value(self, value: Any) -> bool:
        return value is None or isinstance(value, self.native_type)


def CharField(max_length: int = 0) -> Any:  # pylint: disable=invalid-name
    return Field(str, max_length)


def IntField() -> Any:  # pylint: disable=invalid-name
    return Field(int)


def BoolField() -> Any:  # pylint: disable=invalid-name
    return Field(bool)
