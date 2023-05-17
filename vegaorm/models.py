from functools import reduce
from typing import Any, Type, Dict


class InvalidField(Exception):

    def __init__(self, field: str, model: str):
        message = f"Field '{field}' not defined in {model} model"
        super().__init__(message)


class InvalidFieldValue(Exception):

    def __init__(self, field: str, f_type: str, exp_f_type: str):
        message = f"Field '{field}' expected a value of type {f_type}, but found value of type {exp_f_type} instead"
        super().__init__(message)


class ValueNotInitialized(Exception):

    def __init__(self, field: str):
        super().__init__(f"Field '{field}' was not initialized")


class Field:

    def __init__(self, native_type: Type, sql_type: str, max_length: int = -1):
        self.native_type = native_type
        self.sql_type = sql_type
        self.max_length = max_length

    def __eq__(self, other):
        return self.sql_type == other.sql_type and self.max_length == other.max_length

    def get_max_length(self) -> str:
        return '' if self.max_length < 0 else f'({str(self.max_length)})'

    def validate_value(self, value: any) -> bool:
        return type(value) == self.native_type or value is None


class CharField(Field):

    def __init__(self, max_length: int = -1):
        super().__init__(str, 'VARCHAR')
        self.max_length = max_length


class IntField(Field):

    def __init__(self):
        super().__init__(int, 'INTEGER')


class BoolField(Field):

    def __init__(self):
        super().__init__(bool, 'BOOLEAN')


class FieldFactory:
    _mapping = {'integer': IntField, 'boolean': BoolField, 'character varying': CharField}

    @classmethod
    def create_field(cls, name: str, f_type: str, max_length: int) -> Dict[str, Field]:
        f_class = cls._mapping[f_type]
        return {name: f_class(max_length) if max_length else f_class()}


class Model:
    _data = {}
    id: int

    def __init__(self, **kwargs):
        self._validate_fields(kwargs)
        self._data: Dict[str, any] = {'id': None}
        self._data = self._data | kwargs
        for field in self.get_field_names():
            if field not in self._data:
                raise ValueNotInitialized(field)

    def __getattribute__(self, name: str) -> Any:
        _data = object.__getattribute__(self, '_data')
        return _data[name] if name in _data else super().__getattribute__(name)

    def __setattr__(self, name: str, value: Any) -> None:
        if name in self._data.keys():
            self._data[name] = value
        else:
            super().__setattr__(name, value)

    def __str__(self) -> str:
        return str(self._data)

    @classmethod
    def get_field_definitions(cls) -> Dict[str, Field]:
        return {name: value for name, value in vars(cls).items() if issubclass(type(value), Field)}

    @classmethod
    def get_field_names(cls) -> list[str]:
        return [name for name, value in vars(cls).items() if issubclass(type(value), Field)]

    @classmethod
    def _validate_fields(cls, fields: Dict[str, any]):
        field_defs = cls.get_field_definitions()
        field_defs['id'] = IntField()
        for field, value in fields.items():
            if field not in field_defs.keys():
                raise InvalidField(field, cls.__name__)
            field_type = field_defs[field]
            if not field_type.validate_value(value):
                raise InvalidFieldValue(field, field_type.native_type.__name__, type(value).__name__)

    def get_field_values(self) -> Dict[str, any]:
        return {name: self._data[name] for name in self.get_field_names()}


class Query:

    def __init__(self, model: Type[Model], db):
        self.model = model
        self.db = db
        self._result_cache: [Model] = []
        self._criteria: Dict[str, any] = {}

    def __len__(self):
        if not self._result_cache:
            self._result_cache = self.db.fetch_results(self.model, self._criteria)
        return len(self._result_cache)

    def __iter__(self):
        if not self._result_cache:
            self._result_cache = self.db.fetch_results(self.model, self._criteria)
        return self._result_cache.__iter__()

    def __contains__(self, val):
        if not self._result_cache:
            self._result_cache = self.db.fetch_results(self.model, self._criteria)
        return type(val) == self.model and filter(lambda m: m.id == val.id, self._result_cache)

    def __getitem__(self, k):
        if not self._result_cache:
            self._result_cache = self.db.fetch_results(self.model, self._criteria)
        return self._result_cache[k]

    def filter(self, **criteria):
        # noinspection PyProtectedMember
        self.model._validate_fields(criteria)
        if self._result_cache:
            def val(m):
                return reduce(lambda t, f: t or m.__getattribute__(f) == criteria[f], criteria.keys(), False)

            self._result_cache = list(filter(val, self._result_cache))
        else:
            self._criteria.update(criteria)
        return self

    def first(self) -> Model:
        if not self._result_cache:
            self._result_cache = self.db.fetch_results(self.model, self._criteria, limit=1)
        return self._result_cache[0]

    def all(self) -> [Model]:
        if not self._result_cache:
            self._result_cache = self.db.fetch_results(self.model, self._criteria)
        return self._result_cache
