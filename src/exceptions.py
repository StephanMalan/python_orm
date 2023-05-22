from typing import Any


class InvalidFieldError(Exception):
    def __init__(self, field: str, model: str):
        message = f"Field '{field}' not defined in {model} model"
        super().__init__(message)


class InvalidFieldValueError(Exception):
    def __init__(self, field: str, f_type: str, exp_f_type: str):
        message = f"Field '{field}' expected a value of type {f_type}, but found value of type {exp_f_type} instead"
        super().__init__(message)


class ValueNotInitializedError(Exception):
    def __init__(self, field: str):
        super().__init__(f"Field '{field}' was not initialized")


class NoConnectionError(Exception):
    def __init__(self, exception: Any):
        super().__init__(exception)


class FeatureNotImplementedError(Exception):
    def __init__(self, feature: str):
        super().__init__(f"Feature '{feature}' not implemented.")
