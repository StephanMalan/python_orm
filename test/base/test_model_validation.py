import pytest

import src


def test_model_repr() -> None:
    class Book(src.BaseModel):
        name: str = src.CharField(max_length=32)

    book = Book(name="John")
    assert repr(book) == "Book(name='John')"


def test_model_str() -> None:
    class Book(src.BaseModel):
        name: str = src.CharField(max_length=32)

    book = Book(name="John")
    assert str(book) == "{'id': None, 'name': 'John'}"


def test_init_with_invalid_field_name() -> None:
    with pytest.raises(src.InvalidFieldError):

        class Book(src.BaseModel):
            name: str = src.CharField(max_length=32)

        # Initialize model with a field that does not exist
        Book(author="George Orwell")


def test_init_with_invalid_field_value() -> None:
    with pytest.raises(src.InvalidFieldValueError):

        class Person(src.BaseModel):
            name: str = src.CharField(max_length=32)

        # Initialize model with a field with the wrong value type
        Person(name=12)


def test_model_with_invalid_fields() -> None:
    with pytest.raises(src.InvalidFieldError):

        class Book(src.BaseModel):
            name: str = "Test"

        Book(name="John")


def test_value_not_initialized() -> None:
    class Book(src.BaseModel):
        name: str = src.CharField(max_length=32)

    # When all the defined fields are not provided, an error is thrown
    with pytest.raises(src.ValueNotInitializedError):
        Book()


def test_compare_fields() -> None:
    field = src.Field(str)
    assert field == src.Field(str)
    assert field != src.Field(int)
    assert field != src.Field(str, max_length=32)
    assert field != "invalid"
