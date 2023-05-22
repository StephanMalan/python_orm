# pylint: disable=W0212
from test.dialects import MYSQL_CONFIG, POSTGRESS_CONFIG, SQLITE_CONFIG

import pytest

import src


class TestTableModifiable:
    databases = [POSTGRESS_CONFIG, MYSQL_CONFIG]

    def test_table_migration(self, db: src.Database) -> None:
        class Book(src.BaseModel):
            name: str = src.CharField(max_length=32)
            pages: int = src.IntField()
            available: bool = src.BoolField()

        db.create_table(Book)

        book = Book(name="Atomic Habits", pages=324, available=True)
        db.save(book)

        # Following statements causes the table to drop, add, and modify a column on the table
        delattr(Book, "available")
        Book.name = src.CharField(max_length=64)
        Book.author = src.CharField(max_length=128)  # type: ignore

        db.create_table(Book)

        assert db._get_table_schema(Book) == {
            "id": src.IntField(),
            "name": src.CharField(max_length=64),
            "pages": src.IntField(),
            "author": src.CharField(max_length=128),
        }


class TestTableNotModifiable:
    databases = [SQLITE_CONFIG]

    def test_table_migration(self, db: src.Database) -> None:
        class Book(src.BaseModel):
            name: str = src.CharField(max_length=32)
            pages: int = src.IntField()
            available: bool = src.BoolField()

        db.create_table(Book)

        book = Book(name="Atomic Habits", pages=324, available=True)
        db.save(book)

        # Following statements causes the table to drop, add, and modify a column on the table
        delattr(Book, "available")
        Book.name = src.CharField(max_length=64)
        Book.author = src.CharField(max_length=128)  # type: ignore

        with pytest.raises(src.FeatureNotImplementedError):
            db.create_table(Book)
