# pylint: disable=W0212
from test.dialects import MYSQL_CONFIG, POSTGRESS_CONFIG

import src


class TestLazyEval:
    databases = [MYSQL_CONFIG, POSTGRESS_CONFIG]

    def test_lazy_evaluation_method_first(self, db: src.Database) -> None:
        class Book(src.BaseModel):
            name: str = src.CharField(max_length=32)

        db.create_table(Book)

        books = db.query(Book).filter(name="1984")

        book = Book(name="1984")
        db.save(book)

        # Lazy evaluates since the latest saved value is returned
        assert str(books.first()) == "{'id': 1, 'name': '1984'}"

    def test_lazy_evaluation_method_all(self, db: src.Database) -> None:
        class Book(src.BaseModel):
            name: str = src.CharField(max_length=32)

        db.create_table(Book)

        books = db.query(Book).filter(name="1984")

        book = Book(name="1984")
        db.save(book)

        # Lazy evaluates since the latest saved value is returned
        lst = books.all()
        assert isinstance(lst, list)
        assert str(lst[0]) == "{'id': 1, 'name': '1984'}"

    def test_lazy_evaluation_method_getitem(self, db: src.Database) -> None:
        class Book(src.BaseModel):
            name: str = src.CharField(max_length=32)

        db.create_table(Book)

        books = db.query(Book).filter(name="1984")
        book = Book(name="1984")
        db.save(book)

        # Lazy evaluates since the latest saved value is returned
        assert str(books[0]) == "{'id': 1, 'name': '1984'}"

    def test_lazy_evaluation_method_iter(self, db: src.Database) -> None:
        class Book(src.BaseModel):
            name: str = src.CharField(max_length=32)

        db.create_table(Book)

        books = db.query(Book).filter(name="1984")

        book = Book(name="1984")
        db.save(book)

        # Lazy evaluates since the latest saved value is returned
        books_iter = iter(books)
        # assert 1 == 2
        assert str(next(books_iter)) == "{'id': 1, 'name': '1984'}"

    def test_lazy_evaluation_method_contains(self, db: src.Database) -> None:
        class Book(src.BaseModel):
            name: str = src.CharField(max_length=32)

        db.create_table(Book)

        books = db.query(Book).filter(name="1984")

        book = Book(name="1984")
        db.save(book)

        # Lazy evaluates since the latest saved value is returned
        assert book in books
