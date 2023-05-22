# pylint: disable=W0212
from test.dialects import MYSQL_CONFIG, POSTGRESS_CONFIG, SQLITE_CONFIG

import pytest

import src


class TestBasic:
    databases = [POSTGRESS_CONFIG, MYSQL_CONFIG, SQLITE_CONFIG]

    def test_given_example(self, db: src.Database) -> None:
        class Author(src.BaseModel):
            name: str = src.CharField(max_length=32)
            email: str = src.CharField(max_length=128)

        # Creates a table called author in mydb
        db.create_table(Author)

        author = Author(name="John", email="john@abc.com")
        db.save(author)

        del author

        # Select author data has email of "john@abc.com"
        authors = db.query(Author)
        authors = authors.filter(email="john@abc.com")
        author = authors[0]
        assert author.to_dict() == {"email": "john@abc.com", "id": 1, "name": "John"}

        author.name = "John Doe"
        assert author.to_dict() == {"id": 1, "name": "John Doe", "email": "john@abc.com"}
        db.save(author)
        author_id = author.id

        del author

        author = db.query(Author).filter(id=author_id)[0]
        assert author.name == "John Doe"

    def test_multiple_field_types(self, db: src.Database) -> None:
        class Car(src.BaseModel):
            name: str = src.CharField(max_length=32)
            mileage: int = src.IntField()
            drivable: bool = src.BoolField()

        db.create_table(Car)

        # Create model using all three field types
        car = Car(name="Mercedes", mileage=12500, drivable=True)
        db.save(car)

        cars = db.query(Car)
        assert len(cars) == 1
        assert cars[0].to_dict() == {"id": 1, "name": "Mercedes", "mileage": 12500, "drivable": True}
        assert car.id == 1

        car.mileage = 12700
        db.save(car)

        cars = db.query(Car)
        assert len(cars) == 1
        assert cars[0].to_dict() == {"id": 1, "name": "Mercedes", "mileage": 12700, "drivable": True}

    def test_null_value(self, db: src.Database) -> None:
        class Book(src.BaseModel):
            name: str = src.CharField(max_length=32)

        db.create_table(Book)

        book = Book(name=None)
        db.save(book)

        books = db.query(Book)
        assert len(books) == 1
        assert books[0].to_dict() == {"id": 1, "name": None}

    def test_invalid_contains(self, db: src.Database) -> None:
        class Book(src.BaseModel):
            name: str = src.CharField(max_length=32)

        db.create_table(Book)

        db.save(Book(name="John"))
        query = db.query(Book)
        assert "invalid" not in query

    def test_filter_by_invalid_field_name(self, db: src.Database) -> None:
        with pytest.raises(src.InvalidFieldError):

            class Book(src.BaseModel):
                name: str = src.CharField(max_length=32)

            # Filter by field that is not defined
            db.query(Book).filter(page=3).first()

    def test_filter_by_invalid_field_value(self, db: src.Database) -> None:
        with pytest.raises(src.InvalidFieldValueError):

            class Person(src.BaseModel):
                name: str = src.CharField(max_length=32)

            # Filter field by the wrong value type
            db.query(Person).filter(name=3).first()

    def test_create_table_again(self, db: src.Database) -> None:
        class Book(src.BaseModel):
            name = src.CharField(max_length=128)
            pages = src.IntField()
            available = src.BoolField()

        db.create_table(Book)
        assert db._get_table_schema(Book) == {
            "id": src.IntField(),
            "name": src.CharField(max_length=128),
            "pages": src.IntField(),
            "available": src.BoolField(),
        }

        db.create_table(Book)
        assert db._get_table_schema(Book) == {
            "id": src.IntField(),
            "name": src.CharField(max_length=128),
            "pages": src.IntField(),
            "available": src.BoolField(),
        }

    def test_filter_already_filtered_data(self, db: src.Database) -> None:
        class Book(src.BaseModel):
            name = src.CharField()
            author = src.CharField()
            available = src.BoolField()

        db.create_table(Book)

        db.save(Book(name="Fluent Python", author="Luciano Ramalho", available=True))
        db.save(Book(name="1984", author="George Orwell", available=True))
        db.save(Book(name="Animal Farm", author="George Orwell", available=True))
        db.save(Book(name="Homage to Catalonia", author="George Orwell", available=False))

        # Can filter on previously filtered query before evaluation
        books = db.query(Book).filter(author="George Orwell")
        books = books.filter(available=True)

        assert len(books) == 2
        assert str(books[0]) == "{'id': 2, 'name': '1984', 'author': 'George Orwell', 'available': True}"
        assert str(books[1]) == "{'id': 3, 'name': 'Animal Farm', 'author': 'George Orwell', 'available': True}"

        # Can filter on previously filtered query after evaluation
        books.filter(name="Animal Farm")
        assert len(books) == 1
        assert str(books[0]) == "{'id': 3, 'name': 'Animal Farm', 'author': 'George Orwell', 'available': True}"
