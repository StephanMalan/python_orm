from threading import Thread

import psycopg2
import pytest
from psycopg2._psycopg import connection
from psycopg2.pool import PoolError

import vegaorm
from vegaorm import DatabaseConfig, Database, InvalidField, InvalidFieldValue, ValueNotInitialized

conn: connection = None
HOST = 'postgres'
DEFAULT_CONN_DETAILS = DatabaseConfig(host=HOST, user='postgres', password='password', database='mydb')


def test_given_example():
    # Create a database called mydb
    conn_details = DatabaseConfig(host=HOST, user='postgres', password='password', database='mydb')
    db = Database(conn_details)

    class Author(vegaorm.Model):
        name = vegaorm.CharField(max_length=32)
        email = vegaorm.CharField(max_length=128)

    # Creates a table called author in mydb
    db.create_table(Author)

    author = Author(name='John', email="john@abc.com")
    db.save(author)

    del author

    # Select author data has email of "john@abc.com"
    authors = db.query(Author)
    authors = authors.filter(email="john@abc.com")
    author = authors[0]
    assert str(author) == '''{'id': 1, 'name': 'John', 'email': 'john@abc.com'}'''

    author.name = 'John Doe'
    assert str(author) == '''{'id': 1, 'name': 'John Doe', 'email': 'john@abc.com'}'''
    db.save(author)
    author_id = author.id

    del author

    author = db.query(Author).filter(id=author_id)[0]
    assert author.name == 'John Doe'


def test_database_already_exists():
    # Create mydb database
    vegaorm.Database(DEFAULT_CONN_DETAILS)
    # Try to create mydb database again and fails silently
    vegaorm.Database(DEFAULT_CONN_DETAILS)


def test_filter_by_invalid_field_name():
    with pytest.raises(InvalidField):
        db = vegaorm.Database(DEFAULT_CONN_DETAILS)

        class Book(vegaorm.Model):
            name = vegaorm.CharField(max_length=32)

        # Filter by field that is not defined
        db.query(Book).filter(page=3).first()


def test_filter_by_invalid_field_value():
    with pytest.raises(InvalidFieldValue):
        db = vegaorm.Database(DEFAULT_CONN_DETAILS)

        class Person(vegaorm.Model):
            name = vegaorm.CharField(max_length=32)

        # Filter field by the wrong value type
        db.query(Person).filter(name=3).first()


def test_init_with_invalid_field_name():
    with pytest.raises(InvalidField):
        class Book(vegaorm.Model):
            name = vegaorm.CharField(max_length=32)

        # Initialize model with a field that does not exist
        Book(author='George Orwell')


def test_init_with_invalid_field_value():
    with pytest.raises(InvalidFieldValue):
        class Person(vegaorm.Model):
            name = vegaorm.CharField(max_length=32)

        # Initialize model with a field with the wrong value type
        Person(name=12)


def test_multiple_field_types():
    db = vegaorm.Database(DEFAULT_CONN_DETAILS)

    class Car(vegaorm.Model):
        name = vegaorm.CharField(max_length=32)
        mileage = vegaorm.IntField()
        drivable = vegaorm.BoolField()

    db.create_table(Car)

    # Create model using all three field types
    car = Car(name='Mercedes', mileage=12500, drivable=True)
    db.save(car)

    assert execute_sql('mydb', 'SELECT * FROM Car') == [(1, 'Mercedes', 12500, True)]
    assert car.id == 1

    car.mileage = 12700
    db.save(car)

    assert execute_sql('mydb', 'SELECT * FROM Car') == [(1, 'Mercedes', 12700, True)]


def test_lazy_evaluation_method_first():
    db = vegaorm.Database(DEFAULT_CONN_DETAILS)

    class Book(vegaorm.Model):
        name = vegaorm.CharField(max_length=32)

    db.create_table(Book)

    books = db.query(Book).filter(name='1984')

    book = Book(name='1984')
    db.save(book)

    # Lazy evaluates since the latest saved value is returned
    assert str(books.first()) == "{'id': 1, 'name': '1984'}"


def test_lazy_evaluation_method_all():
    db = vegaorm.Database(DEFAULT_CONN_DETAILS)

    class Book(vegaorm.Model):
        name = vegaorm.CharField(max_length=32)

    db.create_table(Book)

    books = db.query(Book).filter(name='1984')

    book = Book(name='1984')
    db.save(book)

    # Lazy evaluates since the latest saved value is returned
    lst = books.all()
    assert type(lst) == list
    assert str(lst[0]) == "{'id': 1, 'name': '1984'}"


def test_lazy_evaluation_method_getitem():
    db = vegaorm.Database(DEFAULT_CONN_DETAILS)

    class Book(vegaorm.Model):
        name = vegaorm.CharField(max_length=32)

    db.create_table(Book)

    books = db.query(Book).filter(name='1984')
    book = Book(name='1984')
    db.save(book)

    # Lazy evaluates since the latest saved value is returned
    assert str(books[0]) == "{'id': 1, 'name': '1984'}"


def test_lazy_evaluation_method_iter():
    db = vegaorm.Database(DEFAULT_CONN_DETAILS)

    class Book(vegaorm.Model):
        name = vegaorm.CharField(max_length=32)

    db.create_table(Book)

    books = db.query(Book).filter(name='1984')

    book = Book(name='1984')
    db.save(book)

    # Lazy evaluates since the latest saved value is returned
    books_iter = iter(books)
    assert str(next(books_iter)) == "{'id': 1, 'name': '1984'}"


def test_lazy_evaluation_method_contains():
    db = vegaorm.Database(DEFAULT_CONN_DETAILS)

    class Book(vegaorm.Model):
        name = vegaorm.CharField(max_length=32)

    db.create_table(Book)

    books = db.query(Book).filter(name='1984')

    book = Book(name='1984')
    db.save(book)

    # Lazy evaluates since the latest saved value is returned
    assert book in books


def test_multithreading():
    conn_details = DatabaseConfig(host=HOST, user='postgres', password='password', database='mydb', max_conn=2)
    db = vegaorm.Database(conn_details)

    class Book(vegaorm.Model):
        name = vegaorm.CharField(max_length=32)

    db.create_table(Book)

    book = Book(name='1984')
    db.save(book)

    # Start new thread with db connection that will wait for 3 seconds then update the table
    def run_sleep_sql(): db._execute('UPDATE book set name = (SELECT pg_sleep(3))')

    sleep_thread = Thread(target=run_sleep_sql)
    sleep_thread.start()

    # Value returned before thread can modify the value
    assert db._execute('SELECT * FROM book') == [(1, '1984')]
    sleep_thread.join()


def test_multithreading_no_connection_available():
    conn_details = DatabaseConfig(host=HOST, user='postgres', password='password', database='mydb', max_conn=1)
    db = vegaorm.Database(conn_details)

    class Author(vegaorm.Model):
        name = vegaorm.CharField(max_length=32)

    db.create_table(Author)

    book = Author(name='George Orwell')
    db.save(book)

    # Start new thread with db connection that will wait for 3 seconds then update the table
    def run_sleep_sql(): db._execute('UPDATE author set name = (SELECT pg_sleep(3))')

    sleep_thread = Thread(target=run_sleep_sql)
    sleep_thread.start()

    # Database connection started with a max of 1 connection, and it's being used by the other thread
    with pytest.raises(PoolError):
        db._execute('SELECT * FROM author')
    sleep_thread.join()


def test_null_value():
    db = vegaorm.Database(DEFAULT_CONN_DETAILS)

    class Book(vegaorm.Model):
        name = vegaorm.CharField(max_length=32)

    db.create_table(Book)

    book = Book(name=None)
    db.save(book)

    # Assigning a field value to None, causes a NULL value in the database
    assert execute_sql('mydb', 'SELECT * FROM Book') == [(1, None)]


def test_value_not_initialized():
    class Book(vegaorm.Model):
        name = vegaorm.CharField(max_length=32)

    # When all the defined fields are not provided, an error is thrown
    with pytest.raises(ValueNotInitialized):
        Book()


def test_table_migration():
    db = vegaorm.Database(DEFAULT_CONN_DETAILS)

    class Book(vegaorm.Model):
        name = vegaorm.CharField(max_length=32)
        pages = vegaorm.IntField()
        available = vegaorm.BoolField()

    db.create_table(Book)

    book = Book(name='Atomic Habits', pages=324, available=True)
    db.save(book)

    # Following statements causes the table to drop, add, and modify a column on the table
    delattr(Book, 'available')
    Book.name = vegaorm.CharField(max_length=64)
    Book.author = vegaorm.CharField()

    db.create_table(Book)

    sql = "SELECT column_name, data_type, character_maximum_length " \
          "FROM information_schema.columns WHERE table_name = 'book'"
    assert execute_sql('mydb', sql) == [
        ('id', 'integer', None),
        ('pages', 'integer', None),
        ('name', 'character varying', 64),
        ('author', 'character varying', None)
    ]


def test_filter_already_filtered_data():
    db = vegaorm.Database(DEFAULT_CONN_DETAILS)

    class Book(vegaorm.Model):
        name = vegaorm.CharField()
        author = vegaorm.CharField()
        available = vegaorm.BoolField()

    db.create_table(Book)

    book = Book(name='Fluent Python', author='Luciano Ramalho', available=True)
    db.save(book)
    book = Book(name='1984', author='George Orwell', available=True)
    db.save(book)
    book = Book(name='Animal Farm', author='George Orwell', available=True)
    db.save(book)
    book = Book(name='Homage to Catalonia', author='George Orwell', available=False)
    db.save(book)

    # Can filter on previously filtered query before evaluation
    books = db.query(Book).filter(author='George Orwell')
    books = books.filter(available=True)

    assert len(books) == 2
    assert str(books[0]) == "{'id': 2, 'name': '1984', 'author': 'George Orwell', 'available': True}"
    assert str(books[1]) == "{'id': 3, 'name': 'Animal Farm', 'author': 'George Orwell', 'available': True}"

    # Can filter on previously filtered query after evaluation
    books.filter(name='Animal Farm')
    assert len(books) == 1
    assert str(books[0]) == "{'id': 3, 'name': 'Animal Farm', 'author': 'George Orwell', 'available': True}"


@pytest.fixture(autouse=True)
def clean_up():
    # Clean-up method to run before each test case
    execute_sql('postgres', 'DROP DATABASE IF EXISTS mydb;')
    sql = "SELECT 1 WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mydb')"
    assert execute_sql('postgres', sql) == [(1,)]


########################################################################################################################

# Helper method to run SQL without using orm
def execute_sql(db_name: str, query: str):
    _get_connection(db_name)
    cur = conn.cursor()
    cur.execute(query)
    return cur.fetchall() if cur.description else None


# Provides connection to run SQL without using orm
def _get_connection(db_name: str):
    global conn
    if not conn or conn.info.dbname != db_name:
        conn = psycopg2.connect(host=HOST, database=db_name, user='postgres', password='password')
        conn.autocommit = True
