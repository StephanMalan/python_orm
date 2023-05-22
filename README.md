# Python ORM

In this project, I use Python to create my own simple implementation of an ORM.

It currently has the following features:

- Supports Postgres, MySQL, SQLite
- Create table based on class definition
- Provides three field types to define the table (CharField, IntField, BoolField)
- Save row to table based on instantiated class values
- Validates that user input the correct field names and field values
- Update row of table based on instantiated class values if the id is the same
- Query table based on exact matching
- Support lazy evaluation of query
- Supports filtering of already filtered query
- Supports multi-threading by increasing the maximum amount of connections to create
- Automatic changes to table schema based on class definition changes

For testing, I use pytest and coverage to run multiple test scenarios and report on the code coverage.

The testing is done using three Docker containers, orchestrated by docker compose:

- A container that runs the postgres database
- A container that runs the mysql database
- A container that runs all test cases and displays the results and the code coverage

To run the tests, use the following command:

```
make run_test
```

---
**NOTE**

This is a simple implementation and lacks a lot of functionality of a standard ORM. Foreign key support has not been
added yet. All use cases and error validation have not been met.
