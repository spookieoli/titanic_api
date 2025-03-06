# tests/test_db_handler.py

import pytest
from sqlalchemy import create_engine, text
from utils.db_handler import DBHandler


@pytest.fixture(scope="module")
def db_handler():
    """
    Fixture to provide a database handler for tests.

    This pytest fixture initializes and returns an instance of the DBHandler
    class configured with a SQLite database connection. The handler is kept
    at the module scope, meaning it is reused across all tests in the same
    module.

    :yield:
        An instance of DBHandler connected to the specified SQLite database URL.
    """
    handler = DBHandler("sqlite:///../data/titanic.db")
    return handler


def test_get_all_tables(db_handler):
    """
    Tests whether the table "Observation" exists in the list of all tables retrieved
    from the database handler. The function asserts that "Observation" is present
    in the output of the `get_all_tables` method of the `db_handler` object.

    :param db_handler: The database handler object that provides access
                       to database operations, specifically the `get_all_tables` method.
    :type db_handler: Any

    :return: None
    """
    tables = db_handler.get_all_tables()
    assert "Observation" in tables


def test_get_table_columns(db_handler):
    """
    Tests whether specified columns exist in the "Observation"
    table retrieved by the given database handler.

    :param db_handler: Database handler instance used to perform the
        query.
    :type db_handler: Any
    :return: None
    """
    columns = db_handler.get_table_columns("Observation")
    assert "survived" in columns
    assert "pclass" in columns


def test_get_values(db_handler):
    """
    Fetches and validates the number of values retrieved from the database using the `get_values`
    method of the provided database handler. The function ensures that the number of fetched
    values matches the expected count.

    :param db_handler: A database handler object that provides the `get_values` method to
        query data from a database table.
    :returns: None. The function performs an assertion to validate the retrieved values.
    """
    values = db_handler.get_values("Observation", ["parch"])
    assert len(values) == 891


def test_get_distinct_values(db_handler):
    """
    Retrieves distinct values for the specified column(s) from a database table and
    validates the presence and count of the entries in the results.

    :param db_handler: The database handler instance used to perform the query.
    :type db_handler: object
    :return: None
    """
    values = db_handler.get_distinct_values("Observation", ["parch"])
    assert {"parch": 6} in values
    assert len(values) == 7


def test_get_count_all(db_handler):
    """
    Tests the `get_count` function of the `db_handler` object for the "Observation" table
    to ensure it retrieves the correct count of records.

    :param db_handler: Database handler object that provides the `get_count`
        method for retrieving counts from the database.
    :type db_handler: Any
    :return: None
    """
    count = db_handler.get_count("Observation")
    assert count == [{"COUNT(*)": 891}]


def test_get_count_column(db_handler):
    """
    Tests the ``get_count`` method of the provided database handler for retrieving a count
    of values in a specific column of a specific table.

    The method is expected to query the database for the count of all records in the
    column "parch" from the table "Observation", and return the result in the
    specified format.

    :param db_handler: The database handler object responsible for executing the
        ``get_count`` query for the specified table and column.
    :type db_handler: Any
    :return: None
    :rtype: None
    """
    count = db_handler.get_count("Observation", "parch")
    assert count == [{"COUNT(parch)": 891}]


def test_get_sum(db_handler):
    """
    Test the functionality of the `get_sum` method from the provided database handler.

    The function verifies that the `get_sum` method, when called with specific
    parameters, returns the expected result. It performs an assert statement to
    compare the actual return value of the method with the expected outcome.

    :param db_handler: The database handler object that contains the `get_sum`
        method to be tested.
    :type db_handler: Any
    :return: None
    """
    total = db_handler.get_sum("Observation", "parch")
    assert total == [{"SUM(parch)": 340}]


def test_get_min(db_handler):
    """
    Tests the functionality of retrieving the minimum value from a database table and column
    using the provided database handler.

    This function verifies that the `get_min` method of the `db_handler` correctly retrieves the
    minimum value for the specified table and column. It performs an assertion to ensure the output
    matches the expected result.

    :param db_handler: The database handler instance that provides the `get_min` method.
    :type db_handler: Any
    :return: None
    :rtype: NoneType
    """
    minimum = db_handler.get_min("Observation", "parch")
    assert minimum == [{"MIN(parch)": 0}]


def test_get_max(db_handler):
    """
    Tests the `get_max` method of the provided database handler.

    This function evaluates whether the `get_max` method of the
    `db_handler` correctly retrieves the maximum value of the specified
    column from a given table in the database. It asserts that the
    resulting maximum value matches the expected predefined value.

    :param db_handler: The database handler object used to query the
        database.
    :type db_handler: object
    :return: None
    """
    maximum = db_handler.get_max("Observation", "parch")
    assert maximum == [{"MAX(parch)": 6}]


def test_get_mean(db_handler):
    """
    Calculates the mean for a specified column in the database and verifies the result.

    This function interacts with the provided database handler to compute the mean
    of the 'parch' column within the "Observation" table. It then verifies
    the correctness of the computed mean against the expected value.

    :param db_handler: The database handler used to execute the query for
          calculating the mean.
    :type db_handler: object
    :return: None
    """
    mean = db_handler.get_mean("Observation", "parch")
    assert mean == [{"AVG(parch)": .38159371492704824}]


def test_get_all(db_handler):
    """
    Tests the retrieval of all records from the "Class" database table using the db_handler.

    This function ensures that the database handler correctly fetches all records from the
    "Class" table. Using assertions, the function verifies that the expected number of
    records exist and the predefined record is present in the retrieved data.

    :param db_handler: The database handler responsible for fetching records. It should
        provide a `get_all` method that takes the name of a table as an argument and
        returns a list of records.
    :return: None
    """
    all_data = db_handler.get_all("Class")
    assert len(all_data) == 3
    assert {"class_id": 0, "class": "First"} in all_data
