import os

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine.row import Row
from typing import Sequence, List, Tuple


class DBHandler:
    """
    Handles database connections and query executions.

    The DBHandler class is responsible for managing database connections using
    a connection pool and for executing SQL queries on the database. It retrieves
    the database URL from the environment variables and utilizes SQLAlchemy for
    setting up the connection engine and executing the queries securely.

    :ivar _engine: Database engine created to manage the connection pool and execute queries.
    :type _engine: sqlalchemy.engine.base.Engine
    """

    def __init__(self) -> None:

        # create instance variables
        try:
            db_url = os.getenv("DB_URL")
        except KeyError:
            raise KeyError("DB_URL environment variable is not set")

        # create the engine with connection pool
        self._engine = create_engine(
            db_url,
            pool_size=10,  # Size of the connection pool
            max_overflow=5  # Allow up to 5 extra connections
        )

    def getall(self) -> List:
        """
        Retrieves all the records from the database.

        :return: The list of all records from the database.
        :rtype: list
        """
        query = "SELECT * FROM records"
        return self._execute_query(query)

    def _execute_query(self, query: str) -> List:
        """
        Executes a SQL query on the database engine and retrieves the results
        in a JSON-compatible list format. This method establishes a connection
        to the database, executes the provided query string, fetches all the
        results, and processes them into a convenient JSON-compatible data
        structure.

        :param query: The SQL query string to be executed.
        :type query: str
        :return: A list of results formatted as JSON-compatible dictionaries.
        :rtype: List
        """
        with self._engine.connect() as connection:
            result = connection.execute(text(query))
            return self._get_json_list(result.fetchall())

    def _get_json_list(self, data: Sequence[Row]) -> List:
        """
        Converts a sequence of Row objects into a list of JSON-compatible dictionaries.

        This method iterates over a sequence of Row objects and converts each row into
        a JSON-compatible dictionary format. For every row in the input sequence, all
        column-value pairs are extracted and inserted into a dictionary, which is then
        added to the resulting list.

        :param data: Sequence of Row objects where each Row is treated as a collection
                     of key-value pairs representing columns and their respective values.
                     Each row is converted into its dictionary representation.
        :type data: Sequence[Row]
        :return: A list of dictionaries where each dictionary corresponds to the
                 JSON-compatible representation of a row in the input sequence.
        :rtype: List
        """
        json_list = []
        for row in data:
            row_data = {}
            for column, value in row.items():
                row_data[column] = value
                json_list.append(row_data)
        return json_list
