import os

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import text


class DBHandler:
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

    def execute_query(self, query: str) -> list:
        """
        Executes a query on the database and returns the result.

        :param query: The query to execute.
        :type query: str
        :return: The result of the query.
        :rtype: list
        """
        with self._engine.connect() as connection:
            result = connection.execute(text(query))
            return result.fetchall()
