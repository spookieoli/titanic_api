import os

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import text


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
