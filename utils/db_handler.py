import os

import sqlalchemy
from sqlalchemy import create_engine, Table
from sqlalchemy import text
from sqlalchemy.engine.row import Row
from typing import Sequence, List, Tuple, Any


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

    def get_distinct_values(self, table: str, columns: List) -> List:
        """
        Retrieves distinct values from specified columns in a database table.

        This function queries the specified `table` to fetch unique combinations of
        values across the provided `columns`. If no columns are provided, it returns
        an empty list.

        :param table: The name of the database table to query.
        :param columns: A list of column names whose distinct values are to be fetched.
        :return: A list of rows, each containing distinct values for the specified
            columns.
        :rtype: List
        """
        if len(columns) == 0:
            return []
        sql = f"SELECT DISTINCT {', '.join(columns)} FROM {table}"
        return self._execute_query(sql)

    def get_table_columns(self, table: str) -> List:
        """
        Retrieves a list of column names for a specified table from the database.

        This function queries the database schema to fetch all column names
        associated with the given table name. It relies on the INFORMATION_SCHEMA
        view to extract the column metadata. The result is a list containing the
        names of the columns.

        :param table: The name of the table for which the column names are to be
            retrieved.
        :type table: str
        :return: A list of column names found in the specified table.
        :rtype: List
        """
        sql = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'"
        return self._execute_query(sql)

    def get_all_tables(self) -> List[str]:
        """
        Retrieves the names of all tables in a database.

        This function uses SQLAlchemy's metadata reflection capability to fetch
        all table names from the connected database engine. It is particularly
        useful for dynamic database introspection purposes.

        :return: A list of strings, where each string is the name of a table in
            the connected database.
        :rtype: List[str]
        """
        meta = sqlalchemy.MetaData()
        meta.reflect(bind=self._engine)
        return list(meta.tables.keys())

    def get_sum(self, table: str, column: str) -> List:
        """
        Calculate the sum of all values within a specified column from a given table
        in the database by executing a SQL query. This function constructs the
        SQL query dynamically using the provided table and column names and retrieves
        the sum of the column values.

        This method uses the `_execute_query` function to execute the query and
        obtain the result. The returned result contains the computed sum.

        :param table: Name of the table to query data from
        :type table: str
        :param column: Name of the column whose values will be summed
        :type column: str
        :return: The result of the executed query containing the computed sum of the column values
        :rtype: List
        """
        sql = f"SELECT SUM({column}) FROM {table}"
        return self._execute_query(sql)

    def get_count(self, table: str, column: str = None) -> List:
        """
        Retrieve count of rows or column values from a database table.

        This method executes an SQL query to count the number of rows in a specified
        table or, if a column is provided, the number of values in that column. If no
        specific column is given, it counts all rows in the table.

        :param table: Name of the database table from which the count is to be retrieved.
        :type table: str
        :param column: Optional name of the table column to count values for. If not
            provided, rows in the table are counted.
        :type column: str, optional
        :return: Result of the query containing the count of the rows or column values.
        :rtype: List
        """
        if column is None:
            sql = f"SELECT COUNT(*) FROM {table}"
        else:
            sql = f"SELECT COUNT({column}) FROM {table}"
        return self._execute_query(sql)

    def get_min(self, table: str, column: str) -> List:
        """
        Retrieve the minimum value from the specified column in the given table.

        This function constructs a SQL query to obtain the smallest value
        from the provided column within the specified table. It then executes
        the query utilizing the `_execute_query` method and returns the result.

        :param table: The name of the table from which to fetch the minimum value.
        :type table: str
        :param column: The name of the column to retrieve the minimum value from.
        :type column: str
        :return: A list containing the result of the query execution, which includes
                 the minimum value of the specified column.
        :rtype: List
        """
        sql = f"SELECT MIN({column}) FROM {table}"
        return self._execute_query(sql)

    def get_max(self, table: str, column: str) -> List:
        """
        Fetches the maximum value of a given column from a specified database table.

        This method executes a SQL query to find and return the maximum value within
        a specified column of a table in the database. The method relies on an
        internal `_execute_query` function for executing the constructed SQL query.

        :param table: The name of the table to be queried.
        :param column: The name of the column from which the maximum value is to be retrieved.
        :return: A list containing the maximum value retrieved from the specified column.
        :rtype: List
        """
        sql = f"SELECT MAX({column}) FROM {table}"
        return self._execute_query(sql)

    def get_mean(self, table: str, column: str) -> List:
        """
        Calculate the mean (average) value of a specified column in a given database
        table. This function constructs a SQL query to retrieve the mean from the
        column of interest and executes it using the internal query execution
        mechanism.

        :param table: The name of the database table where the column exists.
        :param column: The name of the column for which the mean value is to be
            calculated.
        :return: A list containing the result of the mean value calculation.
        :rtype: List
        """
        sql = f"SELECT AVG({column}) FROM {table}"
        return self._execute_query(sql)

    def get_all(self, table: str) -> List:
        """
        Retrieves all records from the specified database table.

        This method constructs an SQL query to fetch all rows from the
        given table and executes it using the `_execute_query` method. The
        result is returned as a list.

        :param table: The name of the table from which to retrieve all records.
        :type table: str

        :return: A list of all records retrieved from the table.
        :rtype: List
        """
        sql = f"SELECT * FROM {table}"
        return self._execute_query(sql)

    def _execute_query(self, query: str) -> List:
        """
        Executes a SQL query on the database engine and retrieves the results
        in a JSON-compatible list format. This method establishes a connection
        to the database, executes the provided query string, fetches all the
        results, and processes them into a convenient JSON-compatible data
        structure by calling the `_get_json_list` method.

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
