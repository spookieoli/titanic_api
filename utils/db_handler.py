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
        """
        Initializes the database connection and creates an engine with a connection pool.

        This constructor retrieves the database URL from the environment variables, sets
        up a SQLAlchemy database engine with a specified connection pool size, and configures
        extra overflow connections. It ensures robust management of database connections.

        :raises KeyError: If the `DB_URL` environment variable is not set.
        """
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

    def get_stddev(self, table: str, columns: List) -> List:
        """
        Calculates the standard deviation for the specified columns in a table.

        This method computes the standard deviation for the provided columns in the
        given database table. If no columns are provided, or if the table does not
        exist in the database, it returns an empty list. The function constructs an
        SQL query using the provided table and column names and executes it to
        retrieve the required statistical information.

        :param table: The name of the database table to compute standard deviation.
                      It should be a string representing a valid table name.
        :type table: str
        :param columns: A list of column names from the given table to compute
                        standard deviations for. The list should contain at least one
                        column, unless the table does not exist.
        :type columns: List
        :return: A list containing the standard deviation values for the specified
                 columns. Returns an empty list if no columns are provided or the
                 table does not exist.
        :rtype: List
        """
        if len(columns) == 0 and self.check_table_column_exist(table) is False:
            return []
        sql = text(f"SELECT STDDEV({', '.join(columns)}) FROM {table}")
        return self._execute_query(sql)

    def get_values(self, table: str, columns: List[str]) -> List:
        """
        Retrieves specific values from a database table based on the provided column names.
        If the given column list is empty or the columns do not exist in the specified table,
        an empty list is returned.

        :param table: The name of the table to query.
        :type table: str
        :param columns: A list of column names to retrieve from the table.
        :type columns: List[str]
        :return: A list containing the retrieved values from the specified table and columns.
        :rtype: List
        """
        if len(columns) == 0 or self.check_table_column_exist(table, columns) is False:
            return []
        sql = text(f"SELECT {', '.join(columns)} FROM {table}")
        return self._execute_query(sql)

    def get_distinct_values(self, table: str, columns: List) -> List:
        """
        Fetches distinct values for specified columns from a given database table.

        This method queries the database to retrieve all unique value combinations
        of the specified columns from the given table. If no columns are provided
        or if the table or columns do not exist, the method returns an empty list.

        :param table: Name of the database table to query.
        :type table: str
        :param columns: List of column names in the table whose distinct values
            are to be retrieved.
        :type columns: List
        :return: A list containing rows of distinct values for the specified
            columns, where each row is represented as a list.
        :rtype: List
        """
        if len(columns) == 0 or self.check_table_column_exist(table, columns) is False:
            return []
        sql = f"SELECT DISTINCT {', '.join(columns)} FROM {table}"
        return self._execute_query(sql)

    def get_table_columns(self, table: str) -> List:
        """
        Retrieves the list of column names for a specified table within the database.

        This method checks if the given table exists in the database schema by
        cross-referencing it with the list of all available tables. If the table
        exists, it executes an SQL query to fetch all the column names associated
        with that table. If the table does not exist, an empty list is returned.

        :param table: The name of the table for which to retrieve the column names.
        :type table: str
        :return: A list of column names in the specified table, or an empty list if
            the table does not exist.
        :rtype: List
        """
        if self.check_table_column_exist(table):
            sql = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'"
            result = self._execute_query(sql)
            return [row['COLUMN_NAME'] for row in result]
        return []

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
        Gets the sum of the values in a specified column within a table if the column exists
        in the provided table.

        This method first validates if the specified column exists in the table by checking
        against the table's columns. If the column exists, it constructs an SQL query to
        calculate the sum of the column's values. Otherwise, it returns an empty list.

        :param table: The name of the database table.
        :type table: str
        :param column: The name of the column whose values need to be summed.
        :type column: str
        :return: A list containing the sum of the column's values if the column exists;
            otherwise, an empty list.
        :rtype: List
        """
        if self.check_table_column_exist(table, [column]):
            sql = f"SELECT SUM({column}) FROM {table}"
            return self._execute_query(sql)
        return []

    def get_count(self, table: str, column: str = None) -> List:
        """
        Generates and executes a SQL query to count rows in a given database table. If a
        column is specified, it counts non-NULL values in that column. Returns the query
        result as a list. If the table or column does not exist, an empty list is returned.

        :param table: Name of the database table to count rows from.
        :type table: str
        :param column: Name of the column to count non-NULL values from, defaults to None.
        :type column: str, optional
        :return: List containing query result or an empty list if conditions are not met.
        :rtype: List
        """
        sql = ""
        if column is None and table in self.get_all_tables():
            sql = text(f"SELECT COUNT(*) FROM {table}")
            return self._execute_query(sql)
        elif column is not None and column in self.get_table_columns(table) and table in self.get_all_tables():
            sql = text(f"SELECT COUNT({column}) FROM {table}")
            return self._execute_query(sql)
        else:
            return []

    def get_min(self, table: str, column: str) -> List:
        """
        Retrieve the minimum value from a specified column in a given table.

        This function checks if the given column exists in the specified table and if the
        table is present in the database. If both conditions are satisfied, it constructs
        and executes a SQL query to fetch the minimum value from the specified column of
        the table. If the column or table is not valid, it returns an empty list.

        :param table: The name of the table from which to retrieve data. Must be a valid
                      table in the database.
        :type table: str
        :param column: The name of the column for which to find the minimum value. Must
                       belong to the specified table.
        :type column: str
        :return: A list containing the result of the SQL query, which includes the minimum
                 value from the specified column, or an empty list if the table or column
                 is invalid.
        :rtype: List
        """
        if self.check_table_column_exist(table, [column]):
            sql = f"SELECT MIN({column}) FROM {table}"
            return self._execute_query(sql)
        return []

    def get_max(self, table: str, column: str) -> List:
        """
        Retrieve the maximum value from a specified column within a table.

        This function queries the database for the largest value in the specified
        column of the provided table name. If the table or column does not exist
        in the database schema, the function returns an empty list. It utilizes
        the constructed SQL query to fetch the maximum value from the database.

        :param table: The name of the database table from which to retrieve the
            maximum value.
        :type table: str
        :param column: The name of the column from within the table, whose
            maximum value is to be retrieved.
        :type column: str
        :return: The result of the SQL query, containing the maximum value, or
            an empty list if the table or column is invalid.
        :rtype: List
        """
        if self.check_table_column_exist(table, [column]) is False:
            return []
        sql = f"SELECT MAX({column}) FROM {table}"
        return self._execute_query(sql)

    def get_mean(self, table: str, column: str) -> List:
        """
        Calculate the mean value for a given column in a specific table.

        This method checks whether the specified column exists within the given
        table and if the given table exists in the database. If either is not
        present, an empty list is returned. Otherwise, it performs an SQL query
        to calculate the average value of the specified column.

        :param table:
            The name of the table in the database from which the mean value
            of the column will be calculated.
        :type table: str
        :param column:
            The name of the column within the table to calculate the mean value.
        :type column: str
        :return:
            A list which contains the result of the SQL query for the mean value
            of the specified column. If the table or column does not exist, an
            empty list is returned.
        :rtype: List
        """
        if self.check_table_column_exist(table, [column]) is False:
            return []
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
        if self.check_table_column_exist(table):
            return self._execute_query(f"SELECT * FROM {table}")
        else:
            return []

    def _execute_query(self, query: text, parameters: dict = None) -> List:
        """
        Executes a SQL query on the database engine and retrieves the results
        in a JSON-compatible list format. This method establishes a connection
        to the database, executes the provided query string with parameters,
        fetches all the results, and processes them into a convenient JSON-compatible data
        structure by calling the `_get_json_list` method.
        
        :param query: The SQL query string to be executed.
        :type query: str
        :param parameters: A dictionary of parameters to safely bind to the query.
        :type parameters: dict
        :return: A list of results formatted as JSON-compatible dictionaries.
        :rtype: List
        """
        with self._engine.connect() as connection:
            result = connection.execute(query, parameters or {})
            return self._get_json_list(result.fetchall())

    def check_table_column_exist(self, table: str = None, columns: [str] = None) -> bool:
        """
        Checks if the specified columns exist in a given table.

        This function validates the presence of a table and a list of columns.
        It ensures that both the table exists and all specified columns are part
        of the table's schema. If either the table or the columns are not provided,
        the validation directly returns False.

        :param table: The name of the table to be checked.
        :type table: str
        :param columns: A list of column names to check within the specified table.
        :type columns: list of str
        :return: Returns True if the table exists and all specified columns are present
                 in the table schema, otherwise False.
        :rtype: bool
        """
        if table is None or columns is None:
            return False
        if table not in self.get_all_tables():
            return False
        for column in columns:
            if column not in self.get_table_columns(table):
                return False
        return True

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
