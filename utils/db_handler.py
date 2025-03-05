import os

import sqlalchemy
from sqlalchemy import create_engine, Table
from sqlalchemy import text
from sqlalchemy.engine.row import Row
from typing import Sequence, List, Tuple, Any
import logging

# create logging
logging.basicConfig(filename='app.log', level=logging.DEBUG)


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

    def __init__(self, db_url: str = "sqlite:///./data/titanic.db") -> None:
        """
        Initializes the database connection and creates an engine with a connection pool.

        This constructor retrieves the database URL from the environment variables, sets
        up a SQLAlchemy database engine with a specified connection pool size, and configures
        extra overflow connections. It ensures robust management of database connections.

        :raises KeyError: If the `DB_URL` environment variable is not set.
        """
        # create the engine with connection pool
        self._engine = create_engine(
            db_url
        )

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
        sql = text(f"SELECT DISTINCT {', '.join(columns)} FROM {table}")
        return self._execute_query(sql)

    def get_table_columns(self, tab: str) -> List:
        """
        This method retrieves the names of all columns in a specified database table. It first checks if the table
        exists and then extracts the column names using SQLAlchemy's metadata and reflection features.

        :param tab: The name of the table to retrieve column names for.
        :type tab: str
        :return: A list of column names from the specified table. If the table does not exist, an empty list is returned.
        :rtype: List[str]
        """
        meta = sqlalchemy.MetaData()
        if self.check_table_column_exist(tab):
            table = sqlalchemy.Table(tab, meta, autoload_with=self._engine)
            return [column.name for column in table.columns]
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
            sql = text(f"SELECT SUM({column}) FROM {table}")
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

    def get_min(self, table: str, columns: List[str]) -> List:
        """
        Get the minimum values of the specified columns from a given database table.

        This method checks if the given table and columns exist in the database
        before executing the query. If the columns and table are valid, it retrieves
        the minimum values for the specified columns from the table. If the table or
        columns are invalid, it returns an empty list without executing the query.

        :param table: Name of the table in the database.
        :type table: str
        :param columns: List of column names for which the minimum values are to
            be retrieved.
        :type columns: List[str]
        :return: A list containing the minimum values for the specified columns if
            valid; otherwise, an empty list.
        :rtype: List
        """
        if self.check_table_column_exist(table, columns):
            sql = text(f"SELECT MIN({', '.join(columns)}) FROM {table}")
            return self._execute_query(sql)
        return []

    def get_max(self, table: str, columns: List[str]) -> List:
        """
        Retrieves the maximum values for specified columns from a given table.

        The method checks if the specified table and columns exist before executing
        an SQL query to fetch the maximum values for the given columns from the table.
        If the table or columns do not exist, it returns an empty list.

        :param table: The name of the table from which the maximum values should
                      be retrieved.
        :param columns: A list of column names to fetch their maximum values
                        from the specified table.
        :type table: str
        :type columns: List[str]
        :return: A list containing the maximum values from the specified columns.
                 If the table or columns do not exist, an empty list is returned.
        :rtype: List
        """
        if self.check_table_column_exist(table, columns) is False:
            return []
        sql = text(f"SELECT MAX({', '.join(columns)}) FROM {table}")
        return self._execute_query(sql)

    def get_mean(self, table: str, columns: List[str]) -> List:
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
        if self.check_table_column_exist(table, columns) is False or len(columns) == 0 or len(columns) > 1:
            return []
        sql = text(f"SELECT AVG({', '.join(columns)}) FROM {table}")
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
            return self._execute_query(text(f"SELECT * FROM {table}"))
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

        try:
            with self._engine.connect() as connection:
                result = connection.execute(query, parameters or {})
                return self._get_json_list(result)
        except Exception as e:
            logging.error(f"Error executing query: {e}")
            return []

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
        if table is None:
            return False
        if table not in self.get_all_tables():
            return False
        if columns is not None:
            for column in columns:
                if column not in self.get_table_columns(table):
                    return False
        return True

    def _get_json_list(self, result: sqlalchemy.Result) -> List:
        """
        Converts the result set of a SQL query into a list of dictionaries representing
        each row.

        This method transforms a SQLAlchemy Result object into a JSON-like
        list of dictionaries, where each dictionary corresponds to a row
        from the database query result, with column names as keys and their
        respective values as the dictionary's values. If the provided result is
        None, an empty list is returned.

        :param result: The SQLAlchemy Result object containing the query result.
        :type result: sqlalchemy.Result
        :return: A list of dictionaries where each dictionary corresponds to a row
            in the result set with column names as keys and values as the row's values.
        :rtype: List
        """
        if result is None:
            return []

        column_names = result.keys()
        json_list = []
        rows = result.fetchall()

        for row in rows:
            row_data = {}
            for idx, column_name in enumerate(column_names):
                row_data[column_name] = row[idx]
            json_list.append(row_data)
        return json_list
