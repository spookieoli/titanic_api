import os

import sqlalchemy
from sqlalchemy import create_engine, Table
from sqlalchemy import text
from sqlalchemy.engine.row import Row
from typing import Sequence, List, Tuple, Any
import logging
from utils.query_to_sql import QueryToSQL

from utils.data_model import Operator

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

    def check_parameter(self, table: str, parameter: Operator) -> Tuple[bool, str, dict]:
        """
        Validates the provided parameter for a given table and generates a corresponding SQL query.
        This function checks whether all fields required by the parameter exist within the specified table.

        :param table: The name of the table to be queried. Must be provided as a string.
        :param parameter: An operator object containing details of the query parameter. If no parameter
            is provided (None), the function defaults to returning True with an empty query string.
        :return: A tuple containing a boolean and a string. The boolean indicates whether the parameter
            validation and table-field existence checks succeeded. The string represents the corresponding
            SQL query, if applicable. Returns an empty query when validation fails or parameter is None.
        """
        if parameter is not None:
            q = QueryToSQL()
            query, parameters = q.query_selector_to_sql(parameter.model_dump())
            v = q.get_fields()
            if self.check_table_column_exist(table, v) is False:
                return False, "", {}
            else:
                return True, query, parameters
        else:
            return True, "", {}

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

    def get_values(self, table: str, columns: List[str], parameter: Operator) -> List:
        """
        Retrieve specific column values from a database table based on the provided parameters.

        This method performs a query on the given database table to fetch specific column values. If a parameter
        is provided, it will be used to create a WHERE clause to filter the results. If no columns are provided
        or the specified columns or table do not exist, it will return an empty list.

        :param table: The name of the database table to query.
        :type table: str
        :param columns: A list of column names to return values for. If empty or non-existent in the table,
            returns an empty list.
        :type columns: List[str]
        :param parameter: An operator object used to construct a conditional clause for filtering results.
            Can be None for unconditional queries.
        :type parameter: Operator
        :return: A list of rows containing the values for the specified columns or an empty list if no data
            is found or the query is invalid.
        :rtype: List
        """

        # create statement from Operator if present
        exists, query, parameters = self.check_parameter(table, parameter)

        if exists is False:
            return []

        if len(columns) == 0 or self.check_table_column_exist(table, columns) is False and exists is False:
            return []
        if query == "":
            sql = text(f"SELECT {', '.join(columns)} FROM {table}")
            return self._execute_query(sql)
        else:
            sql = text(f"SELECT {', '.join(columns)} FROM {table} WHERE {query}")
            return self._execute_query(sql, parameters)

    def get_distinct_values(self, table: str, columns: List, parameter: Operator) -> List:
        """
        Retrieve distinct values from a specified table and columns, optionally filtered by a condition.
        This method queries a database table to fetch unique records from specified columns. If a condition
        is provided through the `parameter` argument, it filters the data based on the constructed query
        from its value.

        :param table:
            The name of the database table from which to retrieve the data.
        :type table: str

        :param columns:
            A list of column names in the table to return distinct values from.
        :type columns: List

        :param parameter:
            An optional parameter used to construct a filtering query based on an operator. This can
            be used to apply conditions to the data retrieval process.
        :type parameter: Operator

        :return:
            A list containing distinct values from the specified columns, optionally filtered by the
            provided parameter. If columns or parameters are invalid, an empty list is returned.
        :rtype: List
        """
        # create statement from Operator if present
        exists, query, parameters = self.check_parameter(table, parameter)

        if exists is False:
            return []

        if len(columns) == 0 or self.check_table_column_exist(table, columns) is False and exists is False:
            return []
        if query == "":
            sql = text(f"SELECT DISTINCT {', '.join(columns)} FROM {table}")
            return self._execute_query(sql)
        else:
            sql = text(f"SELECT DISTINCT {', '.join(columns)} FROM {table} WHERE {query}")
            return self._execute_query(sql, parameters)

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

    def get_sum(self, table: str, column: str, parameter: Operator) -> List:
        """
        Calculates the sum of the values in a specified column of a given table. If a specific
        condition is provided via an Operator parameter, it applies the condition as a filter
        before performing the summation. This method returns a list containing the result of
        the executed query.

        :param table: The name of the database table to query.
        :type table: str
        :param column: The name of the column from which the sum is calculated.
        :type column: str
        :param parameter: An Operator instance representing the condition to filter the rows.
        :type parameter: Operator
        :return: A list containing the result of the summation query. Could be empty if the
            table or column does not exist, or if no rows match the given condition.
        :rtype: List
        """
        # create statement from Operator if present
        exists, query, parameters = self.check_parameter(table, parameter)

        if exists is False:
            return []

        if self.check_table_column_exist(table, [column]) is False and exists is False:
            return []

        if query == "":
            sql = text(f"SELECT SUM({column}) FROM {table}")
            return self._execute_query(sql)
        else:
            sql = text(f"SELECT SUM({column}) FROM {table} WHERE {query}")
            return self._execute_query(sql, parameters)

    def get_count(self, table: str, column: str, parameter: Operator) -> List:
        """
        Retrieves the count of rows in a database table, optionally filtered by a
        specific condition or using a specific column.

        This method constructs an SQL query to count rows in a table based on the
        provided parameters. If a column is specified, it counts values in that
        column; otherwise, it counts rows in general. Query filtering can also
        be applied if an operator is provided.

        :param table: The name of the database table to query.
        :param column: The column to be counted from the specified table. If None, the
            method counts rows in the table.
        :param parameter: The operator used to define a filtering condition for the
            query. May be None if no filtering is required.
        :return: A list of results from the executed SQL query, typically containing
            the count as a single value.
        :rtype: List
        """

        # create statement from Operator if present
        exists, query, parameters = self.check_parameter(table, parameter)

        if exists is False:
            return []

        sql = ""
        if column is None and table in self.get_all_tables() and query == "":
            sql = text(f"SELECT COUNT(*) FROM {table}")
            return self._execute_query(sql)
        elif column is None and table in self.get_all_tables() and query != "":
            sql = text(f"SELECT COUNT(*) FROM {table} WHERE {query}")
            return self._execute_query(sql, parameters)
        elif column is not None and column in self.get_table_columns(table) and table in self.get_all_tables():
            sql = text(f"SELECT COUNT({column}) FROM {table}")
            return self._execute_query(sql)
        elif column is not None and column in self.get_table_columns(
                table) and table in self.get_all_tables() and query != "":
            sql = text(f"SELECT COUNT({column}) FROM {table} WHERE {query}")
            return self._execute_query(sql, parameters)
        else:
            return []

    def get_min(self, table: str, columns: List[str], parameter: Operator) -> List:
        """
        Retrieve the minimum value(s) of specified column(s) from a database table while considering optional filtering
        conditions. This method generates an SQL query with optional filters provided through the `Operator` parameter
        and executes it against the connected database. If no valid table or columns are provided, an empty list is
        returned. The query evaluates specified columns and supports conditional filters if a valid `Operator` instance
        is passed.

        :param table: The name of the database table from which to query.
        :type table: str
        :param columns: A list of column names whose minimum values should be retrieved.
        :type columns: list of str
        :param parameter: An `Operator` instance specifying optional filter criteria for the query.
        :type parameter: Operator
        :return: A list containing the result(s) from the executed query, or an empty list if table or column validation fails.
        :rtype: list
        """

        # create statement from Operator if present
        exists, query, parameters = self.check_parameter(table, parameter)

        if exists is False:
            return []

        if self.check_table_column_exist(table, columns) and query == "":
            sql = text(f"SELECT MIN({', '.join(columns)}) FROM {table}")
            return self._execute_query(sql)
        elif self.check_table_column_exist(table, columns) and query != "":
            sql = text(f"SELECT MIN({', '.join(columns)}) FROM {table} WHERE {query}")
            return self._execute_query(sql, parameters)
        return []

    def get_max(self, table: str, columns: List[str], parameter: Operator) -> List:
        """
        Retrieve the maximum value(s) of specified columns in a given database table, filtered
        by an optional condition provided through an Operator.

        This method fetches the maximum value(s) from one or more specified columns in the
        provided table. If an optional filtering condition is specified through the provided
        Operator instance, it will apply the condition to the query. If the condition is invalid
        or the columns do not exist in the table, an empty list is returned.

        :param table: The name of the database table to query.
        :type table: str
        :param columns: List of column names for which maximum values need to be retrieved.
        :type columns: List[str]
        :param parameter: An optional Operator instance that specifies a condition to apply to the query.
        :type parameter: Operator
        :return: A list containing the maximum value(s) from the specified columns. Returns an empty list
                 if the table, columns, or query condition are invalid.
        :rtype: List
        """

        # create statement from Operator if present
        exists, query, parameters = self.check_parameter(table, parameter)

        if exists is False:
            return []

        if self.check_table_column_exist(table, columns) and query == "":
            sql = text(f"SELECT MAX({', '.join(columns)}) FROM {table}")
            return self._execute_query(sql)
        elif self.check_table_column_exist(table, columns) and query != "":
            sql = text(f"SELECT MAX({', '.join(columns)}) FROM {table} WHERE {query}")
            return self._execute_query(sql, parameters)
        return []

    def get_mean(self, table: str, columns: List[str], parameter: Operator) -> List:
        """
        Calculate the mean of specified columns in a database table optionally constrained by
        a parameter.

        This method retrieves the average (mean) value of the given column(s) from the specified
        database table. It allows filtering the data using an optional parameter, encapsulated
        within an `Operator` type. The function ensures the table and columns exist before
        executing the query. If a parameter is specified, it appends the relevant WHERE clause
        to the query.

        :param table: The name of the database table to query.
        :type table: str
        :param columns: A list of column names whose averages are to be computed.
        :type columns: List[str]
        :param parameter: Optional parameter defining constraints for the query.
        :type parameter: Operator
        :return: A list containing the computed average values, or an empty list if the
            table/columns do not exist, or no results match the query.
        :rtype: List
        """
        # create statement from Operator if present
        exists, query, parameters = self.check_parameter(table, parameter)

        if exists is False:
            return []

        if self.check_table_column_exist(table, columns) and query == "":
            sql = text(f"SELECT AVG({', '.join(columns)}) FROM {table}")
            return self._execute_query(sql)
        elif self.check_table_column_exist(table, columns) and query != "":
            sql = text(f"SELECT AVG({', '.join(columns)}) FROM {table} WHERE {query}")
            return self._execute_query(sql, parameters)
        return []

    def get_all(self, table: str, parameter: Operator) -> List:
        """
        Retrieves all records from a specified table that satisfy the specified condition.

        This method interacts with a database to fetch records. It first checks
        if the condition provided as an `Operator` object is applicable. If the
        condition exists, it prepares a query and fetches the data from the table.
        Only records from the existing table and columns will be retrieved based on
        the query condition.

        :param table: The name of the database table from which records need to be
            retrieved.
        :type table: str
        :param parameter: An instance of `Operator` representing the condition to
            apply on table data, used to form the query.
        :type parameter: Operator
        :return: A list of records retrieved from the specified table that satisfy
            the condition, or an empty list if no records satisfy the condition or
            the table/columns do not exist.
        :rtype: List
        """

        # create statement from Operator if present
        exists, query, parameters = self.check_parameter(table, parameter)

        if exists is False:
            return []

        if self.check_table_column_exist(table):
            return self._execute_query(text(f"SELECT * FROM {table}"))
        elif self.check_table_column_exist(table) and query != "":
            return self._execute_query(text(f"SELECT * FROM {table} WHERE {query}"), parameters)
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
