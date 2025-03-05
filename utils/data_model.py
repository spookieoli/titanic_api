from typing import List, Dict, Any, AnyStr, Optional
from pydantic import BaseModel

class QueryResult(BaseModel):
    """
    Represents the result of a query, containing a list of query result rows.

    Provides a structure to hold the rows returned from a query execution. Each
    row is represented as an instance of `QueryResultRow`, and all rows are
    maintained in a list.

    :ivar result: A list of rows returned from the query, where each row is an
        instance of `QueryResultRow`.
    :type result: List[QueryResultRow]
    """
    result: List[Dict[AnyStr, Any]]

class Query(BaseModel):
    """
    Represents a query model which defines the structure for data query operations.

    This class is used to model a database query with attributes specifying
    the target table and selected columns. It provides a structured way of
    defining query parameters for data retrieval operations and is typically
    used in conjunction with database handling modules or query builder utilities.

    :ivar query_table: The name of the table to query.
    :type query_table: AnyStr
    :ivar query_columns: A list of column names to be retrieved from the table.
    :type query_columns: List[AnyStr]
    """
    query_table: AnyStr
    query_columns: Optional[List[AnyStr]] = None