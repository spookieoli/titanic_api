from typing import List, Dict, Any, AnyStr, Optional, Literal
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
    Represents a query model for interacting with a database or other data sources.

    This class provides attributes for specifying a table, columns, and selection
    criteria for constructing and executing queries programmatically. It is designed
    to encapsulate query-related data and to allow validation through the BaseModel's
    capabilities, ensuring that the query structure adheres to predefined types and
    constraints.

    :ivar query_table: Name of the table to query.
    :type query_table: AnyStr
    :ivar query_columns: List of column names to retrieve from the table. If None,
        all columns will be selected.
    :type query_columns: Optional[List[AnyStr]]
    :ivar selector: Dictionary defining selection criteria in a structured format.
        Supports logical operators ("OR" or "AND") as keys and allows defining
        comparison operations such as "GT", "GTE", "LT", "LTE", and "EQ".
    :type selector: Optional[Dict[Literal["OR", "AND"], Dict[AnyStr, Dict[Literal["GT",
        "GTE", "LT", "LTE", "EQ"], AnyStr]]]]
    """
    query_table: AnyStr
    query_columns: Optional[List[AnyStr]] = None
    selector: Optional[Dict[Literal["OR", "AND"], Dict[AnyStr, Dict[Literal["GT", "GTE", "LT", "LTE", "EQ"], AnyStr]]]] = None