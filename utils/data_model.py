from typing import List, Dict, Any, AnyStr, Optional, ForwardRef, Literal, Union
from pydantic import BaseModel


class QueryResult(BaseModel):
    """
    Represents the result of a query execution.

    This class encapsulates the structure and data resulting from a query operation. It provides
    a list of dictionaries where each dictionary corresponds to a single result row, containing
    key-value pairs for column names and their respective values. It is typically used for handling
    query results in a structured way which models a tabular response.

    :ivar result: List of dictionaries containing query result rows. Each dictionary represents a
        single row with key-value pairs corresponding to column names and their respective values.
    :type result: List[Dict[AnyStr, Any]]
    """
    result: List[Dict[AnyStr, Any]]


class Statement(BaseModel):
    """
    Represents a logical statement model for structured queries.

    This class is used for defining a model that encapsulates a structured query
    in the form of statements with conditions. Each statement includes a dictionary
    where keys are strings and values themselves are dictionaries with specific
    operators ('$eq', '$ne', '$lt', '$lte', '$gt', '$gte') as keys and their
    corresponding values. The model validates the structured query to ensure
    proper usage of these operators with associated data.

    :ivar statement: A dictionary representing a statement. Each key is a string
        and the value is a dictionary of conditions where the keys are valid
        operators ('$eq', '$ne', '$lt', '$lte', '$gt', '$gte') and the values are
        the comparison values for these operators.
    :type statement: Dict[AnyStr, Dict[Literal['$eq', '$ne', '$lt', '$lte', '$gt',
        '$gte'], Any]]
    """
    statement: Dict[AnyStr, Dict[Literal['$eq', '$ne', '$lt', '$lte', '$gt', '$gte'], Any]]


OperatorRef = ForwardRef('Operator')


class Operator(BaseModel):
    """
    Represents a logical operator used within a query system.

    This class serves as a structure to combine multiple statements or references to
    other operators into a single logical group. It supports logical conjunctions
    (AND) and disjunctions (OR) and is useful for building complex query expressions
    in a structured manner.

    :ivar operator: Defines the logical operation to perform. This dictionary includes
        a specific logical key ('$or' or '$and') with its value as a list of statements
        or references to other operators. The key determines whether the expressions
        within this grouping should be logically combined using AND or OR.
    :type operator: Dict[Literal['$or', '$and'], List[Union[Statement, OperatorRef]]]
    """
    operator: Dict[Literal['$or', '$and'], List[Union[Statement, OperatorRef]]]


class Query(BaseModel):
    """
    Represents a database query configuration.

    This class encapsulates the details required for structuring
    and executing a database query. It allows specifying the target
    table, the columns to query, and a logical selector to filter
    or refine query results.

    :ivar query_table: The name of the database table to query.
    :type query_table: AnyStr
    :ivar query_columns: A list of column names to include in the query.
                         If None, all columns are included.
    :type query_columns: Optional[List[AnyStr]]
    :ivar selector: A logical operator used to filter or refine query
                    results. This can include conditions like `AND`,
                    `OR`, etc. If None, no filtering is applied.
    :type selector: Optional[Operator]
    """
    query_table: AnyStr
    query_columns: Optional[List[AnyStr]] = None
    selector: Optional[Operator] = None
