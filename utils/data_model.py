from typing import List, Dict, Any
from pydantic import BaseModel

class QueryResultRow(BaseModel):
    """
    Represents a single row of query results as a dictionary-like object.

    This class wraps the result of a query, allowing each row of the results
    to be represented as a dictionary with key-value pairs where keys are
    column names and values are corresponding data. It inherits from
    `BaseModel`, providing all the features of Pydantic models, such as
    data validation and processing.

    :ivar __root__: Dictionary representing the row data with column names as keys
        and corresponding values.
    :type __root__: Dict[str, Any]
    """
    __root__: Dict[str, Any]

class QueryResult(BaseModel):
    """
    Represents the result of a query operation as a list of query result rows.

    This class encapsulates the data returned by a query, structured as a list of
    `QueryResultRow` objects. It inherits from `BaseModel` and utilizes Pydantic's
    validation features. This allows for defining strict requirements and ensuring
    type safety for the query results. This class makes it easier to manipulate,
    validate, and access query results in a structured and predictable manner.

    :ivar __root__: List containing the query result rows.
    :type __root__: List[QueryResultRow]
    """
    __root__: List[QueryResultRow]