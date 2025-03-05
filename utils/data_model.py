from typing import List, Dict, Any, AnyStr, Optional, Union
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

class ComparisonOperator(BaseModel):
    """
    Represents a set of comparison operators for evaluating conditions.

    This class is used to define optional comparison operator attributes such as
    greater than, greater than or equal, less than, less than or equal, and equal
    to. The values of these attributes can be integers or strings. It is built
    on top of BaseModel for validation and structure.

    :ivar GT: Optional value for the 'greater than' operator.
    :type GT: Optional[Union[int, AnyStr]]
    :ivar GTE: Optional value for the 'greater than or equal to' operator.
    :type GTE: Optional[Union[int, AnyStr]]
    :ivar LT: Optional value for the 'less than' operator.
    :type LT: Optional[Union[int, AnyStr]]
    :ivar LTE: Optional value for the 'less than or equal to' operator.
    :type LTE: Optional[Union[int, AnyStr]]
    :ivar EQ: Optional value for the 'equal to' operator.
    :type EQ: Optional[Union[int, AnyStr]]
    """
    GT: Optional[Union[int, AnyStr]] = None
    GTE: Optional[Union[int, AnyStr]] = None
    LT: Optional[Union[int, AnyStr]] = None
    LTE: Optional[Union[int, AnyStr]] = None
    EQ: Optional[Union[int, AnyStr]] = None


class SelectorCondition(BaseModel):
    """
    Represents a condition used to evaluate a selection criterion.

    This class is used to define a selection condition with a field, its
    comparison operation, and an associated value. It is useful in cases where
    a logical condition must be applied to data for selection or filtering. It
    is built upon a BaseModel and supports various comparison operations.

    :ivar field: The field or attribute on which the condition is applied.
    :type field: AnyStr
    :ivar operators: The operator used for comparison in the condition.
    :type operators: ComparisonOperator
    """
    field: AnyStr
    operators: ComparisonOperator


class LogicalOperator(BaseModel):
    """
    Represents a logical operator for combining selector conditions.

    This class is used to define logical operations (`OR` and `AND`)
    to combine multiple selector conditions. It supports optional lists
    of SelectorCondition objects for both logical operations.

    :ivar OR: A list of selector conditions to be combined using the OR operator.
    :type OR: Optional[List[SelectorCondition]]
    :ivar AND: A list of selector conditions to be combined using the AND operator.
    :type AND: Optional[List[SelectorCondition]]
    """
    OR: Optional[List[SelectorCondition]] = None
    AND: Optional[List[SelectorCondition]] = None


class Query(BaseModel):
    """
    Represents a query configuration for a database model.

    This class is designed to encapsulate the details of a query operation,
    including the target table, specific columns to retrieve, and any logical
    filter operators. It is a part of the data modeling layer and is used to
    facilitate structured queries.

    :ivar query_table: The name of the table to query.
    :type query_table: AnyStr
    :ivar query_columns: A list of column names to query from the table. If None,
        all columns will be retrieved.
    :type query_columns: Optional[List[AnyStr]]
    :ivar selector: The logical operation or filter applied to the query. Used
        for specifying complex conditional logic.
    :type selector: Optional[LogicalOperator]
    """
    query_table: AnyStr
    query_columns: Optional[List[AnyStr]] = None
    selector: Optional[LogicalOperator] = None