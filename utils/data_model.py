from typing import List, Dict, Any, Optional, Union
from pydantic import BaseModel, Field

class QueryResult(BaseModel):
    """
    Represents the result of a query operation.
    """
    result: List[Dict[str, Any]]

class ComparisonOperator(BaseModel):
    """
    This class represents a set of comparison operators.
    """
    gt: Optional[Union[int, str]] = Field(default=None, alias="$gt")
    gte: Optional[Union[int, str]] = Field(default=None, alias="$gte")
    lt: Optional[Union[int, str]] = Field(default=None, alias="$lt")
    lte: Optional[Union[int, str]] = Field(default=None, alias="$lte")
    eq: Optional[Union[int, str]] = Field(default=None, alias="$eq")

    def model_dump(self, **kwargs):
        """
        Serialisiert nur die gesetzten Operatoren und entfernt Felder mit Wert None.
        """
        data = super().model_dump(**kwargs)
        return {k: v for k, v in data.items() if v is not None}

class SelectorCondition(BaseModel):
    """
    Represents a condition used for selecting and filtering data.
    """
    field: str
    operators: ComparisonOperator

class LogicalOperator(BaseModel):
    """
    Represents a model for logical operators used in conditional queries.
    """
    or_: Optional[List[SelectorCondition]] = Field(default=None, alias="$or")
    and_: Optional[List[SelectorCondition]] = Field(default=None, alias="$and")

    def model_dump(self, **kwargs):
        """
        Serialisiert nur die gesetzten logischen Operatoren (OR/AND) und entfernt Felder mit Wert None.
        """
        data = super().model_dump(**kwargs)
        return {k: v for k, v in data.items() if v is not None}

class Query(BaseModel):
    """
    Represents a query configuration model.
    """
    query_table: str
    query_columns: Optional[List[str]] = None
    selector: Optional[LogicalOperator] = None