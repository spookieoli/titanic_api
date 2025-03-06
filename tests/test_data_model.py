import pytest
from typing import Dict, Any
from utils.data_model import Query


def test_query_instantiation():
    """
    Tests the instantiation of a Query object with nested query data structure.

    A dictionary containing detailed query data is used to instantiate a Query
    object. The test validates that the object is correctly initialized with
    appropriate values for the query table, query columns, and the nested
    conditions within the selector.

    :raises AssertionError: If the instantiation of the Query object does not
                             result in expected attribute values.
    """
    query_data = {
        "query_table": "users",
        "query_columns": ["name", "age", "country"],
        "selector": {
            "operator": {
                "$and": [
                    {"operator": {
                        "$or": [
                            {"statement": {"age": {"$gte": 18}}},
                            {"statement": {"membership": {"$eq": "premium"}}}
                        ]
                    }},
                    {"statement": {"country": {"$eq": "Germany"}}}
                ]
            }
        }
    }

    query = Query(**query_data)
    assert query.query_table == "users"
    assert "name" in query.query_columns
    assert "$and" in query.selector.operator


def test_query_to_dict():
    """
    Tests the conversion of a query representation into a dictionary format and verifies the
    output against given data and structure. The function initializes a `Query` object with
    predefined attributes, converts it to a dictionary, and checks if the resulting
    dictionary matches the input and expected structure. It ensures that the object is
    appropriately serialized and retains all nested structures and values.

    :return: None
    """
    query_data = {
        "query_table": "users",
        "query_columns": ["name", "age", "country"],
        "selector": {
            "operator": {
                "$and": [
                    {"operator": {
                        "$or": [
                            {"statement": {"age": {"$gte": 18}}},
                            {"statement": {"membership": {"$eq": "premium"}}}
                        ]
                    }},
                    {"statement": {"country": {"$eq": "Germany"}}}
                ]
            }
        }
    }

    query = Query(**query_data)
    query_dict = query.dict()

    assert isinstance(query_dict, Dict)
    assert query_dict["query_table"] == "users"
    assert query_dict["query_columns"] == ["name", "age", "country"]
    assert "$and" in query_dict["selector"]["operator"]


def test_invalid_query():
    """
    Tests the behavior of the Query class when initialized with invalid parameters.

    This test ensures that the Query class raises a ValueError when provided
    with invalid parameters, such as an improperly typed query_table,
    query_columns, or selector.

    :raises ValueError: Raised when the initialization parameters do not meet
        the expected types or values.
    """
    with pytest.raises(ValueError):
        Query(query_table=123, query_columns=["name"], selector={})

    with pytest.raises(ValueError):
        Query(query_table="users", query_columns="invalid", selector={})

    with pytest.raises(ValueError):
        Query(query_table="users", query_columns=["name"], selector=None)


if __name__ == "__main__":
    pytest.main()
