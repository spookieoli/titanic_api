import pytest
from typing import Dict, Any
from utils.data_model import Query


def test_query_instantiation():
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
    with pytest.raises(ValueError):
        Query(query_table=123, query_columns=["name"], selector={})

    with pytest.raises(ValueError):
        Query(query_table="users", query_columns="invalid", selector={})

    with pytest.raises(ValueError):
        Query(query_table="users", query_columns=["name"], selector=None)


if __name__ == "__main__":
    pytest.main()
