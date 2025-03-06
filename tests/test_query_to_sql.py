import pytest
from utils.query_to_sql import QueryToSQL

def test_simple_eq():
    """
    Tests the equality operator conversion for the QueryToSQL class by ensuring
    that the output SQL string and parameters match the expected results. This
    function verifies that the `$eq` operator is correctly translated into the
    SQL equality syntax.

    :return: None
    """
    converter = QueryToSQL()
    selector = {"statement": {"age": {"$eq": 30}}}
    sql, params = converter.convert_operator(selector["statement"])
    assert sql == "age = :age"
    assert params == {"age": 30}

def test_simple_ne():
    """
    Tests a simple query that uses the $ne (not equal) operator. Converts the query
    statement into an SQL equivalent and validates the generated SQL expression
    and parameters.

    The test ensures that the correct SQL syntax and parameter mapping are
    produced when using a $ne operator in the input query.

    :raises AssertionError: If the generated SQL query or parameters do not match
        the expected values.
    """
    converter = QueryToSQL()
    selector = {"statement": {"age": {"$ne": 25}}}
    sql, params = converter.convert_operator(selector["statement"])
    assert sql == "age != :age"
    assert params == {"age": 25}

def test_complex_and():
    """
    Tests the logical AND operator within a complex query structure by converting
    it to a SQL representation. The test verifies that the resulting SQL query
    and parameters match the expected values.

    :raises AssertionError: If the generated SQL or parameters do not match the
        expected output.
    """
    converter = QueryToSQL()
    selector = {
        "operator": {
            "$and": [
                {"statement": {"age": {"$gte": 18}}},
                {"statement": {"country": {"$eq": "Germany"}}}
            ]
        }
    }
    sql, params = converter.query_selector_to_sql(selector)
    assert sql == "(age >= :age) AND (country = :country)"
    assert params == {"age": 18, "country": "Germany"}

def test_complex_or():
    """
    Tests the functionality of logical OR operator processing in the query-to-SQL
    conversion. This function evaluates whether the `QueryToSQL` converter
    correctly handles a selector with an `$or` operator. The selector combines two
    conditions: one that checks if `age` is less than 18, and another that checks
    if `membership` equals `'premium'`.

    The function asserts that the output SQL statement correctly translates the
    logical OR operator and that the parameter bindings are accurate.

    :raises AssertionError: If the SQL string or the parameter bindings generated
                            by the `query_selector_to_sql` method do not meet the
                            expected results.
    """
    converter = QueryToSQL()
    selector = {
        "operator": {
            "$or": [
                {"statement": {"age": {"$lt": 18}}},
                {"statement": {"membership": {"$eq": "premium"}}}
            ]
        }
    }
    sql, params = converter.query_selector_to_sql(selector)
    assert sql == "(age < :age) OR (membership = :membership)"
    assert params == {"age": 18, "membership": "premium"}

def test_nested_conditions():
    """
    Tests the conversion of a nested query selector to SQL syntax with
    parameters. This function checks the ability of the QueryToSQL converter
    to handle logical operators such as `$and` and `$or` with multiple
    conditions. It validates the output SQL statement and corresponding
    parameters against expected results for a given input query selector.

    The test ensures the converter properly processes both `statement` and
    `operator` keys within nested conditions, verifying the integrity of
    complex query transformations.

    :raises AssertionError: If the resulting SQL or parameters do not match
        the expected values.
    """
    converter = QueryToSQL()
    selector = {
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
    sql, params = converter.query_selector_to_sql(selector)
    expected_sql = "((age >= :age) OR (membership = :membership)) AND (country = :country)"
    expected_params = {"age": 18, "membership": "premium", "country": "Germany"}
    assert sql == expected_sql
    assert params == expected_params

def test_empty_selector():
    """
    Tests the behavior of the query_selector_to_sql method when provided with an
    empty selector object. This ensures the method handles empty input correctly
    by returning an empty SQL string and an empty parameters dictionary.

    :raises AssertionError: If the test fails due to non-matching output.
    :return: None
    """
    converter = QueryToSQL()
    selector = {}
    sql, params = converter.query_selector_to_sql(selector)
    assert sql == ""
    assert params == {}

def test_get_fields():
    """
    Tests the ``get_fields`` method of the ``QueryToSQL`` class.

    This test validates that the fields extracted from a query selector
    by the ``get_fields`` method are correct. It ensures that the query
    selector to SQL conversion occurs successfully beforehand.

    :raises AssertionError: Raised when the field set extracted by
        ``get_fields`` does not match the expected fields based on the
        original query selector.
    """
    converter = QueryToSQL()
    selector = {
        "operator": {
            "$and": [
                {"statement": {"name": {"$eq": "John"}}},
                {"statement": {"age": {"$gt": 25}}}
            ]
        }
    }
    converter.query_selector_to_sql(selector)
    assert set(converter.get_fields()) == {"name", "age"}

if __name__ == "__main__":
    pytest.main()
