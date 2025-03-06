import pytest
from utils.query_to_sql import QueryToSQL

def test_simple_eq():
    """
    Tests the equality operator conversion of a query statement into SQL syntax.

    The function creates an instance of the `QueryToSQL` converter and a sample
    selector dictionary, ensuring that the equality operator `$eq` is properly
    translated into its SQL equivalent (`=`).

    :raises AssertionError: If the conversion of the `$eq` operator does not match
        the expected SQL equivalent.
    """
    converter = QueryToSQL()
    selector = {"statement": {"age": {"$eq": 30}}}
    assert converter.convert_operator(selector["statement"]) == "age = '30'"

def test_simple_ne():
    """
    Tests the conversion of "$ne" (not equal) operator in the QueryToSQL class.

    This function verifies that the QueryToSQL class converts a MongoDB-style query
    selector containing the "$ne" operator into the correct SQL equivalent. The
    function creates an instance of QueryToSQL, sets up the selector with the "$ne"
    operator, and asserts that the conversion output matches the expected SQL string.

    :rtype: None
    """
    converter = QueryToSQL()
    selector = {"statement": {"age": {"$ne": 25}}}
    assert converter.convert_operator(selector["statement"]) == "age != '25'"

def test_complex_and():
    """
    Tests the conversion of a complex query selector into an SQL WHERE clause
    with the logical "$and" operation.

    This function calls the `query_selector_to_sql` method on a `QueryToSQL`
    instance with a selector containing multiple conditions combined with the
    "$and" operator and verifies that the resulting SQL string matches the
    expected output.

    :return: None
    :rtype: None
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
    assert converter.query_selector_to_sql(selector) == "(age >= 18) AND (country = 'Germany')"

def test_complex_or():
    """
    Converts a logical query with an "$or" operator into SQL syntax. This function
    tests the ability of the `QueryToSQL` converter to handle complex queries
    involving the "$or" operator, combining multiple conditions.

    :raises AssertionError: If the SQL conversion result does not match the
        expected SQL representation.
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
    assert converter.query_selector_to_sql(selector) == "(age < 18) OR (membership = 'premium')"

def test_nested_conditions():
    """
    Tests the conversion of a nested logical condition selector into an SQL
    query string format. The test ensures that the combinations of `$and`
    and `$or` operators, as well as various statement conditions, are
    accurately translated into the corresponding SQL structure by the
    QueryToSQL converter.

    This test includes:
    1. An `$and` logical operation.
    2. A nested `$or` logical operation inside the `$and`.
    3. Statement-level conditions represented in a dictionary format.
    4. Ensures that the resultant SQL query maintains the precedence of
       logical operations and properly groups conditions using parentheses.

    :param selector: A dictionary defining a query with nested logical
        operators and conditions that need to be converted into an SQL
        query by the QueryToSQL converter.
    :type selector: dict

    :param expected_sql: Expected SQL query string that represents the
        logically equivalent format of the provided selector in SQL.
    :type expected_sql: str

    :raises AssertionError: If the converted query does not match the
        expected SQL query format.

    :return: None
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
    expected_sql = "((age >= 18) OR (membership = 'premium')) AND (country = 'Germany')"
    assert converter.query_selector_to_sql(selector) == expected_sql
    assert converter.get_fields() == ['membership', 'age', 'country']

def test_empty_selector():
    """
    Tests the behavior of the `query_selector_to_sql` method when given an empty selector.

    This test checks the expected output of the `query_selector_to_sql` method in the
    `QueryToSQL` class when provided with an empty dictionary as input. It ensures the
    returned SQL string is empty as a result of processing the empty selector.

    :return: None
    """
    converter = QueryToSQL()
    selector = {}
    assert converter.query_selector_to_sql(selector) == ""

if __name__ == "__main__":
    pytest.main()