import pytest
from utils.query_to_sql import QueryToSQL

def test_single_statement():
    """
    Converts a MongoDB-like query selector to an SQL WHERE clause along with its
    parameters. It ensures that a single statement selector is accurately
    translated into SQL syntax.

    :param selector: A dictionary representing the MongoDB-like query selector.
    :type selector: dict
    :return: A tuple containing the SQL WHERE clause as a string and a dictionary
             of parameters to be substituted in the SQL statement.
    :rtype: tuple
    """
    converter = QueryToSQL()
    selector = {"statement": {"age": {"$eq": 30}}}
    sql, params = converter.query_selector_to_sql(selector)
    assert sql == "(age = :age)"
    assert params == {"age": 30}

def test_single_statement_ne():
    """
    Converts a query selector with a "$ne" operator to its equivalent SQL WHERE clause
    and parameter dictionary. The "$ne" operator signifies "not equal to" in the query
    selector, and this logic is converted appropriately into an SQL valid conditional
    expression.

    :param selector: Dictionary representing the query with a "$ne" selector.
    :type selector: dict

    :return: A tuple consisting of the generated SQL WHERE clause and the corresponding
        dictionary of SQL parameters.
    :rtype: tuple
    """
    converter = QueryToSQL()
    selector = {"statement": {"name": {"$ne": "John"}}}
    sql, params = converter.query_selector_to_sql(selector)
    assert sql == "(name != :name)"
    assert params == {"name": "John"}

def test_complex_and():
    """
    Tests the logical `$and` operator in a query selector to verify proper conversion into SQL syntax
    using the `QueryToSQL` converter.

    The input selector contains multiple conditions combined under the `$and` logical operator.
    The method validates if the resulting SQL string and parameter mapping are generated correctly
    to reflect all conditions in the `AND` clause.

    :raises AssertionError: Raised if the SQL string or parameter dictionary does not match the
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
    Tests the `query_selector_to_sql` method with a complex logical OR operation in
    the query selector, ensuring that the SQL generated represents the correct
    logical structure and the corresponding parameters are correctly formed.

    This test validates the correct conversion of a selector object containing an
    "$or" operator that combines two conditions into an SQL query using the
    `QueryToSQL` class. The expected SQL must reflect the logical OR operation,
    and the parameters should map correctly to the values provided in the selector.

    :return: None
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
    Tests nested logical conditions in a query selector conversion process using the QueryToSQL
    converter. This ensures that the converter correctly transforms a complex nested query structure
    with logical operators (`$and`, `$or`) and conditions into an equivalent SQL representation
    with properly formatted statements and parameters. It checks for accurate SQL strings and
    parameter mappings based on the input selector.

    :raises AssertionError: If the SQL string or parameters returned by the converter do not
        match the expected SQL statement and parameter mapping.

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
    sql, params = converter.query_selector_to_sql(selector)
    expected_sql = "((age >= :age) OR (membership = :membership)) AND (country = :country)"
    expected_params = {"age": 18, "membership": "premium", "country": "Germany"}
    assert sql == expected_sql
    assert params == expected_params

def test_empty_selector():
    """
    Tests the `query_selector_to_sql` method of the `QueryToSQL` class with an
    empty selector. Ensures that the method correctly returns an empty SQL
    statement and an empty set of parameters when provided an empty selector.

    This test validates the behavior of the method under the condition where no
    filtering criteria are applied.

    :raises AssertionError: If the returned SQL statement or parameters are not
        as expected.
    """
    converter = QueryToSQL()
    selector = {}
    sql, params = converter.query_selector_to_sql(selector)
    assert sql == ""
    assert params == {}

def test_get_fields():
    """
    Test the functionality of extracting fields from a query converted into SQL.

    This function tests the process of converting a structured query selector into an
    SQL-compatible format using the `QueryToSQL` converter and then ensures the fields
    defined in the query are accurately returned by the `get_fields` method.

    :raises AssertionError: If the set of extracted fields does not match the expected
        fields.
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

def test_statement_with_multiple_conditions():
    """
    Converts a MongoDB-like query selector containing multiple conditions into an equivalent
    SQL WHERE clause and parameters. This test ensures the QueryToSQL class correctly
    handles queries with multiple conditions on a single key.

    :raises AssertionError: If the resulting SQL query string or parameters do not match
                            the expected values.
    """
    converter = QueryToSQL()
    selector = {"statement": {"age": {"$gte": 18, "$lte": 30}}}
    sql, params = converter.query_selector_to_sql(selector)
    assert sql == "(age >= :age AND age <= :age)"
    assert params == {"age": 30}  # Age should only be updated with the latest value

def test_mixed_and_or():
    """
    Tests the conversion of a nested query with mixed logical operators "$and" and "$or"
    into its equivalent SQL representation using the `QueryToSQL` class.

    This function validates whether the `query_selector_to_sql` method correctly handles
    nested logical combinations and translates them into the correct SQL query syntax
    with appropriate placeholders and corresponding parameter mappings. It uses a
    complex selector structure to ensure the proper evaluation of logical operator
    precedence and composition.

    :return: None
    """
    converter = QueryToSQL()
    selector = {
        "operator": {
            "$and": [
                {"statement": {"age": {"$gte": 18}}},
                {"operator": {
                    "$or": [
                        {"statement": {"membership": {"$eq": "premium"}}},
                        {"statement": {"country": {"$eq": "Germany"}}}
                    ]
                }}
            ]
        }
    }
    sql, params = converter.query_selector_to_sql(selector)
    expected_sql = "(age >= :age) AND ((membership = :membership) OR (country = :country))"
    expected_params = {"age": 18, "membership": "premium", "country": "Germany"}
    assert sql == expected_sql
    assert params == expected_params
