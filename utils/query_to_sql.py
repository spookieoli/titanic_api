from typing import Dict, Any, List, Union, Set


class QueryToSQL:
    """
    Provides functionality to convert query selectors and statements into their corresponding SQL
    representations.

    This class is designed to facilitate the conversion of structured query selectors, defined
    using a specific JSON-like format, into SQL WHERE clause conditions. It processes each
    condition and builds the appropriate SQL string based on the designated operators and
    values.

    :ivar supported_operators: Dictionary of supported query operators mapped to their SQL
        equivalents.
    :type supported_operators: Dict[str, str]
    """

    def __init__(self):
        self._fields: Set[str] = set()

    def convert_operator(self, statement: Dict[str, Dict[str, Any]]) -> str:
        """
        Converts a dictionary-based query operator structure into an SQL expression string.

        The function takes a dictionary where the keys are field names and the values are
        dictionaries specifying a series of conditions with MongoDB-like operators such as
        "$eq", "$ne", "$lt", "$lte", "$gt", and "$gte". Each operator is translated into
        its corresponding SQL operation. Multiple conditions are combined using "AND".

        :param statement:
            A dictionary where each key represents a field name and its value is another
            dictionary containing conditions with operators (e.g., `$eq`, `$ne`) and corresponding values.
        :return:
            A string representing the translated SQL expression, which combines the conditions
            using logical "AND".
        """
        sql_parts = []
        for field, conditions in statement.items():
            self._fields.add(field)
            for operator, value in conditions.items():
                if operator == "$eq":
                    sql_parts.append(f"{field} = '{value}'")
                elif operator == "$ne":
                    sql_parts.append(f"{field} != '{value}'")
                elif operator == "$lt":
                    sql_parts.append(f"{field} < {value}")
                elif operator == "$lte":
                    sql_parts.append(f"{field} <= {value}")
                elif operator == "$gt":
                    sql_parts.append(f"{field} > {value}")
                elif operator == "$gte":
                    sql_parts.append(f"{field} >= {value}")
        return " AND ".join(sql_parts)

    def query_selector_to_sql(self, selector: Dict[str, Any]) -> str:
        """
        Convert a nested query selector into an SQL-compliant logical condition.

        This method takes a dictionary-based selector object and recursively converts
        it into an equivalent SQL string representing the logical condition. The
        selector can include `$and` or `$or` operators, which process multiple
        statements or nested operators. If no valid "operator" key is detected in the
        selector, the method will return an empty string.

        :param selector: A dictionary containing the logical query conditions. The
            dictionary may include "$and" or "$or" operators, each containing
            a list of conditions, which can be other nested operator-based
            conditions or a "statement" to be converted.
        :return: A string containing the SQL logical condition generated from the
            input `selector`. If no operators or valid statements exist,
            an empty string is returned.
        """
        if "operator" in selector:
            for key, conditions in selector["operator"].items():
                sql_conditions = []
                for condition in conditions:
                    if "statement" in condition:
                        sql_conditions.append(f"({self.convert_operator(condition['statement'])})")
                    elif "operator" in condition:
                        sql_conditions.append(f"({self.query_selector_to_sql(condition)})")
                if key == "$and":
                    return " AND ".join(sql_conditions)
                elif key == "$or":
                    return " OR ".join(sql_conditions)
        return ""

    def get_fields(self) -> List[str]:
        """
        Provides functionality to retrieve fields stored in the class instance.

        :return: A list of strings containing field names stored in the class.
        :rtype: List[str]
        """
        return list(self._fields)
