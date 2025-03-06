from typing import Dict, Any, List, Union, Set, Tuple


class QueryToSQL:
    """
    Handles the conversion of MongoDB-like query selectors and statements into
    SQL WHERE condition expressions and associated parameters. The purpose is to
    bridge Mongo-like conditional queries to SQL-compatible formats, ensuring
    precision and consistency with mapping.

    This utility can be used for query transformations where SQL databases are
    employed, and Mongo-like query syntax is desired for the application layer.

    :ivar _fields: Set containing fields processed in the query statements.
    :type _fields: Set[str]
    """

    def __init__(self):
        self._fields: Set[str] = set()

    def convert_operator(self, statement: Dict[str, Dict[str, Any]]) -> Tuple[str, Dict[str, Any]]:
        """
        Converts a dictionary of field conditions into an SQL query and a dictionary
        of parameters for use in that query. The method processes logical operators
        and generates corresponding SQL fragments.

        :param statement: A dictionary where keys are field names and values are
            dictionaries mapping operators (e.g., $eq, $lt) to their associated
            values.

        :return: A tuple consisting of:
            - An SQL query string that combines the conditions using "AND".
            - A dictionary of parameters mapping field names to their values
              for the prepared SQL statement.
        """
        sql_parts = []
        params = {}
        for field, conditions in statement.items():
            self._fields.add(field)
            for operator, value in conditions.items():
                param_name = field.replace(".", "_")  # Ensure param names are valid
                params[param_name] = value
                if operator == "$eq":
                    sql_parts.append(f"{field} = :{param_name}")
                elif operator == "$ne":
                    sql_parts.append(f"{field} != :{param_name}")
                elif operator == "$lt":
                    sql_parts.append(f"{field} < :{param_name}")
                elif operator == "$lte":
                    sql_parts.append(f"{field} <= :{param_name}")
                elif operator == "$gt":
                    sql_parts.append(f"{field} > :{param_name}")
                elif operator == "$gte":
                    sql_parts.append(f"{field} >= :{param_name}")
        return " AND ".join(sql_parts), params

    def query_selector_to_sql(self, selector: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """
        Converts a nested selector dictionary into a SQL WHERE clause and its corresponding
        parameters. The function processes the input selector, which may contain nested logical
        operators ("$and", "$or") and statements, recursively converting these into SQL
        conditions. The resulting SQL conditions and the associated parameters are returned.

        :param selector: The dictionary representing the query selector. It may contain the keys
            "statement" to define a condition and/or "operator" which itself contains logical
            operators such as "$and" or "$or" to group multiple conditions.
        :type selector: Dict[str, Any]

        :return: A tuple containing the SQL WHERE clause as a string and a dictionary of the
            associated parameters to be used in the SQL query. If the selector results in
            no conditions, an empty string and an empty dictionary are returned.
        :rtype: Tuple[str, Dict[str, Any]]

        """
        sql_conditions = []
        params = {}

        if "statement" in selector:
            condition_sql, condition_params = self.convert_operator(selector["statement"])
            sql_conditions.append(f"({condition_sql})")
            params.update(condition_params)

        if "operator" in selector:
            for key, conditions in selector["operator"].items():
                sub_conditions = []
                for condition in conditions:
                    if "statement" in condition:
                        condition_sql, condition_params = self.convert_operator(condition["statement"])
                        sub_conditions.append(f"({condition_sql})")
                        params.update(condition_params)
                    elif "operator" in condition:
                        nested_sql, nested_params = self.query_selector_to_sql(condition)
                        sub_conditions.append(f"({nested_sql})")
                        params.update(nested_params)
                if key == "$and":
                    sql_conditions.append(" AND ".join(sub_conditions))
                elif key == "$or":
                    sql_conditions.append(" OR ".join(sub_conditions))

        return " AND ".join(sql_conditions) if sql_conditions else "", params

    def get_fields(self) -> List[str]:
        """
        Retrieves a list of all field names stored within the object.
        """
        return list(self._fields)