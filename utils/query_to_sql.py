from typing import Dict, Any, List, Union, Set, Tuple


class QueryToSQL:
    """
    Transforms structured, dictionary-like query representations into SQL-compatible
    language while keeping track of fields and safely binding parameters. The primary
    use case is to bridge the gap between a structured query language and SQL, enabling
    safe and dynamic query building.

    The class provides methods to convert individual operators, nested selectors, and
    logical conditions into SQL expressions. Fields referenced during the conversion
    are collected and can be retrieved for further operations.

    :ivar _fields: Set of field names that have been used in the conversion processes.
    :type _fields: set
    """

    def __init__(self):
        self._fields: Set[str] = set()

    def convert_operator(self, statement: Dict[str, Dict[str, Any]]) -> Tuple[str, Dict[str, Any]]:
        """
        Converts a MongoDB-like query statement with operators into an SQL-like
        query string and a corresponding dictionary of parameters. The mapping
        between operators and their SQL equivalents is precise and strictly adheres
        to supported operations. The function handles equality, inequality, less than,
        less than or equal, greater than, and greater than or equal conditions.

        During conversion:
        - Field names are used to generate SQL-compliant query parts.
        - Parameters are derived from field values in the query, ensuring they
          are safe and valid for SQL parameterized statements.

        :param statement:
            A dictionary where each key is a field being queried and the value is
            another dictionary defining the operators and their corresponding
            values. Operators include `$eq`, `$ne`, `$lt`, `$lte`, `$gt`, and `$gte`.
            Example:
            {
                "field_name": {
                    "$operator": value
                }
            }
        :return:
            A tuple containing:
            - A string representing the SQL condition clause where the
              conditions for the provided fields are joined with `AND`.
            - A dictionary where keys are derived from field names
              (ensuring compliance with SQL) and values correspond to
              the conditions specified in the input statement.
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
        Converts a selector dictionary into an SQL WHERE clause and its associated parameters. The method
        parses a nested dictionary structure with logical operators such as `$and` and `$or`,
        and builds the corresponding SQL conditions while collecting the necessary query
        parameters. It supports deeply nested logical expressions and conditional statements.

        :param selector: A dictionary containing the selector query structure to be converted
            into SQL format. The dictionary can contain keys such as `operator`,
            which may include logical operators `$and` or `$or`, along with statements.
            Each statement represents an individual condition or nested condition to
            be processed.
        :type selector: Dict[str, Any]

        :return: A tuple where the first element is a string containing the SQL WHERE clause
            generated from the selector, and the second element is a dictionary of parameters
            referenced within the SQL query. Parameters are populated as they are derived
            from statements and operators in the selector.
        :rtype: Tuple[str, Dict[str, Any]]
        """
        sql_conditions = []
        params = {}

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

        This method accesses the internal `_fields` attribute, which contains
        field names. The attribute is presumed to be a collection of field
        identifiers, and this method ensures that an independent copy of
        those field names is returned as a list. This avoids unintended
        modifications to the original `_fields` attribute.

        :return: A list of strings representing field names.
        :rtype: List[str]
        """
        return list(self._fields)

