import sqlglot
from sqlglot.expressions import Expression

class SQLParser:
    """
    SQLParser is a utility class for parsing SQL queries into ASTs using sqlglot.
    """

    def __init__(self):
        pass

    def parse(self, sql: str) -> Expression:
        """
        Parse the given SQL string and return its AST (Abstract Syntax Tree).

        Parameters:
            sql (str): A valid SQL query string.

        Returns:
            Expression: The root node of the parsed SQL AST.
        """
        try:
            parsed = sqlglot.parse_one(sql)
            #print("parsed：")
            return parsed
        except Exception as e:
            print(f"[SQLParser Error] Failed to parse SQL: {sql}")
            raise e
