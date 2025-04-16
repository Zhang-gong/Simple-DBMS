import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from sqlglot.expressions import Expression, Column, EQ, Literal, Where
from typing import List, Dict, Any
from storage.table import Table  # import your Table class

class Executor:
    """
    Executor runs queries defined by an AST on registered Table instances.
    """

    def __init__(self, tables: Dict[str, Table]):
        """
        Initialize with a mapping of table name to Table instances.

        Parameters:
            tables (dict): Table name -> Table instance
        """
        self.tables = tables

    def execute(self, ast: Expression) -> List[Dict[str, Any]]:
        """
        Execute a SQL AST on the registered tables.

        Parameters:
            ast (Expression): Parsed SQL AST.

        Returns:
            List[Dict[str, Any]]: Result rows.
        """
        # Get table name from AST
        from_expr = ast.args.get("from")
        table_expr = from_expr.this
        table_name = table_expr.name

        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' not found.")

        table_obj = self.tables[table_name]
        all_rows = table_obj.select_all()

        # Get selected columns
        select_fields = [col.name for col in ast.args["expressions"]]

        # WHERE clause
        where_expr = ast.args.get("where")
        if where_expr:
            condition = where_expr.this
            if isinstance(condition, EQ):
                key = condition.this.name
                value = condition.expression.this
                try:
                    value = int(value)
                except:
                    pass
                filtered_rows = [row for row in all_rows if row.get(key) == value]
            else:
                raise NotImplementedError("Only '=' condition is supported.")
        else:
            filtered_rows = all_rows

        # Projection
        result = []
        for row in filtered_rows:
            projected = {field: row[field] for field in select_fields}
            result.append(projected)

        return result
