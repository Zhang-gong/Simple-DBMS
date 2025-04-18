import sys
import os
from sqlglot import exp
from sqlglot.expressions import Expression, Column, EQ, Literal, Where
from typing import List, Dict, Any
from catalog.table import Table  # import your Table class

class Executor:
    """
    Executor runs queries defined by an AST on registered Table instances.
    """

    def __init__(self, schema):
        """
        Initialize the executor with a schema object.
        """
        self.schema = schema

    def execute(self, ast):
        """
        Execute a SQL AST on the registered tables.

        Parameters:
            ast (Expression): Parsed SQL AST.

        Returns:
            List[Dict[str, Any]]: Result rows.
        """

        #create
        # Check if the AST is a SELECT statement
        if isinstance(ast, exp.Create):
            self._execute_create( ast)
        elif isinstance(ast, exp.Select):
            self._execute_select(ast)

    def _execute_create(self, ast: exp.Create):
        """
        Execute a CREATE TABLE statement with primary key support.
        """
        table_name = ast.this.this.this.name
        columns = []
        primary_keys = []

        for column_def in ast.this.expressions:
            if isinstance(column_def, exp.ColumnDef):
                col_name = column_def.name
                columns.append(col_name)

                # 检查是否有 PRIMARY KEY 附加约束
                constraints = column_def.args.get("constraints") or []
                for cons in constraints:
                    if isinstance(cons.kind, exp.PrimaryKeyColumnConstraint):
                        primary_keys.append(col_name)

            elif isinstance(column_def, exp.Constraint):  # 表级 PRIMARY KEY
                if isinstance(column_def.this, exp.PrimaryKey):
                    pk_cols = [col.name for col in column_def.expressions]
                    primary_keys.extend(pk_cols)

        if self.schema.has_table(table_name):
            raise Exception(f"Table '{table_name}' already exists.")

        table = Table(table_name, columns, primary_key=primary_keys)
        self.schema.create_table(table)

        print(f"✅ Table '{table_name}' created with columns {columns}, primary key: {primary_keys}")

        #select
    def _execute_select(self, ast: exp.Select):

        # Get table name from AST
        from_expr = ast.args.get("from")
        table_expr = from_expr.this
        table_name = table_expr.name

        if table_name not in self.schema.tables:
            raise ValueError(f"Table '{table_name}' not found.")

        table_obj = self.schema.tables[table_name]
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
