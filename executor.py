import sys
import os
from sqlglot import exp
from sqlglot.expressions import Expression, Column, EQ, Literal, Where
from typing import List, Dict, Any
from catalog.table import Table, ForeignKey  # import your Table class

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

        if isinstance(ast, exp.Create):
            self._execute_create(ast)
        elif isinstance(ast, exp.Select):
            return self._execute_select(ast)
        elif isinstance(ast, exp.Insert):
            self._execute_insert(ast)
        elif isinstance(ast, exp.Drop):
            self._execute_drop(ast)
        elif isinstance(ast, exp.Delete):
            self._execute_delete(ast)
        elif isinstance(ast, exp.Update):
            self._execute_update(ast)







    def _apply_where_clause(self, rows: list[dict], where_expr: exp.Expression) -> list[dict]:
        """
        Apply WHERE clause with support for =, !=, <, <=, >, >=, AND, OR.
        """
        condition = where_expr.this
        return [row for row in rows if self._evaluate_condition(row, condition)]









    def _evaluate_condition(self, row: dict, condition: exp.Expression) -> bool:
        """
        Recursively evaluate a WHERE condition on a single row.
        Supports: =, !=, <, <=, >, >=, AND, OR
        """
        if isinstance(condition, exp.And):
            return self._evaluate_condition(row, condition.left) and self._evaluate_condition(row, condition.right)
        elif isinstance(condition, exp.Or):
            return self._evaluate_condition(row, condition.left) or self._evaluate_condition(row, condition.right)

        elif isinstance(condition, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
            col = condition.this.name
            val = condition.expression.this

            try:
                val = int(val)
            except:
                val = str(val)

            row_val = row.get(col)

            if isinstance(condition, exp.EQ): return row_val == val
            if isinstance(condition, exp.NEQ): return row_val != val
            if isinstance(condition, exp.GT): return row_val > val
            if isinstance(condition, exp.GTE): return row_val >= val
            if isinstance(condition, exp.LT): return row_val < val
            if isinstance(condition, exp.LTE): return row_val <= val

        else:
            raise NotImplementedError(f"Unsupported condition type: {type(condition)}")









    def _execute_create(self, ast: exp.Create):
        """
        Execute a CREATE TABLE statement with primary key and type support.
        """
        table_name = ast.this.this.this.name
        columns = []
        primary_keys = []
        foreign_keys = []

        for column_def in ast.this.expressions:
            if isinstance(column_def, exp.ColumnDef):
                col_name = column_def.name
                col_type = column_def.args["kind"].sql().upper()

                if col_type not in ["INT", "TEXT"]:
                    raise ValueError(f"Unsupported column type: {col_type}")

                columns.append({"name": col_name, "type": col_type})

                constraints = column_def.args.get("constraints") or []
                for cons in constraints:
                    if isinstance(cons.kind, exp.PrimaryKeyColumnConstraint):
                        primary_keys.append(col_name)
            #table-level primary key
            elif isinstance(column_def, exp.Constraint):
                if isinstance(column_def.this, exp.PrimaryKey):
                    pk_cols = [col.name for col in column_def.expressions]
                    primary_keys.extend(pk_cols)

            #check if the column is a foreign key

            elif isinstance(column_def, exp.ForeignKey):
                local_cols = [col.name for col in column_def.expressions]
                ref_table = column_def.args['reference'].this.this.name
                ref_cols = column_def.args['reference'].this.expressions[0].name

                if len(local_cols) != 1:
                    raise ValueError("Only single-column foreign keys are supported for now.")

                # referenced table and column(s)
                fk = ForeignKey(
                    local_col=local_cols[0],
                    ref_table=ref_table,
                    ref_col=ref_cols[0],
                    policy="RESTRICT"  # 可扩展后支持 ast.args["on_delete"]
                )
                foreign_keys.append(fk)

        if self.schema.has_table(table_name):
            raise Exception(f"Table '{table_name}' already exists.")

        if len(primary_keys) != 1:
            raise Exception("You must define exactly one primary key.")

        table = Table(table_name, columns, primary_key=primary_keys[0])
        self.schema.create_table(table)

        for fk in foreign_keys:
            self.schema.referenced_by.setdefault(fk.ref_table, []).append((table_name, fk))

        print(f"✅ Table '{table_name}' created with columns {columns}, primary key: {primary_keys[0]}")
        self.schema.save()
        print(f"Table '{table_name}' saved to schema '{self.schema.name}' directory.")



    def _execute_delete(self, ast: exp.Delete):
        """
        Execute a DELETE FROM statement.
        Supports:
            DELETE FROM table;
            DELETE FROM table WHERE column = value;
        """
        table_name = ast.this.this.name

        if table_name not in self.schema.tables:
            raise ValueError(f"Table '{table_name}' does not exist.")

        table = self.schema.tables[table_name]
        original_count = len(table.rows)

        where_expr = ast.args.get("where")
        if where_expr:
            matching_rows = self._apply_where_clause(table.rows, where_expr)
            table.rows = [row for row in table.rows if row not in matching_rows]
        else:
            table.rows.clear()

        deleted_count = original_count - len(table.rows)
        self.schema.save()
        print(f"🗑️ Deleted {deleted_count} row(s) from '{table_name}'.")















    def _execute_select(self, ast: exp.Select):
        from_expr = ast.args.get("from")
        table_expr = from_expr.this
        table_name = table_expr.name

        if table_name not in self.schema.tables:
            raise ValueError(f"Table '{table_name}' not found.")

        table_obj = self.schema.tables[table_name]
        all_rows = table_obj.select_all()

        expressions = ast.args["expressions"]
        if len(expressions) == 1 and isinstance(expressions[0], exp.Star):
            select_fields = table_obj.column_names
        else:
            select_fields = [col.name for col in expressions]

        where_expr = ast.args.get("where")
        if where_expr:
            filtered_rows = self._apply_where_clause(all_rows, where_expr)
        else:
            filtered_rows = all_rows

        result = []
        for row in filtered_rows:
            projected = {field: row[field] for field in select_fields}
            result.append(projected)
        
        return result









    def _execute_insert(self, ast: exp.Insert):
        """
        Execute an INSERT INTO statement.
        Example: INSERT INTO users (id, name, age) VALUES (1, 'Alice', 20);
        """
        table_name = ast.this.this.name  # get table name string

        if table_name not in self.schema.tables:
            raise ValueError(f"Table '{table_name}' does not exist.")

        table = self.schema.tables[table_name]

        # Extract columns from INSERT statement (optional)
        col_exprs = ast.args.get("columns")
        if col_exprs:
            column_names = [col.name for col in col_exprs]
        else:
            column_names = table.column_names

        # ✅ FIX: extract values from the 'expression' node (which contains tuples)
        values_expr = ast.args["expression"].expressions
        for tuple_expr in values_expr:
            value_exprs = tuple_expr.expressions
            values = [val.this for val in value_exprs]

            if len(values) != len(column_names):
                raise ValueError("Number of values does not match number of columns.")

            # Convert to correct types
            row = {}
            for i, col in enumerate(column_names):
                declared_type = next(c["type"] for c in table.columns if c["name"] == col)
                if declared_type == "INT":
                    values[i] = int(values[i])
                elif declared_type == "TEXT":
                    values[i] = str(values[i])
                row[col] = values[i]

            table.insert(row)

        print(f"✅ Inserted row(s) into '{table_name}'")
        self.schema.save()



    def _execute_drop(self, ast: exp.Drop):
        """
        Execute a DROP TABLE statement.
        Example: DROP TABLE users;
        """
        table_name = ast.this.this.name  # Get the table name as a string

        if not self.schema.has_table(table_name):
            raise ValueError(f"Table '{table_name}' does not exist.")

        # 1. Remove from memory
        self.schema.drop_table(table_name)

        # 2. Delete folder from disk
        table_path = os.path.join("data", table_name)
        if os.path.isdir(table_path):
            import shutil
            shutil.rmtree(table_path)

        print(f"🗑️ Table '{table_name}' has been dropped.")








    
    def _execute_update(self, ast: exp.Update):
        """
        Execute an UPDATE statement.
        Supports:
            UPDATE table SET column = value;
            UPDATE table SET column = value WHERE column = value;
        """
        table_name = ast.this.name

        if table_name not in self.schema.tables:
            raise ValueError(f"Table '{table_name}' does not exist.")

        table = self.schema.tables[table_name]

        # 1. Parse SET clause
        assignments = ast.expressions
        updates = {}
        for assign in assignments:
            col_name = assign.this.name
            value = assign.expression.this
            updates[col_name] = value

        # 2. Validate columns and convert types
        for col, val in updates.items():
            col_type = next(c["type"] for c in table.columns if c["name"] == col)
            if col_type == "INT":
                updates[col] = int(val)
            elif col_type == "TEXT":
                updates[col] = str(val)

        # 3. Apply WHERE (optional)
        where_expr = ast.args.get("where")
        if where_expr:
            target_rows = self._apply_where_clause(table.rows, where_expr)
        else:
            target_rows = table.rows

        # 4. Perform updates
        update_count = 0
        for row in table.rows:
            if row in target_rows:
                for col, val in updates.items():
                    row[col] = val
                update_count += 1

        self.schema.save()
        print(f"📝 Updated {update_count} row(s) in '{table_name}'.")


    






    



