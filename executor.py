import sys
import os
from sqlglot import exp
from sqlglot.expressions import Expression, Column, EQ, Literal, Where
from typing import List, Dict, Any
from catalog.table import Table, ForeignKey  # import your Table class
from itertools import product

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
        Evaluate WHERE condition. Supports =, !=, <, >, <=, >=, AND, OR.
        """
        if isinstance(condition, exp.And):
            return self._evaluate_condition(row, condition.left) and self._evaluate_condition(row, condition.right)

        if isinstance(condition, exp.Or):
            return self._evaluate_condition(row, condition.left) or self._evaluate_condition(row, condition.right)

        if isinstance(condition, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
            # Get the column side of the condition
            col_expr = condition.this
            if isinstance(col_expr, exp.Column):
                col_name = col_expr.output_name
                table_prefix = col_expr.table
                key = f"{table_prefix}.{col_name}" if table_prefix else col_name
            else:
                key = col_expr.name

            # Get the right-hand value
            val = condition.expression.this
            try:
                val = int(val)
            except:
                val = str(val)

            row_val = row.get(key)

            if isinstance(condition, exp.EQ): return row_val == val
            if isinstance(condition, exp.NEQ): return row_val != val
            if isinstance(condition, exp.GT): return row_val > val
            if isinstance(condition, exp.GTE): return row_val >= val
            if isinstance(condition, exp.LT): return row_val < val
            if isinstance(condition, exp.LTE): return row_val <= val

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
                ref_cols = column_def.args['reference'].this.expressions[0].name #list

                if len(local_cols) != 1:
                    raise ValueError("Only single-column foreign keys are supported for now.")

                if not self.schema.has_table(ref_table):
                    raise ValueError(f"Referenced table '{ref_table}' does not exist.")
                ref_table_obj = self.schema.get_table(ref_table)

                # ✅ 检查引用列是否为主键
                if ref_cols != ref_table_obj.primary_key:
                    raise ValueError(
                        f"Foreign key must reference the PRIMARY KEY of table '{ref_table}'. "
                        f"But '{ref_cols}' is not the primary key (expected '{ref_table_obj.primary_key}')."
                    )
                # referenced table and column(s)
                fk = ForeignKey(
                    local_col=local_cols[0],
                    ref_table=ref_table,
                    ref_col=ref_cols,
                    policy="RESTRICT"  #  ast.args["on_delete"]
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

        table = self.schema.get_table(table_name)
        original_count = len(table.rows)

        where_expr = ast.args.get("where")
        if where_expr:
            matching_rows = self._apply_where_clause(table.rows, where_expr) #delete matching_rows
            for row in matching_rows:
                self.check_foreign_key_constraints_delete(table_name, row)
            table.rows = [row for row in table.rows if row not in matching_rows]
        else:
            table.rows.clear()

        deleted_count = original_count - len(table.rows)
        self.schema.save()
        print(f"🗑️ Deleted {deleted_count} row(s) from '{table_name}'.")















    def _execute_select(self, ast: exp.Select):
        from_clause = ast.args.get("from")

        from_exprs = []
        if isinstance(from_clause, exp.From):
            if isinstance(from_clause.this, exp.Table):
                # single table case
                from_exprs = [from_clause.this]
            elif hasattr(from_clause, "expressions") and from_clause.expressions:
                # multiple tables
                from_exprs = from_clause.expressions
            else:
                raise ValueError("Invalid FROM clause structure")
        else:
            raise ValueError("Missing FROM clause")

        table_objs = []
        alias_map = {}

        for table_expr in from_exprs:
            if isinstance(table_expr, exp.Table):
                # ✅ precisely correct extraction:
                table_name = table_expr.this.this
                alias = table_expr.alias_or_name
            else:
                raise ValueError(f"Unsupported FROM clause expression: {type(table_expr)}")

            if table_name not in self.schema.tables:
                raise ValueError(f"Table '{table_name}' not found.")

            alias_map[alias] = table_name
            table_objs.append((alias, self.schema.tables[table_name]))

        row_sets = [table.select_all() for _, table in table_objs]
        all_combinations = list(product(*row_sets))


        prefix_keys = len(table_objs) > 1 or any(alias != table.name for alias, table in table_objs)

        combined_rows = []
        for combo in all_combinations:
            merged = {}
            for (alias, _), row in zip(table_objs, combo):
                for k, v in row.items():
                    key = f"{alias}.{k}" if prefix_keys else k
                    merged[key] = v
            combined_rows.append(merged)

        # WHERE clause
        where_expr = ast.args.get("where")
        if where_expr:
            combined_rows = [row for row in combined_rows if self._evaluate_condition(row, where_expr.this)]

        # SELECT projection
        expressions = ast.args["expressions"]
        result = []

        if len(expressions) == 1 and isinstance(expressions[0], exp.Star):
            result = combined_rows
        else:
            for row in combined_rows:
                projected = {}
                for expr in expressions:
                    alias = expr.alias if isinstance(expr, exp.Alias) else None
                    col = expr.find(exp.Column)

                    if not col:
                        raise ValueError(f"Could not resolve column in SELECT: {expr}")

                    col_name = col.output_name
                    table_prefix = col.table
                    key = f"{table_prefix}.{col_name}" if table_prefix or prefix_keys else col_name
                    output_key = alias or key
                    projected[output_key] = row.get(key, None)

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
            self.check_foreign_key_constraints(table_name, row)
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

        # Check primary/foreign key constraints for the rows to be updated

        # 4. PK/FK constraint checks BEFORE applying changes
        pk_col = table.primary_key

        for row in target_rows:
            old_pk = row[pk_col]

            # 4a. 主键更新检查
            if pk_col in updates:
                new_pk = updates[pk_col]
                # 唯一性检查
                if new_pk != old_pk and new_pk in table.rows:
                    raise ValueError(f"Duplicate primary key value {new_pk!r} in '{table_name}'")
                # 外表引用检查
                for child_table_name, fk in self.schema.referenced_by.get(table_name, []):
                    child = self.schema.tables[child_table_name]
                    for crow in child.select_all():
                        if crow[fk.local_col] == old_pk:
                            raise ValueError(
                                f"Cannot update primary key {old_pk!r} in '{table_name}': "
                                f"still referenced by '{child_table_name}.{fk.local_col}'"
                            )

            # 4b. 外键列更新检查（确保新值在父表中存在）
            for fk in getattr(table, "foreign_keys", []):
                if fk.local_col in updates:
                    new_val = updates[fk.local_col]
                    ref = self.schema.tables[fk.ref_table]
                    if not any(r[fk.ref_col] == new_val for r in ref.select_all()):
                        raise ValueError(
                            f"Foreign key violation: no '{fk.ref_table}.{fk.ref_col}' = {new_val!r}"
                        )

        # 4. Perform updates
        update_count = 0
        for row in table.rows:
            if row in target_rows:
                for col, val in updates.items():
                    row[col] = val
                update_count += 1

        self.schema.save()
        print(f"📝 Updated {update_count} row(s) in '{table_name}'.")
















    def _execute_select(self, ast: exp.Select):
        from_clause = ast.args.get("from")

        if not isinstance(from_clause, exp.From):
            raise ValueError("Missing FROM clause")

        table_objs = []
        alias_map = []

        # First table (FROM clause)
        first_table_expr = from_clause.this
        if isinstance(first_table_expr, exp.Table):
            table_name = first_table_expr.this.this
            alias = first_table_expr.alias_or_name
            if table_name not in self.schema.tables:
                raise ValueError(f"Table '{table_name}' not found.")
            alias_map.append(alias)
            table_objs.append((alias, self.schema.tables[table_name]))
        else:
            raise ValueError(f"Unsupported FROM expression: {type(first_table_expr)}")

        # JOINed tables
        join_exprs = ast.args.get("joins", [])
        for join in join_exprs:
            join_table_expr = join.this
            if isinstance(join_table_expr, exp.Table):
                table_name = join_table_expr.this.this
                alias = join_table_expr.alias_or_name
                if table_name not in self.schema.tables:
                    raise ValueError(f"Table '{table_name}' not found.")
                alias_map.append(alias)
                table_objs.append((alias, self.schema.tables[table_name]))
            else:
                raise ValueError(f"Unsupported JOIN expression: {type(join_table_expr)}")

        # Get rows
        row_sets = [table.select_all() for _, table in table_objs]

        # Cartesian product
        raw_combinations = list(product(*row_sets))

        # Apply JOIN ON conditions
        on_conditions = [join.args.get("on") for join in join_exprs]
        filtered_combinations = []

        for combo in raw_combinations:
            merged = {}
            for (alias, _), row in zip(table_objs, combo):
                for k, v in row.items():
                    merged[f"{alias}.{k}"] = v

            passed = True
            for condition in on_conditions:
                if condition and not self._evaluate_condition(merged, condition):
                    passed = False
                    break

            if passed:
                filtered_combinations.append(combo)

        all_combinations = filtered_combinations

        prefix_keys = len(table_objs) > 1 or any(alias != table.name for alias, table in table_objs)

        combined_rows = []
        for combo in all_combinations:
            merged = {}
            for (alias, _), row in zip(table_objs, combo):
                for k, v in row.items():
                    key = f"{alias}.{k}" if prefix_keys else k
                    merged[key] = v
            combined_rows.append(merged)

        # WHERE clause
        where_expr = ast.args.get("where")
        if where_expr:
            combined_rows = [row for row in combined_rows if self._evaluate_condition(row, where_expr.this)]

        # SELECT projection
        expressions = ast.args["expressions"]
        result = []

        if len(expressions) == 1 and isinstance(expressions[0], exp.Star):
            result = combined_rows
        else:
            for row in combined_rows:
                projected = {}
                for expr in expressions:
                    if isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star):
                        table_prefix = expr.table
                        for k, v in row.items():
                            if k.startswith(f"{table_prefix}."):
                                projected[k] = v
                        continue

                    alias = expr.alias if isinstance(expr, exp.Alias) else None
                    col = expr.find(exp.Column)

                    if not col:
                        raise ValueError(f"Could not resolve column in SELECT: {expr}")

                    col_name = col.output_name
                    table_prefix = col.table
                    key = f"{table_prefix}.{col_name}" if table_prefix or prefix_keys else col_name
                    output_key = alias or key
                    projected[output_key] = row.get(key, None)

                result.append(projected)

        return result











    def check_foreign_key_constraints(self, table_name:str, row: dict):
        """
        Check if the row satisfies foreign key constraints.
        """
        for ref_table_name, fk_list in self.schema.referenced_by.items():
            for referencing_table, fk in fk_list:
                # check the referencing table
                if referencing_table != table_name:
                    continue

                # check the local column
                if fk.local_col not in row:
                    continue

                value_to_check = row[fk.local_col]

                # 拿到被引用的表和列
                ref_table = self.schema.get_table(fk.ref_table)
                ref_col = fk.ref_col

                # 检查引用值是否在主表中存在
                match_found = any(
                    ref_row.get(ref_col) == value_to_check
                    for ref_row in ref_table.select_all()
                )

                if not match_found:
                    raise ValueError(
                        f"Foreign key violation: value '{value_to_check}' in column '{fk.local_col}' "
                        f"not found in {fk.ref_table}.{ref_col}"
                    )

    def check_foreign_key_constraints_delete(self, table_name: str, row: dict):
        """
        Check if the row in `table_name` can be safely deleted,
        i.e., its primary key is not referenced as a foreign key by any other table.
        """
        # 当前表主键值
        current_table = self.schema.get_table(table_name)
        pk_col = current_table.primary_key
        pk_val = row[pk_col]

        # 在 schema.referenced_by 中查找是否有表引用了当前表
        if table_name not in self.schema.referenced_by:
            return  # 没有任何表引用当前表，可以安全删除

        for referencing_table_name, fk in self.schema.referenced_by[table_name]:
            referencing_table = self.schema.get_table(referencing_table_name)
            local_col = fk.local_col

            for ref_row in referencing_table.select_all():
                if ref_row.get(local_col) == pk_val:
                    if fk.policy == "RESTRICT":
                        raise ValueError(
                            f"Cannot delete from '{table_name}': primary key value '{pk_val}' "
                            f"is still referenced by '{referencing_table_name}.{local_col}' (RESTRICT)"
                        )
                    elif fk.policy == "CASCADE":
                        # 这里只是检查，可以返回一个标志或者做递归删除
                        # 你也可以在真正的 delete 逻辑里执行：
                        # referencing_table.delete(lambda r: r[local_col] == pk_val)
                        continue
