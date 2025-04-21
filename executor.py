import sys
import os
from sqlglot import exp
from sqlglot.expressions import Expression, Column, EQ, Literal, Where
from typing import List, Dict, Any
from catalog.table import Table, ForeignKey  # import your Table class
from itertools import product
from optimizer import choose_join_strategy, extract_join_keys, sort_merge_join

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
            if isinstance(ast.this, exp.Index):
                return self._execute_build_index(ast)
            elif isinstance(ast.this.this, exp.Table):
                self._execute_create(ast)
            else:
                raise ValueError(f"Unsupported CREATE statement: {ast}")
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




    def _apply_order_by(self, rows: list[dict], order_exprs_node: exp.Order) -> list[dict]:
        if not order_exprs_node:
            return rows

        for order_item in reversed(order_exprs_node.expressions):  # exp.Ordered list
            expr = order_item.this
            is_desc = order_item.args.get("desc", False) is True

            if isinstance(expr, exp.Column):
                col_name = expr.output_name
                table_prefix = expr.table
                key = f"{table_prefix}.{col_name}" if table_prefix else col_name

                def key_func(row):
                    if key in row:
                        return row[key]
                    for k in row:
                        if k.endswith(f".{col_name}"):
                            return row[k]
                    # fallback value to not crash sort
                    return float("-inf") if is_desc else float("inf")

                rows.sort(key=key_func, reverse=is_desc)

        return rows








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
                key = col_expr.table + "." + col_name if col_expr.table else col_name
            elif isinstance(col_expr, exp.Func):  # or exp.Sum / exp.Count etc.
                func_name = col_expr.sql_name().lower()
                col_name = col_expr.this.name
                key = f"{func_name}({col_name})"
            else:
                key = col_expr.name


            # Get the right-hand value
            val = condition.expression.this
            try:
                val = int(val)
            except:
                val = str(val)

            if key in row:
                row_val = row[key]
            else:
                # fallback: support suffix and unqualified aggregate keys
                row_val = next((v for k, v in row.items() if k.endswith(f".{key}") or key.lower() in k.lower()), None)


            if isinstance(condition, exp.EQ): return row_val == val
            if isinstance(condition, exp.NEQ): return row_val != val
            if isinstance(condition, exp.GT): return row_val > val
            if isinstance(condition, exp.GTE): return row_val >= val
            if isinstance(condition, exp.LT): return row_val < val
            if isinstance(condition, exp.LTE): return row_val <= val

        raise NotImplementedError(f"Unsupported condition type: {type(condition)}")









    def _apply_group_by(self, rows: list[dict], group_exprs: list[exp.Expression]) -> dict:
        """
        Group rows by column(s) specified in GROUP BY.

        Returns:
            dict[str, list[dict]]: key = group string, value = list of rows in that group
        """
        if not group_exprs:
            return {"__ALL__": rows}  # no grouping specified

        grouped = {}

        for row in rows:
            keys = []
            for expr in group_exprs:
                if isinstance(expr, exp.Column):
                    col_name = expr.output_name
                    table_prefix = expr.table
                    key = f"{table_prefix}.{col_name}" if table_prefix else col_name

                    if key in row:
                        val = row[key]
                    else:
                        val = next((v for k, v in row.items() if k.endswith(f".{col_name}")), None)

                    keys.append(str(val))

            group_key = "|".join(keys)
            grouped.setdefault(group_key, []).append(row)

        return grouped









    def _apply_aggregations(grouped_rows: dict[str, list[dict]], expressions: list[exp.Expression]) -> list[dict]:
        """
        Given grouped rows and SELECT expressions, apply aggregation functions per group.

        Parameters:
            grouped_rows: dict of group_key -> list of row dicts
            expressions: SELECT clause expressions

        Returns:
            list[dict]: one row per group with computed aggregates
        """
        results = []

        for group_key, rows in grouped_rows.items():
            result_row = {}

            for expr in expressions:
                alias = expr.alias if isinstance(expr, exp.Alias) else None

                # Find aggregate function
                agg = expr.this if isinstance(expr, exp.Alias) else expr

                if isinstance(agg, exp.Func):
                    func_name = agg.sql_name().upper()


                    if func_name == "COUNT":
                        val = len(rows)
                        col_name = alias or "count"

                    elif func_name == "SUM":
                        col_expr = agg.this
                        col_name_raw = col_expr.name
                        val = sum(row.get(k, 0) for row in rows for k in row if k.endswith(f".{col_name_raw}"))
                        col_name = alias or f"sum({col_name_raw})"

                    elif func_name == "MAX":
                        col_expr = agg.this
                        col_name_raw = col_expr.name
                        val = max((row[k] for row in rows for k in row if k.endswith(f".{col_name_raw}")), default=None)
                        col_name = alias or f"max({col_name_raw})"

                    elif func_name == "MIN":
                        col_expr = agg.this
                        col_name_raw = col_expr.name
                        val = min((row[k] for row in rows for k in row if k.endswith(f".{col_name_raw}")), default=None)
                        col_name = alias or f"min({col_name_raw})"

                    else:
                        raise NotImplementedError(f"Unsupported aggregation function: {func_name}")

                    result_row[col_name] = val

                elif isinstance(agg, exp.Column):
                    # Grouping key (non-aggregated)
                    col_name = agg.output_name
                    for k in rows[0]:
                        if k.endswith(f".{col_name}"):
                            result_row[alias or col_name] = rows[0][k]
                            break

            results.append(result_row)

        return results













    def _apply_limit(self, rows: list[dict], limit_expr: exp.Limit) -> list[dict]:
        """
        Apply LIMIT clause to result set.
        """
        if not limit_expr:
            return rows

        try:
            limit_value = int(limit_expr.expression.name or limit_expr.expression.this)
            return rows[:limit_value]
        except Exception as e:
            raise ValueError(f"Invalid LIMIT value: {limit_expr}")














    def _apply_distinct(self, rows: list[dict], distinct_flag: Any) -> list[dict]:
        """
        Apply DISTINCT to remove duplicate rows.
        """
        if not distinct_flag:
            return rows

        seen = set()
        unique_result = []
        for row in rows:
            key = tuple(sorted(row.items()))
            if key not in seen:
                seen.add(key)
                unique_result.append(row)

        return unique_result















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
        print(f"before delete:", dict(table.indexes[table.primary_key]))
        where_expr = ast.args.get("where")
        if where_expr:
            matching_rows = self._apply_where_clause(table.rows, where_expr) #delete matching_rows

            row_ids_to_delete = [
                i for i, row in enumerate(table.rows) if row in matching_rows
            ]

            # Check foreign key constraints for the rows to be deleted
            for row in matching_rows:
                self.check_foreign_key_constraints_delete(table_name, row)

            for row_id in row_ids_to_delete:
                row = table.rows[row_id]
                for col, index in table.indexes.items():
                    if index is not None:
                        val = row[col]
                        # 若该值正好是这个 row_id，则删除（假设唯一索引）
                        if index.get(val) == row_id:
                            del index[val]

            table.rows = [row for row in table.rows if row not in matching_rows]
        else:
            for col, index in table.indexes.items():
                if index is not None:
                    index.clear()
            table.rows.clear()

        print(f"after delete:", dict(table.indexes[table.primary_key]))

        table.rebuild_indexes()
        print(f"after rebuild:", dict(table.indexes[table.primary_key]))
        deleted_count = original_count - len(table.rows)
        self.schema.save()
        print(f"🗑️ Deleted {deleted_count} row(s) from '{table_name}'.")













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
                    child = self.schema.get_table(child_table_name)
                    for crow in child.select_all():
                        if crow[fk.local_col] == old_pk:
                            raise ValueError(
                                f"Cannot update primary key {pk_col}={old_pk!r} in '{table_name}': "
                                f"still referenced by '{child_table_name}.{fk.local_col}'"
                            )

            # 4b. 外键列更新检查（确保新值在父表中存在）
            for parent_table, fk_list in self.schema.referenced_by.items():
                for child_table, fk in fk_list:
                    if child_table == table_name and fk.local_col in updates:
                        new_val = updates[fk.local_col]
                        parent = self.schema.tables[parent_table]
                        # 父表找不到对应值就报错
                        if not any(r[fk.ref_col] == new_val for r in parent.select_all()):
                            raise ValueError(
                                f"Foreign key violation: no '{parent_table}.{fk.ref_col}' = {new_val!r}"
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

        row_sets = [table.select_all() for _, table in table_objs]
        if len(table_objs) > 2:
            print("❌ Error: SELECT queries with more than 2 tables are not supported yet.")
            return []

        if len(table_objs) == 2 and join_exprs:
            _, left_table = table_objs[0]
            _, right_table = table_objs[1]
            left_rows = left_table.select_all()
            right_rows = right_table.select_all()

            on_condition = join_exprs[0].args.get("on")
            if not on_condition:
                raise ValueError("JOIN missing ON condition")

            strategy = choose_join_strategy(left_rows, right_rows, on_condition)

            if strategy == "sort_merge":
                left_key, right_key = extract_join_keys(on_condition)
                raw_combinations = sort_merge_join(left_rows, right_rows, left_key, right_key)
            else:
                from itertools import product
                raw_combinations = list(product(left_rows, right_rows))
                left_key, right_key = extract_join_keys(on_condition)
                raw_combinations = [
                    (l, r) for l, r in raw_combinations
                    if l[left_key] == r[right_key]
                ]
        else:
            from itertools import product
            raw_combinations = list(product(*row_sets))

        combined_rows = []
        for combo in raw_combinations:
            merged = {}
            for (alias, _), row in zip(table_objs, combo):
                for k, v in row.items():
                    merged[f"{alias}.{k}"] = v
            combined_rows.append(merged)

        where_expr = ast.args.get("where")
        if where_expr:
            combined_rows = [row for row in combined_rows if self._evaluate_condition(row, where_expr.this)]

        combined_rows = self._apply_order_by(combined_rows, ast.args.get("order"))

        expressions = ast.args.get("expressions", [])
        if not expressions:
            raise ValueError("No expressions in SELECT clause.")

        group_exprs = ast.args.get("group")
        if group_exprs:
            grouped = self._apply_group_by(combined_rows, group_exprs)
            result = Executor._apply_aggregations(grouped, expressions)

            # ✅ HAVING clause filtering
            having_expr = ast.args.get("having")
            if having_expr:
                result = [row for row in result if self._evaluate_condition(row, having_expr.this)]

        else:
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
                        key = f"{table_prefix}.{col_name}" if table_prefix else None

                        val = None
                        if key and key in row:
                            val = row[key]
                        else:
                            for k in row:
                                if k.endswith(f".{col_name}"):
                                    val = row[k]
                                    break

                        output_key = alias or col_name
                        projected[output_key] = val

                    result.append(projected)

        result = self._apply_distinct(result, ast.args.get("distinct"))
        result = self._apply_limit(result, ast.args.get("limit"))
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


    def _execute_build_index(self, ast):
        """
        BUILD INDEX ON table(column)
        """
        table_name = ast.this.args['table'].name  # e.g. students
        column_name = ast.args['this'].args['params'].args['columns'][0].args['this'].name # e.g. age

        table = self.schema.get_table(table_name)

        if column_name not in table.column_names:
            raise ValueError(f"Column '{column_name}' does not exist in table '{table_name}'")

        if table.indexes[column_name] is None:
            # ✅ Build new index
            table.indexes[column_name] = OOBTree()
            print(f"⚙️  Created new index on {table_name}.{column_name}")
            print(f"after create index:", dict(table.indexes[table.primary_key]))
        else:
            print(f"🔄 Rebuilding existing index on {table_name}.{column_name}")

        # ✅ 建好新 index 或已有 index 后，都重建内容
        table.rebuild_indexes()