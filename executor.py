# executor.py

import sys
import os
from sqlglot import exp
from sqlglot.expressions import Expression, Column, EQ, Literal, Where
from typing import List, Dict, Any
from catalog.table import Table, ForeignKey
from itertools import product
import optimizer

class Executor:
    """
    Executes SQL ASTs against the in-memory schema and tables.
    """

    def __init__(self, schema):
        """
        Initialize the executor with a schema instance.
        """
        self.schema = schema

    def execute(self, ast):
        """
        Dispatch execution based on AST node type.

        Parameters:
            ast (Expression): Parsed SQL AST.

        Returns:
            List[Dict[str, Any]] or None: Result rows for queries, or None.
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
        Filter rows based on a WHERE expression (supports =, !=, <, <=, >, >=, AND, OR).

        Returns:
            list[dict]: Rows satisfying the condition.
        """
        condition = where_expr.this
        return [row for row in rows if self._evaluate_condition(row, condition)]

    def _apply_order_by(self, rows: list[dict], order_exprs_node: exp.Order) -> list[dict]:
        """
        Sort rows according to ORDER BY expressions.

        Parameters:
            rows (list[dict]): Rows to sort.
            order_exprs_node (exp.Order): AST node for ORDER BY.

        Returns:
            list[dict]: Sorted rows.
        """
        if not order_exprs_node:
            return rows

        # Apply each ORDER BY item in reverse to achieve stable multi-column sort
        for order_item in reversed(order_exprs_node.expressions):
            expr = order_item.this
            is_desc = order_item.args.get("desc", False) is True

            if isinstance(expr, exp.Column):
                col_name = expr.output_name
                table_prefix = expr.table
                key = f"{table_prefix}.{col_name}" if table_prefix else col_name

                def key_func(row):
                    # Prefer fully qualified key if present
                    if key in row:
                        return row[key]
                    # Fallback: match by suffix
                    for k in row:
                        if k.endswith(f".{col_name}"):
                            return row[k]
                    # Final fallback to avoid sort errors
                    return float("-inf") if is_desc else float("inf")

                rows.sort(key=key_func, reverse=is_desc)

        return rows

    def _evaluate_condition(self, row: dict, condition: exp.Expression) -> bool:
        """
        Recursively evaluate a WHERE condition against a single row.

        Supports:
          - Parentheses
          - AND, OR
          - Comparison operators: =, !=, >, >=, <, <=

        Returns:
            bool: True if the row satisfies the condition.
        """
        if isinstance(condition, exp.Paren):
            return self._evaluate_condition(row, condition.this)

        if isinstance(condition, exp.And):
            return (self._evaluate_condition(row, condition.left)
                    and self._evaluate_condition(row, condition.right))

        if isinstance(condition, exp.Or):
            return (self._evaluate_condition(row, condition.left)
                    or self._evaluate_condition(row, condition.right))

        if isinstance(condition, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
            # Determine left-hand side column or function
            col_expr = condition.this
            if isinstance(col_expr, exp.Column):
                col_name = col_expr.output_name
                key = f"{col_expr.table}.{col_name}" if col_expr.table else col_name
            elif isinstance(col_expr, exp.Func):
                func_name = col_expr.sql_name().lower()
                col_name = col_expr.this.name
                key = f"{func_name}({col_name})"
            else:
                key = col_expr.name

            # Parse right-hand value
            val = condition.expression.this
            try:
                val = int(val)
            except:
                val = str(val)

            # Retrieve the actual row value
            row_val = row.get(key, None)
            if row_val is None:
                # Fallback: search by suffix or substring match
                row_val = next((v for k, v in row.items()
                                if k.endswith(f".{key}") or key.lower() in k.lower()),
                               None)

            # Perform comparison
            if isinstance(condition, exp.EQ):
                return row_val == val
            if isinstance(condition, exp.NEQ):
                return row_val != val
            if isinstance(condition, exp.GT):
                return row_val > val
            if isinstance(condition, exp.GTE):
                return row_val >= val
            if isinstance(condition, exp.LT):
                return row_val < val
            if isinstance(condition, exp.LTE):
                return row_val <= val

        raise NotImplementedError(f"Unsupported condition type: {type(condition)}")

    def _apply_group_by(self, rows: list[dict], group_exprs: list[exp.Expression]) -> dict:
        """
        Group rows based on GROUP BY columns.

        Returns:
            dict[str, list[dict]]: Mapping from group key to list of rows.
        """
        if not group_exprs:
            return {"__ALL__": rows}

        grouped = {}
        for row in rows:
            keys = []
            for expr in group_exprs:
                if isinstance(expr, exp.Column):
                    col_name = expr.output_name
                    prefix = expr.table
                    key = f"{prefix}.{col_name}" if prefix else col_name

                    val = row.get(key, None)
                    if val is None:
                        # Fallback: match by suffix
                        val = next((v for k, v in row.items() if k.endswith(f".{col_name}")), None)
                    keys.append(str(val))

            group_key = "|".join(keys)
            grouped.setdefault(group_key, []).append(row)

        return grouped

    @staticmethod
    def _apply_aggregations(grouped_rows: dict[str, list[dict]], expressions: list[exp.Expression]) -> list[dict]:
        """
        Apply aggregation functions (COUNT, SUM, MAX, MIN) and select expressions per group.

        Returns:
            list[dict]: One result row per group.
        """
        results = []
        for group_key, rows in grouped_rows.items():
            result_row = {}
            for expr in expressions:
                alias = expr.alias if isinstance(expr, exp.Alias) else None
                agg = expr.this if isinstance(expr, exp.Alias) else expr

                if isinstance(agg, exp.Func):
                    func_name = agg.sql_name().upper()
                    col_expr = agg.this
                    raw = col_expr.name

                    if func_name == "COUNT":
                        val = len(rows)
                        col_name = alias or "count"
                    elif func_name == "SUM":
                        val = sum(row[k] for row in rows for k in row if k.endswith(f".{raw}"))
                        col_name = alias or f"sum({raw})"
                    elif func_name == "MAX":
                        val = max((row[k] for row in rows for k in row if k.endswith(f".{raw}")), default=None)
                        col_name = alias or f"max({raw})"
                    elif func_name == "MIN":
                        val = min((row[k] for row in rows for k in row if k.endswith(f".{raw}")), default=None)
                        col_name = alias or f"min({raw})"
                    else:
                        raise NotImplementedError(f"Unsupported aggregation: {func_name}")

                    result_row[col_name] = val

                elif isinstance(agg, exp.Column):
                    # Preserve grouping key columns
                    col_name = agg.output_name
                    for k in rows[0]:
                        if k.endswith(f".{col_name}"):
                            result_row[alias or col_name] = rows[0][k]
                            break

            results.append(result_row)
        return results

    def _apply_limit(self, rows: list[dict], limit_expr: exp.Limit) -> list[dict]:
        """
        Enforce LIMIT clause on result rows.
        """
        if not limit_expr:
            return rows

        try:
            limit_value = int(limit_expr.expression.name or limit_expr.expression.this)
            return rows[:limit_value]
        except Exception:
            raise ValueError(f"Invalid LIMIT value: {limit_expr}")

    def _apply_distinct(self, rows: list[dict], distinct_flag: Any) -> list[dict]:
        """
        Apply DISTINCT to eliminate duplicate rows.
        """
        if not distinct_flag:
            return rows

        seen = set()
        unique = []
        for row in rows:
            key = tuple(sorted(row.items()))
            if key not in seen:
                seen.add(key)
                unique.append(row)
        return unique

    def _execute_create(self, ast: exp.Create):
        """
        Handle CREATE TABLE with support for primary keys and single-column foreign keys.
        """
        table_name = ast.this.this.this.name
        columns = []
        primary_keys = []
        foreign_keys = []

        for column_def in ast.this.expressions:
            if isinstance(column_def, exp.ColumnDef):
                # Column name and type
                col_name = column_def.name
                col_type = column_def.args["kind"].sql().upper()
                if col_type not in ["INT", "TEXT"]:
                    raise ValueError(f"Unsupported column type: {col_type}")
                columns.append({"name": col_name, "type": col_type})

                # Check for inline PRIMARY KEY constraint
                for cons in column_def.args.get("constraints") or []:
                    if isinstance(cons.kind, exp.PrimaryKeyColumnConstraint):
                        primary_keys.append(col_name)

            elif isinstance(column_def, exp.Constraint):
                # Table-level PRIMARY KEY
                if isinstance(column_def.this, exp.PrimaryKey):
                    pk_cols = [col.name for col in column_def.expressions]
                    primary_keys.extend(pk_cols)

            elif isinstance(column_def, exp.ForeignKey):
                # Single-column foreign key
                local_cols = [col.name for col in column_def.expressions]
                ref_table = column_def.args['reference'].this.this.name
                ref_cols = column_def.args['reference'].this.expressions[0].name

                if len(local_cols) != 1:
                    raise ValueError("Only single-column foreign keys are supported.")
                if not self.schema.has_table(ref_table):
                    raise ValueError(f"Referenced table '{ref_table}' does not exist.")
                ref_table_obj = self.schema.get_table(ref_table)

                # Ensure foreign key references the primary key
                if ref_cols != ref_table_obj.primary_key:
                    raise ValueError(
                        f"Foreign key must reference primary key of '{ref_table}', "
                        f"but '{ref_cols}' is not that key."
                    )

                fk = ForeignKey(
                    local_col=local_cols[0],
                    ref_table=ref_table,
                    ref_col=ref_cols,
                    policy="RESTRICT"
                )
                foreign_keys.append(fk)

        if self.schema.has_table(table_name):
            raise Exception(f"Table '{table_name}' already exists.")
        if len(primary_keys) != 1:
            raise Exception("Exactly one primary key must be defined.")

        # Create and register the table
        table = Table(table_name, columns, primary_key=primary_keys[0])
        self.schema.create_table(table)

        # Track foreign key relationships
        for fk in foreign_keys:
            self.schema.referenced_by.setdefault(fk.ref_table, []).append((table_name, fk))

        print(f"Table '{table_name}' created with columns {columns} and primary key {primary_keys[0]}")
        self.schema.save()
        print(f"Schema '{self.schema.name}' saved.")

    def _execute_delete(self, ast: exp.Delete):
        """
        Execute DELETE FROM with optional WHERE.
        """
        table_name = ast.this.this.name
        if table_name not in self.schema.tables:
            raise ValueError(f"Table '{table_name}' does not exist.")

        table = self.schema.get_table(table_name)
        original_count = len(table.rows)
        where_expr = ast.args.get("where")

        if where_expr:
            # Identify rows to delete
            matching = self._apply_where_clause(table.rows, where_expr)
            # Check foreign key constraints before deletion
            for row in matching:
                self.check_foreign_key_constraints_delete(table_name, row)

            # Remove index entries for deleted rows
            for row in matching:
                for col, index in table.indexes.items():
                    if index is not None:
                        val = row[col]
                        if index.get(val) == table.rows.index(row):
                            del index[val]

            # Physically delete rows
            table.rows = [r for r in table.rows if r not in matching]
        else:
            # Clear all data and indexes
            for index in table.indexes.values():
                if index is not None:
                    index.clear()
            table.rows.clear()

        table.rebuild_indexes()
        deleted_count = original_count - len(table.rows)
        self.schema.save()
        print(f"Deleted {deleted_count} row(s) from '{table_name}'")

    def _execute_insert(self, ast: exp.Insert):
        """
        Execute INSERT INTO ... VALUES (...) statements.
        """
        table_name = ast.this.this.name
        if table_name not in self.schema.tables:
            raise ValueError(f"Table '{table_name}' does not exist.")

        table = self.schema.tables[table_name]
        col_exprs = ast.args.get("columns")
        column_names = [c.name for c in col_exprs] if col_exprs else table.column_names

        # Extract tuple of values from AST
        values_expr = ast.args["expression"].expressions
        for tuple_expr in values_expr:
            values = [v.this for v in tuple_expr.expressions]
            if len(values) != len(column_names):
                raise ValueError("Value count does not match column count.")

            # Convert and assemble row
            row = {}
            for i, col in enumerate(column_names):
                declared = next(c["type"] for c in table.columns if c["name"] == col)
                if declared == "INT":
                    values[i] = int(values[i])
                else:
                    values[i] = str(values[i])
                row[col] = values[i]

            # Enforce foreign key constraints
            self.check_foreign_key_constraints(table_name, row)
            table.insert(row)

        print(f"Inserted {len(values_expr)} row(s) into '{table_name}'")
        self.schema.save()

    def _execute_drop(self, ast: exp.Drop):
        """
        Execute DROP TABLE statements.
        """
        table_name = ast.this.this.name
        if not self.schema.has_table(table_name):
            raise ValueError(f"Table '{table_name}' does not exist.")

        # Remove from memory and disk
        self.schema.drop_table(table_name)
        path = os.path.join("data", table_name)
        if os.path.isdir(path):
            import shutil
            shutil.rmtree(path)
        print(f"Table '{table_name}' dropped.")

    def _execute_update(self, ast: exp.Update):
        """
        Execute UPDATE ... SET ... [WHERE ...] statements.
        """
        table_name = ast.this.name
        if table_name not in self.schema.tables:
            raise ValueError(f"Table '{table_name}' does not exist.")

        table = self.schema.tables[table_name]

        # Parse assignments from SET clause
        updates = {assign.this.name: assign.expression.this for assign in ast.expressions}

        # Convert types based on schema
        for col, val in updates.items():
            col_type = next(c["type"] for c in table.columns if c["name"] == col)
            updates[col] = int(val) if col_type == "INT" else str(val)

        # Identify target rows
        where_expr = ast.args.get("where")
        target = (self._apply_where_clause(table.rows, where_expr)
                  if where_expr else table.rows)

        # 4a. Check primary key updates
        pk = table.primary_key
        for row in target:
            old_val = row[pk]
            if pk in updates:
                new_val = updates[pk]
                if new_val != old_val and new_val in table.rows:
                    raise ValueError(f"Duplicate primary key {new_val}")
                # Ensure no child table still references the old PK
                for child_name, fk in self.schema.referenced_by.get(table_name, []):
                    for crow in self.schema.get_table(child_name).select_all():
                        if crow[fk.local_col] == old_val:
                            raise ValueError(f"Cannot update PK {old_val}: still referenced.")

        # 4b. Check foreign key updates
        for parent, fks in self.schema.referenced_by.items():
            for child, fk in fks:
                if child == table_name and fk.local_col in updates:
                    new_val = updates[fk.local_col]
                    parent_table = self.schema.tables[parent]
                    if not any(r[fk.ref_col] == new_val for r in parent_table.select_all()):
                        raise ValueError(f"Foreign key violation: {new_val!r} not in {parent}.{fk.ref_col}")

        # Apply updates
        count = 0
        for row in table.rows:
            if row in target:
                row.update(updates)
                count += 1

        self.schema.save()
        print(f"Updated {count} row(s) in '{table_name}'")

    def _execute_select(self, ast: exp.Select):
        """
        Execute SELECT queries with support for FROM, JOIN, WHERE, GROUP BY, HAVING, ORDER BY, DISTINCT, LIMIT.
        """
        from_clause = ast.args.get("from")
        if not isinstance(from_clause, exp.From):
            raise ValueError("Missing FROM clause")

        table_objs = []
        alias_map = []

        # Handle first table in FROM
        first = from_clause.this
        if isinstance(first, exp.Table):
            name = first.this.this
            alias = first.alias_or_name
            if name not in self.schema.tables:
                raise ValueError(f"Table '{name}' not found.")
            table_objs.append((alias, self.schema.tables[name]))
            alias_map.append(alias)
        else:
            raise ValueError(f"Unsupported FROM element: {type(first)}")

        # Handle JOINs (only single JOIN supported)
        joins = ast.args.get("joins", [])
        for join in joins:
            tbl = join.this
            if isinstance(tbl, exp.Table):
                name = tbl.this.this
                alias = tbl.alias_or_name
                if name not in self.schema.tables:
                    raise ValueError(f"Table '{name}' not found.")
                table_objs.append((alias, self.schema.tables[name]))
                alias_map.append(alias)
            else:
                raise ValueError(f"Unsupported JOIN element: {type(tbl)}")

        # Cartesian product or join
        row_sets = [tbl.select_all() for _, tbl in table_objs]
        if len(table_objs) == 2 and joins:
            left_rows = table_objs[0][1].select_all()
            right_rows = table_objs[1][1].select_all()
            on_cond = joins[0].args.get("on")
            if not on_cond:
                raise ValueError("JOIN missing ON condition")

            strategy = optimizer.choose_join_strategy(left_rows, right_rows, on_cond)
            if strategy == "sort_merge":
                lk, rk = optimizer.extract_join_keys(on_cond)
                raw = optimizer.sort_merge_join(left_rows, right_rows, lk, rk)
            else:
                raw = [(l, r) for l, r in product(left_rows, right_rows)
                       if l[optimizer.extract_join_keys(on_cond)[0]] ==
                          r[optimizer.extract_join_keys(on_cond)[1]]]
        else:
            raw = list(product(*row_sets))

        # Merge rows with proper aliasing
        combined = []
        for combo in raw:
            merged = {}
            for (alias, tbl), row in zip(table_objs, combo):
                if len(table_objs) == 1:
                    merged.update(row)
                else:
                    for k, v in row.items():
                        merged[f"{alias}.{k}"] = v
            combined.append(merged)

        # Apply WHERE with index if possible
        where_expr = ast.args.get("where")
        if where_expr:
            cond = where_expr.this
            if isinstance(cond, (exp.EQ, exp.GTE, exp.LTE, exp.GT, exp.LT)):
                col, val_node = cond.this, cond.expression
                if isinstance(col, exp.Column) and isinstance(val_node, exp.Literal):
                    cn = col.name
                    tbl_name = first.this.this
                    val = val_node.name or val_node.this
                    try:
                        val = int(val)
                    except:
                        pass

                    tbl = self.schema.get_table(tbl_name)
                    idx = tbl.indexes.get(cn)
                    if idx is not None:
                        print(f"Using index on {tbl_name}.{cn} {cond.key} {val}")

                        # Equality vs. range lookups
                        if isinstance(cond, exp.EQ):
                            rid = idx.get(val)
                            combined = ([{f"{tbl_name}.{k}": v for k, v in tbl.rows[rid].items()}]
                                        if rid is not None else [])
                        else:
                            if isinstance(cond, exp.GTE):
                                items = idx.items(min=val)
                            elif isinstance(cond, exp.LTE):
                                items = idx.items(max=val)
                            elif isinstance(cond, exp.GT):
                                items = idx.items(min=val, excludemin=True)
                            else:  # exp.LT
                                items = idx.items(max=val, excludemax=True)

                            combined = [
                                {f"{tbl_name}.{k}": v for k, v in tbl.rows[rid].items()}
                                for _, rid in items
                            ]

            # Reorder predicates for better performance
            reordered = optimizer.reorder_conditions(where_expr.this)
            print("Reordered WHERE clause:", reordered.sql())
            where_expr.set("this", reordered)
            combined = [r for r in combined if self._evaluate_condition(r, reordered)]

        # ORDER BY, projection, grouping, aggregation, DISTINCT, LIMIT
        combined = self._apply_order_by(combined, ast.args.get("order"))

        expressions = ast.args.get("expressions", [])
        if not expressions:
            raise ValueError("No expressions in SELECT clause.")

        group_exprs = ast.args.get("group")
        if group_exprs:
            grouped = self._apply_group_by(combined, group_exprs)
            result = Executor._apply_aggregations(grouped, expressions)
            having = ast.args.get("having")
            if having:
                result = [r for r in result if self._evaluate_condition(r, having.this)]
        else:
            result = []
            # SELECT *
            if len(expressions) == 1 and isinstance(expressions[0], exp.Star):
                result = combined
            else:
                for row in combined:
                    proj = {}
                    for expr in expressions:
                        if isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star):
                            tbl_prefix = expr.table
                            for k, v in row.items():
                                if k.startswith(f"{tbl_prefix}."):
                                    proj[k] = v
                            continue

                        alias = expr.alias if isinstance(expr, exp.Alias) else None
                        col = expr.find(exp.Column)
                        if not col:
                            raise ValueError(f"Could not resolve column: {expr}")

                        cn = col.output_name
                        key = f"{col.table}.{cn}" if col.table else cn
                        val = row.get(key, None)
                        if val is None:
                            # Fallback by suffix
                            val = next((v for k, v in row.items() if k.endswith(f".{cn}")), None)

                        out = cn if len(table_objs) == 1 else alias or cn
                        proj[out] = val
                    result.append(proj)

        result = self._apply_distinct(result, ast.args.get("distinct"))
        result = self._apply_limit(result, ast.args.get("limit"))
        return result

    def check_foreign_key_constraints(self, table_name: str, row: dict):
        """
        Ensure that any foreign key in 'row' references an existing parent row.
        """
        for ref_table, fk_list in self.schema.referenced_by.items():
            for child_table, fk in fk_list:
                if child_table != table_name:
                    continue
                if fk.local_col not in row:
                    continue
                value = row[fk.local_col]
                parent = self.schema.get_table(fk.ref_table)
                if not any(r[fk.ref_col] == value for r in parent.select_all()):
                    raise ValueError(
                        f"Foreign key violation: value {value!r} in '{fk.local_col}' "
                        f"not found in {fk.ref_table}.{fk.ref_col}"
                    )

    def check_foreign_key_constraints_delete(self, table_name: str, row: dict):
        """
        Prevent deletion of a row whose primary key is still referenced downstream.
        """
        tbl = self.schema.get_table(table_name)
        pk_col = tbl.primary_key
        pk_val = row[pk_col]

        # If no tables reference this one, deletion is safe
        if table_name not in self.schema.referenced_by:
            return

        for child_table, fk in self.schema.referenced_by[table_name]:
            for child_row in self.schema.get_table(child_table).select_all():
                if child_row.get(fk.local_col) == pk_val:
                    if fk.policy == "RESTRICT":
                        raise ValueError(
                            f"Cannot delete {table_name}.{pk_col}={pk_val}: "
                            f"still referenced by {child_table}.{fk.local_col}"
                        )
                    # CASCADE would delete children, but here we only check



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
            print(f"⚙  Created new index on {table_name}.{column_name}")
            print(f"after create index:", dict(table.indexes[table.primary_key]))
        else:
            print(f"🔄 Rebuilding existing index on {table_name}.{column_name}")

        table.rebuild_indexes()