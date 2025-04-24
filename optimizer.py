# optimizer.py

from sqlglot import parse_one, exp
from functools import reduce

def choose_join_strategy(left_rows, right_rows, condition: exp.Expression) -> str:
    """
    Decide whether to use nested loop join or sort-merge join.

    Parameters:
        left_rows (list[dict]): Rows from the left table.
        right_rows (list[dict]): Rows from the right table.
        condition (exp.Expression): JOIN ON condition (e.g., a.id = b.a_id).

    Returns:
        str: 'nested_loop' or 'sort_merge'
    """
    # Only equality joins can be accelerated via sort-merge
    if not isinstance(condition, exp.EQ):
        return "nested_loop"

    # Use sort-merge join when both tables exceed 100 rows
    if len(left_rows) > 100 and len(right_rows) > 100:
        return "sort_merge"

    # Otherwise, default to nested-loop join
    return "nested_loop"

def extract_join_keys(condition: exp.EQ) -> tuple[str, str]:
    """
    Extract fully qualified join keys from an EQ join condition.
    E.g., a.id = b.a_id --> ("a.id", "b.a_id")

    Parameters:
        condition (exp.EQ): Equality expression

    Returns:
        tuple[str, str]: Fully qualified keys for both sides of the condition
    """
    left_col = condition.this
    right_col = condition.expression

    # Both sides must be column references
    if not isinstance(left_col, exp.Column) or not isinstance(right_col, exp.Column):
        raise ValueError("Join condition must be between two columns")

    return left_col.name, right_col.name

def sort_merge_join(left_rows, right_rows, left_key: str, right_key: str) -> list[tuple[dict, dict]]:
    """
    Perform a sort-merge join between two sets of rows.

    Parameters:
        left_rows (list[dict]): Rows from the left table.
        right_rows (list[dict]): Rows from the right table.
        left_key (str): Join key from the left table.
        right_key (str): Join key from the right table.

    Returns:
        list[tuple[dict, dict]]: List of matching (left_row, right_row) tuples.
    """
    # Sort both datasets by their join keys
    left_sorted = sorted(left_rows, key=lambda r: r[left_key])
    right_sorted = sorted(right_rows, key=lambda r: r[right_key])

    i = j = 0
    joined = []

    # Merge-scan through both sorted lists
    while i < len(left_sorted) and j < len(right_sorted):
        lv = left_sorted[i][left_key]
        rv = right_sorted[j][right_key]

        if lv == rv:
            # Emit all matching pairs for this key
            temp_j = j
            while temp_j < len(right_sorted) and right_sorted[temp_j][right_key] == lv:
                joined.append((left_sorted[i], right_sorted[temp_j]))
                temp_j += 1
            i += 1
        elif lv < rv:
            i += 1
        else:
            j += 1

    return joined

def reorder_conditions(expression: exp.Expression) -> exp.Expression:
    """
    Reorder conjunctive (AND) or disjunctive (OR) conditions based on cost estimates.
    - For AND: cheapest predicates first (ascending cost).
    - For OR: most selective predicates first (descending cost).
    """
    if isinstance(expression, exp.And):
        return reorder_logical_conditions(expression, is_and=True)
    elif isinstance(expression, exp.Or):
        return reorder_logical_conditions(expression, is_and=False)
    return expression

def reorder_logical_conditions(expression: exp.Expression, is_and: bool) -> exp.Expression:
    """
    Flatten and sort an AND/OR expression tree, then rebuild it.

    Parameters:
        expression (exp.Expression): The root AND/OR expression.
        is_and (bool): True for AND, False for OR.
    """
    flat_conditions = flatten_conditions(expression, is_and)
    sorted_conditions = sorted(
        flat_conditions,
        key=estimate_cost,
        reverse=not is_and  # AND: ascending; OR: descending
    )
    return rebuild_condition_chain(sorted_conditions, is_and)

def flatten_conditions(expression: exp.Expression, is_and: bool) -> list[exp.Expression]:
    """
    Recursively collect all sub-expressions under AND or OR into a flat list.

    Parameters:
        expression (exp.Expression): AND/OR tree.
        is_and (bool): True for AND, False for OR.
    """
    result = []
    def _recurse(e):
        if (is_and and isinstance(e, exp.And)) or (not is_and and isinstance(e, exp.Or)):
            _recurse(e.left)
            _recurse(e.right)
        else:
            result.append(e)
    _recurse(expression)
    return result

def rebuild_condition_chain(conditions: list[exp.Expression], is_and: bool) -> exp.Expression:
    """
    Rebuild an AND/OR expression tree from a list of conditions.

    Parameters:
        conditions (list[exp.Expression]): List of predicates.
        is_and (bool): True to use AND, False to use OR.
    """
    if not conditions:
        return None
    join_func = exp.and_ if is_and else exp.or_
    # Reduce the list into a binary tree
    return reduce(join_func, conditions)

def estimate_cost(pred: exp.Expression) -> int:
    """
    Heuristic cost assignment for a predicate.
    Lower cost => evaluate earlier in AND; higher cost => evaluate earlier in OR.

    Costs:
      =   : 1
      <,>,<=,>= : 5
      LIKE '%': 50
      Function call or complex expression: 100
      Others: 20
    """
    sql = pred.sql().upper()

    if "=" in sql:
        return 1
    elif ">" in sql or "<" in sql:
        return 5
    elif "LIKE '%" in sql:
        return 50
    elif "(" in sql:  # any function or nested expression
        return 100
    else:
        return 20
