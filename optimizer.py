from sqlglot import exp

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
    if not isinstance(condition, exp.EQ):
        return "nested_loop"  # Only EQ supports sort-merge

    # Heuristic: if both tables have more than 100 rows, use sort-merge
    if len(left_rows) > 100 and len(right_rows) > 100:
        return "sort_merge"

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

    if not isinstance(left_col, exp.Column) or not isinstance(right_col, exp.Column):
        raise ValueError("Join condition must be between two columns")

    return left_col.name, right_col.name

def sort_merge_join(left_rows, right_rows, left_key: str, right_key: str) -> list[tuple[dict, dict]]:
    """
    Perform a sort-merge join between two sets of rows.

    Parameters:
        left_rows (list[dict]): Rows from the left table.
        right_rows (list[dict]): Rows from the right table.
        left_key (str): Key to join on from the left table.
        right_key (str): Key to join on from the right table.

    Returns:
        list[tuple[dict, dict]]: Joined row pairs (left, right)
    """
    left_sorted = sorted(left_rows, key=lambda r: r[left_key])
    right_sorted = sorted(right_rows, key=lambda r: r[right_key])

    i = j = 0
    joined = []

    while i < len(left_sorted) and j < len(right_sorted):
        lv = left_sorted[i][left_key]
        rv = right_sorted[j][right_key]

        if lv == rv:
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

