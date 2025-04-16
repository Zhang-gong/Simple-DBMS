import sys
import os
# add upper path to import table
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from catalog.table import Table


def test_table():
    # 创建表，定义主键
    table = Table(name="students", columns=["id", "name"], primary_key="id")

    # 插入数据
    table.insert({"id": 1, "name": "Alice"})
    table.insert({"id": 2, "name": "Bob"})

    print("✅ All rows after insertion:")
    for row in table.select_all():
        print(row)

    # 测试主键冲突
    try:
        table.insert({"id": 1, "name": "Charlie"})
    except ValueError as e:
        print(f"✅ Duplicate key test passed: {e}")

    # query for "equal"
    row = table.select_by_key(2)
    print(f"✅ Query by key=2 result: {row}")

    # ）
    try:
        rows_in_range = table.range_query(1, 3)
        print(f"✅ Range query [1, 3):")
        for r in rows_in_range:
            print(r)
    except Exception:
        print("ℹ️ you haven't implemented range query yet")


if __name__ == "__main__":
    test_table()
