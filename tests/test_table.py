import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from storage.table import Table
#create table
table = Table("students", ["id", "name"])

# 插入数据
table.insert({"id": 1, "name": "Alice"})
table.insert({"id": 2, "name": "Bob"})

# 查询所有数据
print(table.select_all())