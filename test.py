import os
import json
import csv
from sql_parser import SQLParser
from executor import Executor
from catalog.schema import Schema

# 1. Setup
schema = Schema("test_schema")
executor = Executor(schema)
parser = SQLParser()

# 2. Test CREATE TABLE
sql = """
CREATE TABLE students (
    id INT PRIMARY KEY,
    name TEXT,
    age INT
);
"""
ast = parser.parse(sql)
executor.execute(ast)

# 3. Validate files
table_name = "students"
table_dir = os.path.join("data", table_name)

# Check if directory and files exist
assert os.path.isdir(table_dir), "Table folder not created."
assert os.path.isfile(os.path.join(table_dir, "metadata.json")), "Missing metadata.json"
assert os.path.isfile(os.path.join(table_dir, "data.csv")), "Missing data.csv"

# 4. Validate metadata.json
with open(os.path.join(table_dir, "metadata.json"), "r", encoding="utf-8") as f:
    metadata = json.load(f)

assert metadata["name"] == "students"
assert metadata["primary_key"] == "id"
assert metadata["columns"] == [
    {"name": "id", "type": "INT"},
    {"name": "name", "type": "TEXT"},
    {"name": "age", "type": "INT"}
]

# 5. Validate data.csv header
with open(os.path.join(table_dir, "data.csv"), "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    headers = next(reader)
    assert headers == ["id", "name", "age"]

print("✅ CREATE TABLE test passed.")
