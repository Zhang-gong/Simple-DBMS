import sys
import os

# add upper path to import table
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# Gong Zhang added this code on April 16, 2025

from catalog.catalog_manager import CatalogManager
from catalog.table import Table

# Initialize catalog manager
catalog = CatalogManager()

# Create a schema
catalog.create_schema("school")

# Define a table structure
columns = ["id", "name", "grade"]
primary_key = "id"
students_table = Table(name="students", columns=columns, primary_key=primary_key)

# Register the table to the schema
catalog.create_table("school", students_table)

# Insert sample rows
table = catalog.get_table("school", "students")
table.insert({"id": 1, "name": "Alice", "grade": "A"})
table.insert({"id": 2, "name": "Bob", "grade": "B"})

# Query all rows
print("All rows:")
print(table.select_all())

# Query by primary key
print("Query id=1:")
print(table.select_by_key(1))

# Range query (from id=1 to id=3, right-open)
try:
    print("Range query id in [1, 3):")
    results = table.range_query(1, 3)
    print(results)
except Exception as e:
    raise RuntimeError(f"Error during range query: {e}")
