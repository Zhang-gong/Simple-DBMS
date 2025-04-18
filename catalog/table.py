import os
import json
import csv
from BTrees.OOBTree import OOBTree

class Table:
    def __init__(self, name: str, columns: list[str], primary_key: str):
        self.name = name
        self.columns = columns
        self.primary_key = primary_key
        self.rows = []  # List of row dicts
        self.indexes = {}  # Column -> BTree index
        # Create a BTree index for the primary key
        # self.indexes[primary_key] = OOBTree()

    def insert(self, row: dict):
        if set(row.keys()) != set(self.columns):
            raise ValueError("Column mismatch")

        pk = row[self.primary_key]
        if self.primary_key in self.indexes and pk in self.indexes[self.primary_key]:
            raise ValueError(f"Duplicate primary key: {pk}")

        self.rows.append(row)

        for col, index in self.indexes.items():
            index[row[col]] = row

    def select_all(self):
        return self.rows

    def select_by_key(self, key):
        return self.indexes[self.primary_key].get(key)

    def range_query(self, start_key, end_key):
        index = self.indexes[self.primary_key]
        return list(index.values(start_key, end_key))

    def create_index(self, column: str):
        if column not in self.columns:
            raise ValueError(f"Column {column} does not exist.")
        if column in self.indexes:
            print(f"Index on column '{column}' already exists.")
            return

        btree = OOBTree()
        for row in self.rows:
            key = row[column]
            if key in btree:
                btree[key].append(row)
            else:
                btree[key] = [row]
        self.indexes[column] = btree
        print(f"✅ Index created on column '{column}'")

    def save(self, directory: str):
        os.makedirs(directory, exist_ok=True)

        # Save table definition as JSON
        definition = {
            "name": self.name,
            "columns": self.columns,
            "primary_key": self.primary_key
        }
        with open(os.path.join(directory, "definition.json"), "w", encoding="utf-8") as f:
            json.dump(definition, f, indent=4)

        # Save rows as CSV
        with open(os.path.join(directory, "data.csv"), "w", newline='', encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.columns)
            writer.writeheader()
            writer.writerows(self.rows)
