import os
import json
import csv
from BTrees.OOBTree import OOBTree

class ForeignKey:
    def __init__(self, local_col: str, ref_table: str, ref_col: str, policy: str = "RESTRICT"):
        self.local_col = local_col
        self.ref_table = ref_table
        self.ref_col = ref_col
        self.policy = policy.upper()  # "RESTRICT" or "CASCADE"

class Table:
    def __init__(self, name: str, columns: list[dict], primary_key: str):
        self.name = name
        self.columns = columns  # List of {name, type}
        self.primary_key = primary_key
        self.column_names = [col["name"] for col in columns]
        self.rows = []  # List of row dicts
        self.indexes = {}  # Column -> BTree index

        # Optional: create BTree index for primary key
        # self.indexes[primary_key] = OOBTree()

    def insert(self, row: dict):
        if set(row.keys()) != set(self.column_names):
            raise ValueError("Column mismatch")

        self._validate_row_types(row)
        #check foreign keys
        for fk in self.foreign_keys:
            ref_table = self.schema.get_table(fk.ref_table)
            ref_values = [r[fk.ref_col] for r in ref_table.select_all()]
            if row[fk.local_col] not in ref_values:
                raise ValueError(
                    f"Foreign key constraint failed: {row[fk.local_col]} not found in {fk.ref_table}.{fk.ref_col}"
                )

        pk = row[self.primary_key]

        # Enforce primary key uniqueness manually (if no index)
        if self.primary_key not in self.indexes:
            if any(existing_row[self.primary_key] == pk for existing_row in self.rows):
                raise ValueError(f"Duplicate primary key: {pk}")
        else:
            if pk in self.indexes[self.primary_key]:
                raise ValueError(f"Duplicate primary key: {pk}")

        self.rows.append(row)
        # Update the index for the primary key
        for col, index in self.indexes.items():
            index[row[col]] = row

    def _validate_row_types(self, row: dict):
        for col in self.columns:
            name = col["name"]
            expected_type = col["type"].upper()
            value = row[name]

            if expected_type == "INT" and not isinstance(value, int):
                raise TypeError(f"Column '{name}' expects INT but got {type(value).__name__}")
            if expected_type == "TEXT" and not isinstance(value, str):
                raise TypeError(f"Column '{name}' expects TEXT but got {type(value).__name__}")

    def select_all(self):
        return self.rows

    def select_by_key(self, key):
        return self.indexes[self.primary_key].get(key)

    def range_query(self, start_key, end_key):
        index = self.indexes[self.primary_key]
        return list(index.values(start_key, end_key))

    def create_index(self, column: str):
        if column not in self.column_names:
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
        with open(os.path.join(directory, "metadata.json"), "w", encoding="utf-8") as f:
            json.dump(definition, f, indent=4)

        # Save rows as CSV
        with open(os.path.join(directory, "data.csv"), "w", newline='', encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.column_names)
            writer.writeheader()
            writer.writerows(self.rows)







    @staticmethod
    def load(directory: str) -> "Table":
        # Load table schema
        with open(os.path.join(directory, "metadata.json"), "r", encoding="utf-8") as f:
            metadata = json.load(f)

        name = metadata["name"]
        columns = metadata["columns"]
        primary_key = metadata["primary_key"]

        # Initialize table
        table = Table(name, columns, primary_key)

        # Load rows from CSV
        csv_path = os.path.join(directory, "data.csv")
        if os.path.exists(csv_path):
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    for col in columns:
                        col_name = col["name"]
                        col_type = col["type"]
                        if col_type == "INT" and row[col_name] != "":
                            row[col_name] = int(row[col_name])
                    table.rows.append(row)

        return table
