import os
import json
import csv
# pickle import commented out
from BTrees.OOBTree import OOBTree

class ForeignKey:
    """
    Represents a single-column foreign key constraint.
    """
    def __init__(self, local_col: str, ref_table: str, ref_col: str, policy: str = "RESTRICT"):
        self.local_col = local_col
        self.ref_table = ref_table
        self.ref_col = ref_col
        self.policy = policy.upper()  # "RESTRICT" or "CASCADE"

class Table:
    """
    Encapsulates a table's schema, data rows, and BTree indexes.
    """
    def __init__(self, name: str, columns: list[dict], primary_key: str):
        """
        Parameters:
            name (str): Table name.
            columns (list[dict]): List of {"name": col_name, "type": col_type}.
            primary_key (str): Column name of the primary key.
        """
        self.name = name
        self.columns = columns
        self.primary_key = primary_key
        self.column_names = [col["name"] for col in columns]
        self.rows: list[dict] = []
        # Initialize indexes: one BTree for primary key, others None
        self.indexes: Dict[str, OOBTree] = {col: None for col in self.column_names}
        self._init_primary_key_index()

    def _init_primary_key_index(self):
        """Create a BTree index for the primary key column."""
        self.indexes[self.primary_key] = OOBTree()

    def insert(self, row: dict):
        """
        Insert a new row, enforce primary key uniqueness, and update indexes.

        Raises:
            ValueError: If column mismatch or duplicate primary key.
        """
        if set(row.keys()) != set(self.column_names):
            raise ValueError("Column mismatch")

        self._validate_row_types(row)
        pk = row[self.primary_key]

        # Check uniqueness via primary key index if available
        if self.primary_key in self.indexes and pk in self.indexes[self.primary_key]:
            raise ValueError(f"Duplicate primary key: {pk}")

        row_id = len(self.rows)
        self.rows.append(row)

        # Update all existing indexes with new row
        for col, idx in self.indexes.items():
            if idx is not None:
                idx[row[col]] = row_id

    def _validate_row_types(self, row: dict):
        """
        Ensure each value matches its declared column type.
        """
        for col in self.columns:
            name = col["name"]
            expected = col["type"].upper()
            val = row[name]
            if expected == "INT" and not isinstance(val, int):
                raise TypeError(f"Column '{name}' expects INT but got {type(val).__name__}")
            if expected == "TEXT" and not isinstance(val, str):
                raise TypeError(f"Column '{name}' expects TEXT but got {type(val).__name__}")

    def select_all(self):
        """Return all rows as a list of dicts."""
        return self.rows

    def select_by_key(self, key):
        """Retrieve a row by its primary key via the BTree index."""
        return self.indexes[self.primary_key].get(key)

    def range_query(self, start_key, end_key):
        """
        Return all row IDs whose primary key lies in [start_key, end_key).
        """
        idx = self.indexes[self.primary_key]
        return list(idx.values(start_key, end_key))

    def create_index(self, column: str):
        """
        Build a new BTree index on the specified column.
        """
        if column not in self.column_names:
            raise ValueError(f"Column '{column}' does not exist.")
        if self.indexes[column] is not None:
            print(f"Index already exists on column '{column}'.")
            return

        btree = OOBTree()
        for row_id, row in enumerate(self.rows):
            key = row[column]
            btree.setdefault(key, []).append(row_id)
        self.indexes[column] = btree
        print(f"Index created on column '{column}'")

    def save(self, directory: str):
        """
        Persist table metadata (JSON) and data (CSV) to the given directory.
        """
        os.makedirs(directory, exist_ok=True)

        # Save schema definition
        definition = {
            "name": self.name,
            "columns": self.columns,
            "primary_key": self.primary_key
        }
        with open(os.path.join(directory, "metadata.json"), "w", encoding="utf-8") as f:
            json.dump(definition, f, indent=4)

        # Save row data as CSV
        with open(os.path.join(directory, "data.csv"), "w", newline='', encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.column_names)
            writer.writeheader()
            writer.writerows(self.rows)

    @staticmethod
    def load(directory: str) -> "Table":
        """
        Load table schema and data from the specified directory.
        """
        # Read metadata.json
        with open(os.path.join(directory, "metadata.json"), "r", encoding="utf-8") as f:
            md = json.load(f)
        table = Table(md["name"], md["columns"], md["primary_key"])

        # Read data.csv
        csv_path = os.path.join(directory, "data.csv")
        if os.path.exists(csv_path):
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Convert INT columns back to int
                    for col in md["columns"]:
                        if col["type"] == "INT" and row[col["name"]] != "":
                            row[col["name"]] = int(row[col["name"]])
                    table.rows.append(row)

        # Rebuild all indexes
        table.rebuild_indexes()
        return table

    def rebuild_indexes(self):
        """
        Reconstruct every non-null BTree index from current rows.
        """
        from random import shuffle

        for col, idx in list(self.indexes.items()):
            if idx is not None:
                # Gather (key, row_id) pairs
                pairs = [(row[col], i) for i, row in enumerate(self.rows)]
                shuffle(pairs)
                new_btree = OOBTree()
                for key, row_id in pairs:
                    try:
                        new_btree[key] = row_id
                    except RecursionError:
                        # Skip problematic keys
                        print(f"Skipped inserting key {key} due to recursion error")
                self.indexes[col] = new_btree
