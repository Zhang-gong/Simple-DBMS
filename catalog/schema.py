# schema.py

import os
import json
from typing import Dict
from .table import Table, ForeignKey
from sqlglot import exp

class Schema:
    """
    Represents the database schema, including table definitions and foreign key metadata.
    """

    def __init__(self, name: str):
        """
        Initialize a new schema.

        Attributes:
            tables (Dict[str, Table]): Mapping of table names to Table objects.
            referenced_by (Dict[str, list[tuple[str, ForeignKey]]]):
                Tracks which tables reference a given table.
        """
        self.name = name
        self.tables: Dict[str, Table] = {}
        self.referenced_by: Dict[str, list[tuple[str, ForeignKey]]] = {}

    def create_table(self, table: Table):
        """
        Add a new table to the schema.

        Raises:
            ValueError: If a table with the same name already exists.
        """
        if table.name in self.tables:
            raise ValueError(f"Table '{table.name}' already exists in schema '{self.name}'")
        self.tables[table.name] = table

    def has_table(self, table_name: str) -> bool:
        """Return True if the schema contains a table by that name."""
        return table_name in self.tables

    def get_table(self, table_name: str) -> Table:
        """Retrieve a Table object by name (or None if not found)."""
        return self.tables.get(table_name)

    def drop_table(self, table_name: str, policy: str = "RESTRICT"):
        """
        Remove a table from the schema, enforcing RESTRICT or CASCADE foreign key policy.

        Raises:
            ValueError: If table does not exist or RESTRICT policy prevents drop.
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' not found in schema '{self.name}'")

        # If other tables reference this one, enforce RESTRICT/CASCADE
        if table_name in self.referenced_by:
            refs = self.referenced_by[table_name]
            # If any foreign key is RESTRICT and policy is not CASCADE, block drop
            if any(fk.policy == "RESTRICT" for _, fk in refs) and policy.upper() != "CASCADE":
                ref_names = [tbl for tbl, _ in refs]
                raise ValueError(f"Cannot drop '{table_name}': referenced by {ref_names}")

            # If CASCADE, recursively drop dependent tables first
            if policy.upper() == "CASCADE":
                for child, _ in refs:
                    self.drop_table(child, policy="CASCADE")
            del self.referenced_by[table_name]

        # Remove references to this table from other entries
        for parent in list(self.referenced_by):
            updated = [(t, fk) for t, fk in self.referenced_by[parent] if t != table_name]
            if updated:
                self.referenced_by[parent] = updated
            else:
                del self.referenced_by[parent]

        # Finally, delete the table definition
        del self.tables[table_name]
        print(f"Dropped table '{table_name}' and cleaned up references.")

    def save(self):
        """
        Persist each table's data and metadata, and write foreign key info to disk.
        """
        base = "data"
        # Save each table
        for tbl in self.tables.values():
            path = os.path.join(base, tbl.name)
            tbl.save(path)

        # Serialize foreign key metadata
        fk_data = {}
        for ref_table, refs in self.referenced_by.items():
            fk_data[ref_table] = []
            for tbl_name, fk in refs:
                fk_data[ref_table].append({
                    "table": tbl_name,
                    "columns": fk.local_col,
                    "ref_table": fk.ref_table,
                    "ref_columns": fk.ref_col,
                    "policy": fk.policy,
                })

        fk_path = os.path.join(base, "foreignkey.json")
        with open(fk_path, "w", encoding="utf-8") as f:
            json.dump(fk_data, f, indent=2)

    @staticmethod
    def load(name: str) -> "Schema":
        """
        Load schema from disk: all tables and the foreign key JSON.
        """
        schema = Schema(name)
        base = "data"

        # Recreate tables
        for entry in os.listdir(base):
            tbl_path = os.path.join(base, entry)
            if os.path.isdir(tbl_path):
                tbl = Table.load(tbl_path)
                schema.create_table(tbl)

        # Load foreign key relationships
        fk_path = os.path.join(base, "foreignkey.json")
        if os.path.exists(fk_path):
            with open(fk_path, "r", encoding="utf-8") as f:
                fk_json = json.load(f)
                for ref_table, fk_list in fk_json.items():
                    for ent in fk_list:
                        fk_obj = ForeignKey(
                            local_col=ent["columns"],
                            ref_table=ent["ref_table"],
                            ref_col=ent["ref_columns"],
                            policy=ent.get("policy", "RESTRICT")
                        )
                        schema.referenced_by.setdefault(ref_table, []).append((ent["table"], fk_obj))
        return schema
