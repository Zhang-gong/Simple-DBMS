import os
import json
from typing import Dict
from .table import Table

class Schema:
    def __init__(self, name: str):
        self.name = name
        self.tables: Dict[str, Table] = {}

    def create_table(self, table: Table):
        if table.name in self.tables:
            raise ValueError(f"Table '{table.name}' already exists in schema '{self.name}'")
        self.tables[table.name] = table

    def has_table(self, table_name: str) -> bool:
        return table_name in self.tables

    def get_table(self, table_name: str) -> Table:
        return self.tables.get(table_name)

    def drop_table(self, table_name: str):
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' not found in schema '{self.name}'")
        del self.tables[table_name]

    def save(self):
        # Create schema directory
        base_path = os.path.join("storage", self.name)
        os.makedirs(self.name, exist_ok=True)

        for table in self.tables.values():
            table_path = os.path.join(self.name, table.name)
            table.save(table_path)
