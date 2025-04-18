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
        base_path = "data" 
        for table in self.tables.values():
            table_path = os.path.join(base_path, table.name)
            table.save(table_path)

    @staticmethod
    def load(name: str) -> "Schema":
        schema = Schema("default")
        base_path = "data"

        for table_name in os.listdir(base_path):
            table_path = os.path.join(base_path, table_name)
            if os.path.isdir(table_path):
                table = Table.load(table_path)
                schema.create_table(table)

        return schema
