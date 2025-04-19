import os
import json
from typing import Dict
from .table import Table
from .table import ForeignKey
class Schema:
    def __init__(self, name: str):
        self.name = name
        self.tables: Dict[str, Table] = {}
        self.referenced_by: Dict[str, list[tuple[str, ForeignKey]]] = {}  # 被哪些表引用


    def create_table(self, table: Table):
        if table.name in self.tables:
            raise ValueError(f"Table '{table.name}' already exists in schema '{self.name}'")
        self.tables[table.name] = table
        # for fk in table.foreign_keys:
        #     self.referenced_by.setdefault(fk.ref_table, []).append((table.name, fk))

    def has_table(self, table_name: str) -> bool:
        return table_name in self.tables

    def get_table(self, table_name: str) -> Table:
        return self.tables.get(table_name)

    def drop_table(self, table_name: str):
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' not found in schema '{self.name}'")
        del self.tables[table_name]
        if table_name in self.referenced_by:
            ref_list = self.referenced_by[table_name]
            if ref_list.policy.upper() == "RESTRICT":
                ref_names = [tbl for tbl, _ in ref_list]
                raise ValueError(f"Cannot drop table '{table_name}': referenced by {ref_names}")
            elif ref_list.policy.upper() == "CASCADE":
                for child_table_name, fk in ref_list:
                    self.drop_table(child_table_name, policy="CASCADE")

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
