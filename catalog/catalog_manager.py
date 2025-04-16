from typing import Dict
from .schema import Schema
from .table import Table

class CatalogManager:
    def __init__(self):
        self.schemas: Dict[str, Schema] = {}

    def create_schema(self, schema_name: str):
        if schema_name in self.schemas:
            raise ValueError(f"Schema '{schema_name}' already exists")
        self.schemas[schema_name] = Schema(schema_name)

    def get_schema(self, schema_name: str) -> Schema:
        return self.schemas.get(schema_name)

    def create_table(self, schema_name: str, table: Table):
        schema = self.get_schema(schema_name)
        if not schema:
            raise ValueError(f"Schema '{schema_name}' does not exist")
        schema.create_table(table)

    def get_table(self, schema_name: str, table_name: str) -> Table:
        schema = self.get_schema(schema_name)
        if not schema:
            raise ValueError(f"Schema '{schema_name}' does not exist")
        return schema.get_table(table_name)

    def drop_table(self, schema_name: str, table_name: str):
        schema = self.get_schema(schema_name)
        if not schema:
            raise ValueError(f"Schema '{schema_name}' does not exist")
        schema.drop_table(table_name)
