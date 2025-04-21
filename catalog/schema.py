import os
import json
from typing import Dict
from .table import Table
from .table import ForeignKey
from sqlglot import exp

class Schema:
    def __init__(self, name: str):
        self.name = name
        self.tables: Dict[str, Table] = {}
        self.referenced_by: Dict[str, list[tuple[str, ForeignKey]]] = {}  # 被哪些表引用


    def create_table(self, table: Table):
        if table.name in self.tables:
            raise ValueError(f"Table '{table.name}' already exists in schema '{self.name}'")
        self.tables[table.name] = table

    def has_table(self, table_name: str) -> bool:
        return table_name in self.tables

    def get_table(self, table_name: str) -> Table:
        return self.tables.get(table_name)

    def drop_table(self, table_name: str, policy: str = "RESTRICT"):
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' not found in schema '{self.name}'")

        # ✅ 检查是否被其他表引用
        if table_name in self.referenced_by:
            ref_list = self.referenced_by[table_name]

            # ✅ 策略检查：是否有任意 foreign key 是 RESTRICT
            has_restrict = any(fk.policy == "RESTRICT" for _, fk in ref_list)

            if has_restrict and policy.upper() != "CASCADE":
                ref_names = [tbl for tbl, _ in ref_list]
                raise ValueError(f"❌ Cannot drop table '{table_name}': referenced by {ref_names} with RESTRICT policy")

            # ✅ 如果设置为 CASCADE，递归删除所有引用它的子表
            if policy.upper() == "CASCADE":
                for child_table_name, fk in ref_list:
                    self.drop_table(child_table_name, policy="CASCADE")
            del self.referenced_by[table_name]

        for ref_table_name in list(self.referenced_by.keys()):
            updated_refs = [
                (t, fk) for (t, fk) in self.referenced_by[ref_table_name]
                if t != table_name  # ❗️删除当前表对别人的引用
            ]
            if updated_refs:
                self.referenced_by[ref_table_name] = updated_refs
            else:
                del self.referenced_by[ref_table_name]  # 清理空列表

            # ✅ 3. 删除表本身
        del self.tables[table_name]
        print(self.referenced_by)

    def save(self):
        # Create schema directory
        base_path = "data" 
        for table in self.tables.values():
            table_path = os.path.join(base_path, table.name)
            table.save(table_path)
        # Save foreign key metadata
        foreignkey_data = {}
        for ref_table, ref_list in self.referenced_by.items():
            foreignkey_data[ref_table] = []
            for table_name, fk in ref_list:
                foreignkey_data[ref_table].append({
                    "table": table_name,
                    "columns": fk.local_col,
                    "ref_table": fk.ref_table,
                    "ref_columns": fk.ref_col,
                    "policy": fk.policy,
                })

        fk_path = os.path.join(base_path, "foreignkey.json")
        with open(fk_path, "w") as f:
            json.dump(foreignkey_data, f, indent=2)

    @staticmethod
    def load(name: str) -> "Schema":
        schema = Schema("default")
        base_path = "data"

        for table_name in os.listdir(base_path):
            table_path = os.path.join(base_path, table_name)
            if os.path.isdir(table_path):
                table = Table.load(table_path)
                schema.create_table(table)


        # Load foreign key metadata
        fk_path = os.path.join(base_path, "foreignkey.json")
        if os.path.exists(fk_path):
            with open(fk_path, "r") as f:
                fk_data = json.load(f)

                for ref_table, fk_list in fk_data.items():
                    for fk_entry in fk_list:
                        referencing_table = fk_entry["table"]
                        local_col = fk_entry["columns"]
                        ref_col = fk_entry["ref_columns"]
                        policy = fk_entry.get("policy", "STRICT")  # 给默认值

                        fk_obj = ForeignKey(
                            local_col=local_col,
                            ref_table=fk_entry["ref_table"],
                            ref_col=ref_col,
                            policy=policy
                        )
                        if ref_table not in schema.referenced_by:
                            schema.referenced_by[ref_table] = []
                        schema.referenced_by[ref_table].append((referencing_table, fk_obj))

        return schema
