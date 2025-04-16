from BTrees.OOBTree import OOBTree  # OOBTree: key=object, value=object

class Table:
    def __init__(self, name: str, columns: list[str], primary_key: str):
        self.name = name
        self.columns = columns
        self.primary_key = primary_key
        self.rows = OOBTree()  # key 是主键，value 是完整 row dict

    def insert(self, row: dict):
        if set(row.keys()) != set(self.columns):
            raise ValueError("Column mismatch")
        pk = row[self.primary_key]
        if pk in self.rows:
            raise ValueError(f"Duplicate primary key: {pk}")
        self.rows[pk] = row

    def select_all(self):
        return list(self.rows.values())

    def select_by_key(self, key):
        return self.rows.get(key)

    def range_query(self, start_key, end_key):
        """范围查询，左闭右开"""
        return list(self.rows.values(start=start_key, end=end_key))
