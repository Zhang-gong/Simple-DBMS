from BTrees.OOBTree import OOBTree

class Table:
    def __init__(self, name: str, columns: list[str], primary_key: str):
        self.name = name
        self.columns = columns
        self.primary_key = primary_key
        self.rows = []  # List of row dicts
        self.indexes = {}  # Column -> BTree index

        # build index for primary key
        # self.indexes[primary_key] = OOBTree()

    def insert(self, row: dict):
        if set(row.keys()) != set(self.columns):
            raise ValueError("Column mismatch")

        pk = row[self.primary_key]

        # 检查主键重复（从索引中查）
        if pk in self.indexes[self.primary_key]:
            raise ValueError(f"Duplicate primary key: {pk}")

        # 插入行
        self.rows.append(row)

        # 更新所有索引（至少主键）
        for col, index in self.indexes.items():
            index[row[col]] = row

    def select_all(self):
        return self.rows

    def select_by_key(self, key):
        return self.indexes[self.primary_key].get(key)

    def range_query(self, start_key, end_key):
        """

        """
        index = self.indexes[self.primary_key]
        return list(index.values(start_key, end_key))

    def create_index(self, column: str):
        """

        """
        if column not in self.columns:
            raise ValueError(f"Column {column} does not exist.")
        if column in self.indexes:
            print(f"Index on column '{column}' already exists.")
            return

        btree = OOBTree()
        for row in self.rows:
            key = row[column]
            # 若值重复，用 list 存储
            if key in btree:
                btree[key].append(row)
            else:
                btree[key] = [row]
        self.indexes[column] = btree
        print(f"✅ Index created on column '{column}'")
