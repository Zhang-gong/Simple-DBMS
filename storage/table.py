class Table:
    def __init__(self, name: str, columns: list[str]):
        self.name = name                    # name
        self.columns = columns              # columns name ["id", "name"]
        self.rows = []                      # data, and in every row dict：{col: val}

    def insert(self, row: dict):
        if set(row.keys()) != set(self.columns):
            raise ValueError("Column mismatch")
        self.rows.append(row)

    def select_all(self):
        return self.rows

    def __repr__(self):
        return f"<Table {self.name} with {len(self.rows)} rows>"
