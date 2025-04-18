from sql_parser import SQLParser
from executor import Executor
from catalog.table import Table
from catalog.schema import Schema


if __name__ == '__main__':
    schema=Schema("test_schema")
    parser = SQLParser()
    executor = Executor(schema)


    sql_create = "CREATE TABLE A (id INT PRIMARY KEY, row TEXT)"
    ast_create= parser.parse(sql_create)
    table_a=executor.execute(ast_create)



    # table_A = Table(name="A", columns=["id", "row"],primary_key="id")
    # table_A.insert({"id": 1, "row": "foo"})
    # table_A.insert({"id": 5, "row": "bar"})
    # table_A.insert({"id": 9, "row": "baz"})
    sql_where = "SELECT row FROM A WHERE id = 5"
    ast_where = parser.parse(sql_where)
    print("AST(Abstract Syntax Tree):",ast_where)
    print("AST args:", ast_where.args)
    # executor = Executor(tables={"A": table_A})
    result = executor.execute(ast_where)
    print("Query Result:", result)