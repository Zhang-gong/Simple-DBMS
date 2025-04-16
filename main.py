from parser.sql_parser import SQLParser
from execution.executor import Executor
from storage.table import Table


if __name__ == '__main__':
    #build table A
    table_A = Table(name="A", columns=["id", "row"],primary_key="id")
    table_A.insert({"id": 1, "row": "foo"})
    table_A.insert({"id": 5, "row": "bar"})
    table_A.insert({"id": 9, "row": "baz"})

    parser = SQLParser()
    sql ="SELECT row FROM A WHERE id = 5"
    #sql = input("Enter SQL: ")
    ast = parser.parse(sql)
    print("AST(Abstract Syntax Tree):",ast)
    print("AST args:", ast.args)
    executor = Executor(tables={"A": table_A})
    result = executor.execute(ast)
    print("Query Result:", result)

#Select * from A