from catalog.schema import Schema
from executor import Executor
from sql_parser import SQLParser

def run_sql(executor, parser, sql: str):
    ast = parser.parse(sql)
    result = executor.execute(ast)
    if result:
        print(f"{len(result)} row(s) returned.")
        for row in result:
            print(row)
    else:
        print("(empty set)")


def main():
    schema = Schema("default")
    executor = Executor(schema)
    parser = SQLParser()

    # Drop old tables (ignore errors)
    for stmt in [
        "DROP TABLE IF EXISTS grades;",
        "DROP TABLE IF EXISTS enrollments;",
        "DROP TABLE IF EXISTS students;"
    ]:
        try:
            run_sql(executor, parser, stmt)
        except:
            pass

    # Create tables
    run_sql(executor, parser, """
        CREATE TABLE students (
            id INT PRIMARY KEY,
            name TEXT,
            age INT
        );
    """)

    run_sql(executor, parser, """
        CREATE TABLE enrollments (
            enrollment_id INT PRIMARY KEY,
            student_id INT,
            course TEXT,
            FOREIGN KEY (student_id) REFERENCES students(id)
        );
    """)

    run_sql(executor, parser, """
        CREATE TABLE grades (
            grade_id INT PRIMARY KEY,
            enrollment_id INT,
            score INT,
            FOREIGN KEY (enrollment_id) REFERENCES enrollments(enrollment_id)
        );
    """)

    # Insert students
    run_sql(executor, parser, "INSERT INTO students (id, name, age) VALUES (1, 'Alice', 20);")
    run_sql(executor, parser, "INSERT INTO students (id, name, age) VALUES (2, 'Bob', 21);")

    # Insert enrollments
    run_sql(executor, parser, "INSERT INTO enrollments (enrollment_id, student_id, course) VALUES (101, 1, 'CS');")
    run_sql(executor, parser, "INSERT INTO enrollments (enrollment_id, student_id, course) VALUES (102, 1, 'Math');")
    run_sql(executor, parser, "INSERT INTO enrollments (enrollment_id, student_id, course) VALUES (103, 2, 'Physics');")

    # Insert grades
    run_sql(executor, parser, "INSERT INTO grades (grade_id, enrollment_id, score) VALUES (1001, 101, 95);")
    run_sql(executor, parser, "INSERT INTO grades (grade_id, enrollment_id, score) VALUES (1002, 102, 85);")
    run_sql(executor, parser, "INSERT INTO grades (grade_id, enrollment_id, score) VALUES (1003, 103, 75);")

    # Multi-table JOIN test
    print("\n🔍 Testing 3-table SELECT JOIN:")
    run_sql(executor, parser, """
        SELECT s.name, e.course, g.score
        FROM students s
        JOIN enrollments e ON s.id = e.student_id
        JOIN grades g ON e.enrollment_id = g.enrollment_id;
    """)

if __name__ == "__main__":
    main()
