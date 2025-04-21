from catalog.schema import Schema
from executor import Executor
from sql_parser import SQLParser
import time

# ANSI color codes
RESET = "\033[0m"
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"



def print_mysql_table(rows: list[dict]):
    if not rows:
        print("(empty set)")
        return

    headers = list(rows[0].keys())
    col_widths = {h: max(len(h), max(len(str(row[h])) for row in rows)) for h in headers}

    def format_row(row):
        return "| " + " | ".join(f"{str(row[h]).ljust(col_widths[h])}" for h in headers) + " |"

    border = "+-" + "-+-".join("-" * col_widths[h] for h in headers) + "-+"
    header_row = "| " + " | ".join(h.ljust(col_widths[h]) for h in headers) + " |"

    print(border)
    print(header_row)
    print(border)
    for row in rows:
        print(format_row(row))
    print(border)




def main():
    print("📦 Simple-DBMS started")
    print("ℹ️  Type SQL statements ending in ';'. Type 'quit' to exit.\n")

    # 1. Load schema from /data
    schema = Schema.load("default")
    executor = Executor(schema)
    parser = SQLParser()

    if schema.tables:
        print("📂 Tables loaded:")
        for table_name in schema.tables:
            print(f"   - {table_name}")
    else:
        print("📂 No tables found in data/")
    

    # 2. REPL loop
    buffer = ""
    while True:
        try:
            line = input("> ").strip()
            if line.lower() == "quit":
                break

            buffer += " " + line

            if ";" in buffer:
                try:
                    start_time = time.time()

                    ast = parser.parse(buffer)
                    result = executor.execute(ast)

                    end_time = time.time()
                    duration_ms = (end_time - start_time) * 1000
                    print(f"{YELLOW}⏱ Execution Time: {duration_ms:.2f} ms{RESET}")

                    if result is not None:
                        print(f"{YELLOW}{len(result)} row(s) returned.{RESET}")
                        print_mysql_table(result)
                       


                except Exception as e:
                    print(f"{RED}❌ Error: {e}{RESET}")

                buffer = ""  # Clear buffer after execution
        except KeyboardInterrupt:
            break

    # 3. Save all tables before exit
    print("\n💾 Saving all tables...")
    schema.save()
    print("👋 Exiting Simple-DBMS. Goodbye!")

if __name__ == "__main__":
    main()

#create table students (id INT PRIMARY KEY, name TEXT, age INT);
#INSERT INTO students (id, name,age) VALUES (1, 'Alice', 24);
#CREATE TABLE order (oder_id INT PRIMARY KEY, price INT, student_id INT,FOREIGN KEY (student_id) REFERENCES students(id),age INT,FOREIGN KEY (age) REFERENCES students(age));
#CREATE TABLE order (oder_id INT PRIMARY KEY, price INT, student_id INT,FOREIGN KEY (student_id) REFERENCES students(id),amount INT);
#INSERT INTO order (oder_id, price, student_id, amount) VALUES (1, 20, 1, 5);
#INSERT INTO order (oder_id, price, student_id,amount) VALUES (2, 30, 1, 10);

#INSERT INTO order (oder_id, price, student_id) VALUES ('OID1001', 299.99, 12345);
#Error: invalid literal for int() with base 10: 'OID1001'

#DELETE FROM "order" WHERE oder_id = 2;
#DELETE FROM students WHERE id = 1;

## check primary key and foreign key constraint in update
#UPDATE "order" SET student_id = 3 WHERE student_id = 1;
#UPDATE students SET id = 25 WHERE id = 1;

#CREATE TABLE teacher (teacher_id INT PRIMARY KEY, name TEXT, age INT);
#INSERT INTO teacher (teacher_id, name, age) VALUES (1, 'Bob', 30);
#INSERT INTO teacher (teacher_id, name, age) VALUES (2, 'Charlie', 35);
#INSERT INTO teacher (teacher_id, name, age) VALUES (3, 'David', 40);
#INSERT INTO teacher (teacher_id, name, age) VALUES (4, 'Eve', 28);
#INSERT INTO teacher (teacher_id, name, age) VALUES (5, 'Frank', 45);
#DELETE FROM teacher WHERE teacher_id = 5;

#DELETE FROM teacher WHERE teacher_id = 4;
#DELETE FROM teacher WHERE teacher_id = 1;

#CREATE INDEX idx_age ON teacher(age);