
from catalog.schema import Schema
from executor import Executor
from sql_parser import SQLParser
import time

# ANSI color codes
RESET = "\033[0m"
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"

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
                        for row in result:
                            print(row)

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
#INSERT INTO students (id, name,age) VALUES (1, 'Alice', 24);
#CREATE TABLE order (oder_id INT PRIMARY KEY, price INT, student_id INT,FOREIGN KEY (student_id) REFERENCES Student(id),age INT,FOREIGN KEY (age) REFERENCES Student(age));
#INSERT INTO order (oder_id, price, student_id) VALUES (1, 20, 1);