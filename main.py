from catalog.schema import Schema
from executor import Executor
from sql_parser import SQLParser
import time
import sys
sys.setrecursionlimit(10000000)

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

    # Compute column widths safely
    col_widths = {}
    for h in headers:
        max_len = len(h)
        for row in rows:
            val = row.get(h)
            val_str = "" if val is None else str(val)
            if len(val_str) > max_len:
                max_len = len(val_str)
        col_widths[h] = max_len

    # Build output
    def format_row(row):
        return "| " + " | ".join(str(row.get(h) or "").ljust(col_widths[h]) for h in headers) + " |"

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
                    
                    if result is not None:
                        print_mysql_table(result)
                        print(f"{YELLOW}{len(result)} row(s) returned.{RESET}")

                    print(f"{YELLOW}⏱ Execution Time: {duration_ms:.2f} ms{RESET}")


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