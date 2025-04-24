"""Entry point for the Simple-DBMS command-line interface.

This script initializes the database schema, starts a REPL for SQL command input,
executes queries via the executor, and displays results in a MySQL-like format.
"""

# Standard library imports
import sys
import time

# Local application imports
from catalog.schema import Schema
from executor import Executor
from sql_parser import SQLParser

# Increase the recursion limit to support deep AST processing during parsing
sys.setrecursionlimit(10_000_000)

# ANSI color codes for styled terminal output
RESET = "\033[0m"      # Reset all attributes
RED = "\033[91m"       # Bright red for errors
GREEN = "\033[92m"     # Bright green (currently unused)
YELLOW = "\033[93m"    # Bright yellow for informational messages

def print_mysql_table(rows: list[dict]):
    """Print query results in a MySQL-style table format.

    Args:
        rows (list[dict]): List of rows, where each row is a dict mapping column names to values.
    """
    # If there are no results, print an empty set message
    if not rows:
        print("(empty set)")
        return

    # Extract column headers from the first row
    headers = list(rows[0].keys())

    # Calculate the maximum width for each column based on header and cell contents
    col_widths: dict[str, int] = {}
    for header in headers:
        max_width = len(header)
        for row in rows:
            cell = row.get(header)
            cell_str = "" if cell is None else str(cell)
            if len(cell_str) > max_width:
                max_width = len(cell_str)
        col_widths[header] = max_width

    # Helper function to format a single row
    def format_row(row: dict) -> str:
        cells = [
            str(row.get(header) or "").ljust(col_widths[header])
            for header in headers
        ]
        return "| " + " | ".join(cells) + " |"

    # Construct table border and header row
    border = "+-" + "-+-".join("-" * col_widths[header] for header in headers) + "-+"
    header_row = "| " + " | ".join(header.ljust(col_widths[header]) for header in headers) + " |"

    # Print the complete table
    print(border)
    print(header_row)
    print(border)
    for row in rows:
        print(format_row(row))
    print(border)

def main():
    """Main entry point: initialize components and start the SQL REPL."""
    # Startup banner
    print("📦 Simple-DBMS started")
    print("ℹ️  Type SQL statements ending in ';'. Type 'quit' to exit.\n")

    # 1. Load or create the default schema
    schema = Schema.load("default")
    executor = Executor(schema)
    parser = SQLParser()

    # Display loaded tables or indicate that no tables are present
    if schema.tables:
        print("📂 Tables loaded:")
        for table_name in schema.tables:
            print(f"   - {table_name}")
    else:
        print("📂 No tables found in data/")

    # 2. Enter the Read-Eval-Print Loop (REPL)
    query_buffer = ""
    while True:
        try:
            # Read a line of input and strip whitespace
            line = input("> ").strip()
            # Exit if the user types 'quit'
            if line.lower() == "quit":
                break

            # Accumulate lines until a semicolon is encountered
            query_buffer += " " + line
            if ";" in query_buffer:
                try:
                    # Measure execution time
                    start_time = time.time()
                    # Parse SQL into an AST and execute it
                    ast = parser.parse(query_buffer)
                    result = executor.execute(ast)
                    end_time = time.time()

                    # Display results if any rows are returned
                    if result is not None:
                        print_mysql_table(result)
                        print(f"{YELLOW}{len(result)} row(s) returned.{RESET}")

                    # Print execution duration in milliseconds
                    duration_ms = (end_time - start_time) * 1000
                    print(f"{YELLOW}⏱ Execution Time: {duration_ms:.2f} ms{RESET}")

                except Exception as error:
                    # Print errors in red
                    print(f"{RED}❌ Error: {error}{RESET}")

                # Clear buffer after executing a complete statement
                query_buffer = ""

        except KeyboardInterrupt:
            # Handle Ctrl+C gracefully
            break

    # 3. Persist all tables to disk before exiting
    print("\n💾 Saving all tables...")
    schema.save()
    print("👋 Exiting Simple-DBMS. Goodbye!")

if __name__ == "__main__":
    main()
