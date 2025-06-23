#!/usr/bin/env python3
"""
Interactive DuckDB session with SnowDucks UDF registered.
This script starts a DuckDB interactive session with the snowflake_query UDF
already registered and ready to use.
"""

import duckdb
import re
import signal
import sys
import atexit
import os
import subprocess
import pyarrow as pa

# Add the cli directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from snowducks.core import register_snowflake_udf
from snowducks.config import SnowDucksConfig

# Global connection for cleanup
con = None


def cleanup_connection():
    """Clean up the DuckDB connection on exit."""
    global con
    if con:
        try:
            con.close()
        except Exception:
            pass


def signal_handler(signum, frame):
    """Handle interrupt signals."""
    print("\n👋 Goodbye!", file=sys.stderr)
    cleanup_connection()
    sys.exit(0)


def process_snowflake_query(con, query):
    """
    Process queries that use snowflake_query() in FROM clauses.
    Automatically calls the UDF to get table names and replaces them in the query.
    """
    # Find the start of snowflake_query function call
    start_match = re.search(
        r"SELECT\s+\*\s+FROM\s+snowflake_query\s*\(\s*", query, re.IGNORECASE
    )
    if not start_match:
        return query

    start_pos = start_match.end()

    # Find the matching closing parenthesis by counting parentheses
    paren_count = 1
    pos = start_pos
    while pos < len(query) and paren_count > 0:
        if query[pos] == "(":
            paren_count += 1
        elif query[pos] == ")":
            paren_count -= 1
        pos += 1

    if paren_count != 0:
        raise ValueError("Unmatched parentheses in snowflake_query function call")

    # Extract the arguments string
    args_str = query[start_pos : pos - 1].strip()

    # Parse arguments (simple comma-separated, but handle quoted strings)
    args = []
    current_arg = ""
    in_quotes = False
    quote_char = None
    paren_depth = 0

    for char in args_str:
        if char in ['"', "'"] and not in_quotes:
            in_quotes = True
            quote_char = char
            current_arg += char
        elif char == quote_char and in_quotes:
            in_quotes = False
            quote_char = None
            current_arg += char
        elif char == "(" and not in_quotes:
            paren_depth += 1
            current_arg += char
        elif char == ")" and not in_quotes:
            paren_depth -= 1
            current_arg += char
        elif char == "," and not in_quotes and paren_depth == 0:
            args.append(current_arg.strip())
            current_arg = ""
        else:
            current_arg += char

    if current_arg.strip():
        args.append(current_arg.strip())

    # Default values if not provided
    query_text = args[0].strip("\"'") if len(args) > 0 else ""
    limit = int(args[1]) if len(args) > 1 else 1000
    force_refresh = args[2].lower() == "true" if len(args) > 2 else False

    if not query_text:
        raise ValueError("No query text provided to snowflake_query()")

    print(f"🔄 Executing Snowflake query: {query_text}", file=sys.stderr)
    print(f"   Limit: {limit}, Force refresh: {force_refresh}", file=sys.stderr)

    # Call the UDF to get the table name and cache status
    result = con.execute(
        "SELECT snowflake_query(?, ?, ?)", [query_text, limit, force_refresh]
    )
    table_status = result.fetchone()[0]
    if "|" in table_status:
        table_name, cache_status = table_status.split("|", 1)
    else:
        table_name, cache_status = table_status, "unknown"

    if cache_status == "hit":
        print(f"✅ Using cached table: {table_name}", file=sys.stderr)
    elif cache_status == "miss":
        print(f"✅ Cached table created: {table_name}", file=sys.stderr)
    else:
        print(f"✅ Table: {table_name}", file=sys.stderr)

    # Replace the entire snowflake_query function call with the table name
    # Find the start and end of the snowflake_query function call
    start_match = re.search(
        r"SELECT\s+\*\s+FROM\s+snowflake_query\s*\(\s*", query, re.IGNORECASE
    )
    if start_match:
        start_pos = start_match.start()
        # Find the matching closing parenthesis
        paren_count = 1
        pos = start_match.end()
        while pos < len(query) and paren_count > 0:
            if query[pos] == "(":
                paren_count += 1
            elif query[pos] == ")":
                paren_count -= 1
            pos += 1

        if paren_count == 0:
            # Replace the entire function call
            new_query = query[:start_pos] + f"SELECT * FROM {table_name}" + query[pos:]
            print(f"🔍 Executing: {new_query}", file=sys.stderr)
            return new_query

    # Fallback to original query if replacement fails
    return query


def is_interactive():
    """Check if we're running in interactive mode."""
    return sys.stdin.isatty() and sys.stdout.isatty()


def print_arrow_table(table: pa.Table):
    """Pretty-print a pyarrow Table to the console (without pandas)."""
    if table.num_rows == 0:
        print("Query executed successfully (no results to display)")
        return
    # Print column headers
    headers = [col for col in table.schema.names]
    print("\t".join(headers))
    # Print rows
    for row in table.to_pylist():
        print("\t".join(str(row.get(col, "")) for col in headers))


def main():
    global con

    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Register cleanup function
    atexit.register(cleanup_connection)

    print(
        "🚀 Starting DuckDB interactive session with SnowDucks UDF...", file=sys.stderr
    )
    print(file=sys.stderr)

    try:
        # Load configuration
        config = SnowDucksConfig.from_env()

        # Check if PostgreSQL is configured
        if config.postgres_host:
            # Use in-memory DuckDB with PostgreSQL metadata
            print("🐘 Using PostgreSQL for metadata storage...", file=sys.stderr)
            con = duckdb.connect(":memory:")

            # Install and load DuckLake extension
            con.execute("INSTALL ducklake")
            con.execute("LOAD ducklake")

            # Attach DuckLake with PostgreSQL
            ducklake_attach_string = config.get_ducklake_attach_string()
            data_path = str(config.ducklake_data_path)
            con.execute(
                f"ATTACH '{ducklake_attach_string}' AS snowducks_ducklake (DATA_PATH '{data_path}')"
            )

        else:
            # Use local DuckDB file (legacy mode)
            print("📁 Using local DuckDB file for metadata...", file=sys.stderr)
            db_path = subprocess.check_output(
                [sys.executable, "../utils/get_db_path.py"],
                cwd=os.path.dirname(__file__),
                text=True,
            ).strip()
            con = duckdb.connect(db_path)

        # Register our UDF
        register_snowflake_udf(con)

        print("✅ SnowDucks UDF registered successfully!", file=sys.stderr)
        print(file=sys.stderr)

        # Show that UDF is ready
        result = con.execute("SELECT 'SnowDucks UDF is ready!' as status")
        arrow_table = result.fetch_arrow_table()
        print_arrow_table(arrow_table)

        # Check if we're in interactive mode
        if not is_interactive():
            print(
                "⚠️  Non-interactive mode detected. Use interactive mode for best experience.",
                file=sys.stderr,
            )
            print("   Run: ./snowducks cli (without piping input)", file=sys.stderr)
            return

        print("📖 Usage Examples:", file=sys.stderr)
        print("  -- Direct table-like usage (recommended):", file=sys.stderr)
        print(
            "     SELECT * FROM snowflake_query('SELECT * FROM my_table LIMIT 100')",
            file=sys.stderr,
        )
        print(file=sys.stderr)
        print("  -- With custom limit:", file=sys.stderr)
        print(
            "     SELECT * FROM snowflake_query('SELECT * FROM my_table', 500)",
            file=sys.stderr,
        )
        print(file=sys.stderr)
        print("  -- Force refresh cache:", file=sys.stderr)
        print(
            "     SELECT * FROM snowflake_query('SELECT * FROM my_table', 1000, true)",
            file=sys.stderr,
        )
        print(file=sys.stderr)
        print("  -- Use in CTEs and joins:", file=sys.stderr)
        print(
            "     WITH data AS (SELECT * FROM snowflake_query('SELECT ...', 1000, false))",
            file=sys.stderr,
        )
        print("     SELECT * FROM data WHERE column > 100", file=sys.stderr)
        print(file=sys.stderr)
        print(
            "💡 The UDF works like a real table - use it in FROM, JOIN, CTEs, etc!",
            file=sys.stderr,
        )
        print(file=sys.stderr)
        print("🔧 Type 'quit' to exit the DuckDB session.", file=sys.stderr)
        print(file=sys.stderr)

        # Start interactive mode
        print("\n🔄 Starting interactive mode (Ctrl+C to exit)...", file=sys.stderr)
        print("duckdb> ", end="", flush=True, file=sys.stderr)

        while True:
            try:
                # Read input from stdin (not stderr)
                line = input()

                if line.lower().strip() in ["quit", "exit", "q"]:
                    print("👋 Goodbye!", file=sys.stderr)
                    break

                if not line.strip():
                    print("duckdb> ", end="", flush=True, file=sys.stderr)
                    continue

                # Process the query
                processed_query = process_snowflake_query(con, line)

                # Execute the query
                result = con.execute(processed_query)

                # Display results
                arrow_table = result.fetch_arrow_table()
                print_arrow_table(arrow_table)

                print("duckdb> ", end="", flush=True, file=sys.stderr)

            except KeyboardInterrupt:
                print("\n👋 Goodbye!", file=sys.stderr)
                break
            except EOFError:
                print("\n👋 Goodbye!", file=sys.stderr)
                break
            except Exception as e:
                print(f"❌ Error: {e}", file=sys.stderr)
                print("duckdb> ", end="", flush=True, file=sys.stderr)

    except Exception as e:
        print(f"❌ Failed to start DuckDB session: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
