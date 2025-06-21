#!/usr/bin/env python3
"""
SnowDucks CLI - Instant SQL for Snowflake with DuckLake Caching
"""

import sys
import os
from pathlib import Path

from .core import (
    configure,
    test_connection,
    get_cache_stats,
    clear_cache,
    register_snowflake_udf
)
from .config import SnowDucksConfig
from .utils import generate_normalized_query_hash


def main():
    """Main CLI entry point."""
    if len(sys.argv) < 2:
        _print_help()
        return

    command = sys.argv[1].lower()

    if command == "configure":
        _handle_configure()
    elif command == "test":
        _handle_test()
    elif command == "stats":
        _handle_stats()
    elif command == "clear-cache":
        _handle_clear_cache()
    elif command == "start-duckdb":
        _handle_start_duckdb()
    elif command == "query":
        _handle_query()
    else:
        print(f"❌ Unknown command: {command}")
        _print_help()


def _handle_configure():
    """Handle configure command."""
    print("🔧 Configuring SnowDucks...")
    configure()
    print("✅ Configuration complete!")


def _handle_test():
    """Handle test command."""
    print("🔌 Testing Snowflake connection...")
    if test_connection():
        print("✅ Connection successful!")
    else:
        print("❌ Connection failed!")
        sys.exit(1)


def _handle_stats():
    """Handle stats command."""
    print("📊 SnowDucks Cache Statistics:")
    try:
        stats = get_cache_stats()
        print(f"  Total Queries: {stats['total_queries']}")
        print(f"  Cache Entries: {stats['total_cache_entries']}")
        print(f"  Cache Max Age: {stats['cache_max_age_hours']} hours")
        print(f"  Metadata Path: {stats['ducklake_metadata_path']}")
        print(f"  Data Path: {stats['ducklake_data_path']}")
    except Exception as e:
        print(f"❌ Error getting stats: {e}")


def _handle_clear_cache():
    """Handle clear-cache command."""
    print("🗑️  Clearing SnowDucks cache...")
    try:
        clear_cache()
        print("✅ Cache cleared successfully!")
    except Exception as e:
        print(f"❌ Error clearing cache: {e}")


def _handle_start_duckdb():
    """Handle start-duckdb command."""
    print("🚀 Starting DuckDB interactive session with SnowDucks UDF...")
    print()
    print("The snowflake_query UDF has been registered!")
    print()
    print("📖 Usage Examples:")
    print("  -- Direct table-like usage (recommended):")
    print("     SELECT * FROM snowflake_query('SELECT * FROM my_table LIMIT 100')")
    print()
    print("  -- With custom limit:")
    print("     SELECT * FROM snowflake_query('SELECT * FROM my_table', 500)")
    print()
    print("  -- Force refresh cache:")
    print("     SELECT * FROM snowflake_query('SELECT * FROM my_table', 1000, true)")
    print()
    print("  -- Use in CTEs and joins:")
    print("     WITH data AS (SELECT * FROM snowflake_query('SELECT ...', 1000, false))")
    print("     SELECT * FROM data WHERE column > 100")
    print()
    print("💡 The UDF works like a real table - use it in FROM, JOIN, CTEs, etc!")
    print()
    print("🔧 Type 'quit' to exit the DuckDB session.")
    print()
    
    # Start DuckDB interactive session
    import subprocess
    
    # Get the path to the interactive script
    script_path = Path(__file__).parent.parent / "start_duckdb_interactive.py"
    
    try:
        # Start the interactive script
        subprocess.run([sys.executable, str(script_path)], check=True)
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
    except Exception as e:
        print(f"❌ Error starting DuckDB: {e}")


def _handle_query():
    """Handle query command."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Execute a Snowflake query')
    parser.add_argument('--query', required=True, help='SQL query to execute')
    parser.add_argument('--limit', type=int, default=1000, help='Row limit (default: 1000)')
    parser.add_argument('--force-refresh', action='store_true', help='Force refresh cache')
    parser.add_argument('--no-force-refresh', action='store_true', help='Use cache if available')
    
    # Parse arguments from sys.argv[2:] to skip the 'query' command
    args = parser.parse_args(sys.argv[2:])
    
    try:
        # Import here to avoid circular imports
        from .core import snowflake_query
        
        force_refresh = args.force_refresh
        if args.no_force_refresh:
            force_refresh = False
            
        print(f"Executing query: {args.query}")
        print(f"Limit: {args.limit}")
        print(f"Force refresh: {force_refresh}")
        
        table_name, status = snowflake_query(
            query_text=args.query,
            limit=args.limit,
            force_refresh=force_refresh
        )
        
        print(f"Cache table: {table_name}")
        print(f"Status: {status}")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def _print_help():
    """Print CLI help information."""
    print("🦆 SnowDucks CLI - Instant SQL for Snowflake with DuckLake Caching")
    print()
    print("Commands:")
    print("  snowducksi configure     Configure SnowDucks with your Snowflake credentials")
    print("  snowducksi test          Test your Snowflake connection")
    print("  snowducksi stats         Show cache statistics")
    print("  snowducksi clear-cache   Clear all cached data")
    print("  snowducksi start-duckdb  Start interactive DuckDB session with UDF")
    print("  snowducksi query         Execute a Snowflake query")
    print()
    print("Examples:")
    print("  # Configure your Snowflake connection")
    print("  ./snowducksi configure")
    print()
    print("  # Start interactive SQL session")
    print("  ./snowducksi start-duckdb")
    print()
    print("  # In DuckDB, use the UDF like a table:")
    print("  SELECT * FROM snowflake_query('SELECT COUNT(*) FROM my_table', 1000, false)")
    print()
    print("💡 The interactive session provides the best experience with full UDF support!")


if __name__ == "__main__":
    main() 