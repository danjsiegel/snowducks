#!/usr/bin/env python3
"""
Helper script to get the DuckLake database path from universal configuration.
Used by both CLI and UI to ensure they use the same database.
"""

import sys
import os

# Add the cli directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'cli'))

from snowducks.config import SnowDucksConfig

def main():
    """Get the DuckLake database path."""
    try:
        # Load configuration from .env file
        config = SnowDucksConfig.from_env()
        
        # Initialize the database with proper schema
        config.initialize_ducklake_database()
        
        # Return the database path
        db_path = config.get_duckdb_database_path()
        print(db_path)
        
    except Exception as e:
        print(f"Error getting database path: {e}", file=sys.stderr)
        print("Make sure you have run 'make init' and have a valid .env file", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main() 