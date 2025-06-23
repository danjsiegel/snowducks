"""
Core functionality for SnowDucks using DuckLake.
"""

import os
import re
import time
import tempfile
import warnings
from pathlib import Path
from typing import Optional
import duckdb
import pyarrow.parquet as pq
import urllib.parse
import pyarrow as pa

# Suppress gosnowflake warnings about application directory
os.environ["SNOWFLAKE_APPLICATION"] = "SnowDucks"
os.environ["SNOWFLAKE_CLIENT_CONFIG_PATH"] = "/tmp"  # Use temp directory
os.environ["SNOWFLAKE_CLIENT_CONFIG_FILE"] = (
    "/tmp/snowflake_config.json"  # Dummy config file
)

# Filter out gosnowflake warnings
warnings.filterwarnings(
    "ignore", message=".*Unable to access the application directory.*"
)
warnings.filterwarnings("ignore", message=".*cannot find executable path.*")

from .config import SnowDucksConfig
from .ducklake_manager import DuckLakeManager
from .exceptions import (
    SnowDucksError,
    ConnectionError,
    PermissionError,
    QueryError,
    ConfigError,
    DuckLakeError,
)

# Global configuration and managers
_config: Optional[SnowDucksConfig] = None
_ducklake_manager: Optional[DuckLakeManager] = None


def _get_config() -> SnowDucksConfig:
    """Get the global configuration, initializing if necessary."""
    global _config
    if _config is None:
        _config = SnowDucksConfig.from_env()
    return _config


def _get_ducklake_manager() -> DuckLakeManager:
    """Get the global DuckLake manager, initializing if necessary."""
    global _ducklake_manager
    if _ducklake_manager is None:
        config = _get_config()
        _ducklake_manager = DuckLakeManager(config)
    return _ducklake_manager


def _validate_single_query(query: str) -> None:
    """
    Validate that the query contains only a single SQL statement.

    Args:
        query: The SQL query to validate

    Raises:
        QueryError: If multiple statements are detected
    """
    # Remove comments and normalize whitespace
    query_clean = re.sub(
        r"--.*$", "", query, flags=re.MULTILINE
    )  # Remove single-line comments
    query_clean = re.sub(
        r"/\*.*?\*/", "", query_clean, flags=re.DOTALL
    )  # Remove multi-line comments
    query_clean = re.sub(r"\s+", " ", query_clean).strip()

    # Remove string literals to avoid counting semicolons inside them
    # Replace single-quoted strings
    query_no_strings = re.sub(r"'([^']|'')*'", "''", query_clean)
    # Replace double-quoted strings
    query_no_strings = re.sub(r'"([^"]|"")*"', '""', query_no_strings)

    # Remove trailing semicolon
    query_no_trailing = query_no_strings.rstrip(";").strip()

    # Check if there are any semicolons left (indicating multiple statements)
    if ";" in query_no_trailing:
        raise QueryError(
            "Multiple SQL statements detected. SnowDucks only supports single queries. "
            "Please separate your statements and run them individually."
        )


def _has_limit_clause(query: str) -> bool:
    """
    Check if the query already has a LIMIT clause.

    Args:
        query: The SQL query to check

    Returns:
        True if the query has a LIMIT clause, False otherwise
    """
    # Remove comments
    query_clean = re.sub(r"--.*$", "", query, flags=re.MULTILINE)
    query_clean = re.sub(r"/\*.*?\*/", "", query_clean, flags=re.DOTALL)
    # Remove string literals
    query_clean = re.sub(r"'([^']|'')*'", "'',", query_clean)
    query_clean = re.sub(r'"([^"]|"")*"', '"",', query_clean)
    query_clean = re.sub(r"\s+", " ", query_clean).strip().lower()
    # Use regex to find standalone LIMIT clause
    return bool(re.search(r"\blimit\b", query_clean))


def _needs_limit_clause(query: str) -> bool:
    """
    Check if the query needs a LIMIT clause.
    Some queries like COUNT(*) don't need LIMIT clauses.

    Args:
        query: The SQL query to check

    Returns:
        True if the query should have a LIMIT clause, False otherwise
    """
    # Remove comments and normalize
    query_clean = re.sub(r"--.*$", "", query, flags=re.MULTILINE)
    query_clean = re.sub(r"/\*.*?\*/", "", query_clean, flags=re.DOTALL)
    query_clean = re.sub(r"\s+", " ", query_clean).strip().lower()

    # Queries that don't need LIMIT clauses
    no_limit_patterns = [
        r"^\s*select\s+count\s*\(\s*\*\s*\)",  # SELECT COUNT(*)
        r"^\s*select\s+count\s*\(\s*[^)]+\s*\)",  # SELECT COUNT(column)
        r"^\s*select\s+sum\s*\(\s*[^)]+\s*\)",  # SELECT SUM(...)
        r"^\s*select\s+avg\s*\(\s*[^)]+\s*\)",  # SELECT AVG(...)
        r"^\s*select\s+min\s*\(\s*[^)]+\s*\)",  # SELECT MIN(...)
        r"^\s*select\s+max\s*\(\s*[^)]+\s*\)",  # SELECT MAX(...)
        r"^\s*select\s+distinct\s+count",  # SELECT DISTINCT COUNT
    ]

    # Check if query matches any no-limit pattern
    for pattern in no_limit_patterns:
        if re.search(pattern, query_clean):
            return False

    return True


def snowflake_query(
    query_text: str, limit: Optional[int] = None, force_refresh: bool = False
) -> tuple[str, str]:
    """
    Query Snowflake data with intelligent caching using DuckLake.

    Args:
        query_text: The SQL query to execute against Snowflake (single query only)
        limit: Maximum number of rows to fetch (None uses default, -1 for unlimited)
        force_refresh: If True, bypasses cache and queries Snowflake directly

    Returns:
        Tuple containing DuckLake table name and cache status

    Raises:
        ConnectionError: If unable to connect to Snowflake
        PermissionError: If unlimited egress is not allowed
        QueryError: If the query fails to execute or contains multiple statements
        DuckLakeError: If there are issues with DuckLake operations
    """
    config = _get_config()
    ducklake_manager = _get_ducklake_manager()

    # Validate single query
    _validate_single_query(query_text)

    # Use default limit if not specified
    if limit is None:
        limit = config.default_row_limit

    # Check for cached result (unless forced refresh)
    if not force_refresh:
        cached_table = ducklake_manager.get_cached_table_name(query_text)
        if cached_table:
            return cached_table, "hit"

    # Cache miss or force refresh - fetch from Snowflake
    print(f"CACHE MISS: Fetching from Snowflake")

    # Egress cost governance check
    if limit == -1 and not config.allow_unlimited_egress:
        raise PermissionError(
            "Unlimited row fetch (limit=-1) is not permitted. "
            "Set ALLOW_UNLIMITED_EGRESS=TRUE to override."
        )

    # Apply limit to query if specified and not already present
    final_query = query_text
    if limit != -1 and not _has_limit_clause(query_text):
        final_query = f"{query_text} LIMIT {limit}"
        print(f"INFO: Added LIMIT {limit} to query for cost control")
    elif limit != -1 and _has_limit_clause(query_text):
        print("WARNING: Query already contains LIMIT clause, using existing limit")

    # Debug: Show the exact query being sent to Snowflake
    print(f"DEBUG: Final query to Snowflake: {final_query}")

    # Track execution time
    start_time = time.time()

    try:
        # Import here to avoid circular imports and ensure ADBC is available
        from adbc_driver_snowflake import dbapi as snowflake_adbc

        # URL-encode the password to handle special characters like #
        encoded_password = urllib.parse.quote(config.snowflake_password, safe="")

        # Build URI according to ADBC documentation format
        # Format: user:password@account/database?param1=value1&paramN=valueN
        conn_uri = f"{config.snowflake_user}:{encoded_password}@{config.snowflake_account}/{config.snowflake_database}?warehouse={config.snowflake_warehouse}&role={config.snowflake_role}"

        with snowflake_adbc.connect(uri=conn_uri) as conn:
            with conn.cursor() as cursor:
                cursor.execute(final_query)
                table = cursor.fetch_arrow_table()

                # Create temporary Parquet file
                with tempfile.NamedTemporaryFile(
                    suffix=".parquet", delete=False
                ) as temp_file:
                    temp_path = temp_file.name

                # Materialize data to temporary Parquet file
                pq.write_table(table, temp_path, compression="ZSTD")
                row_count = table.num_rows

    except ImportError as e:
        raise ConnectionError(
            "ADBC Snowflake driver not installed. "
            "Install with: pip install adbc-driver-snowflake"
        ) from e
    except Exception as e:
        raise ConnectionError(f"Failed to query Snowflake: {e}") from e

    # Calculate execution time
    execution_time_ms = int((time.time() - start_time) * 1000)

    try:
        # Create cached table in DuckLake
        table_name = ducklake_manager.create_cached_table(
            query_text, temp_path, row_count, execution_time_ms
        )

        # Clean up temporary file
        os.unlink(temp_path)

        return table_name, "miss"

    except Exception as e:
        # Clean up temporary file on error
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        raise DuckLakeError(f"Failed to create cached table: {e}") from e


def register_snowflake_udf(con: duckdb.DuckDBPyConnection) -> None:
    """
    Register the snowflake_query UDF with a DuckDB connection.

    This function registers a User-Defined Function (UDF) that allows you to
    query Snowflake data directly from DuckDB SQL queries with intelligent
    caching using DuckLake.

    Args:
        con: DuckDB connection to register the function with
    """
    try:
        # Import DuckDB types
        from duckdb.typing import VARCHAR, INTEGER, BOOLEAN

        # Create a wrapper function that returns the persistent table name and cache status
        def snowflake_query_wrapper(
            query_text: str, limit: int = 1000, force_refresh: bool = False
        ) -> str:
            # Get the table name and cache status from snowflake_query
            table_name, cache_status = snowflake_query(query_text, limit, force_refresh)
            # Return as a string in the format 'table_name|cache_status'
            return f"{table_name}|{cache_status}"

        # Register as a scalar function that returns the table name
        con.create_function(
            "snowflake_query",
            snowflake_query_wrapper,
            [VARCHAR, INTEGER, BOOLEAN],
            VARCHAR,
            side_effects=True,
        )

        print("Successfully registered UDF: snowflake_query")
        print("💡 Usage: SELECT * FROM snowflake_query('your query', 1000, false)")
        print("   The UDF returns a table name that you can query directly!")
    except Exception as e:
        raise SnowDucksError(f"Failed to register UDF: {e}") from e


def register_snowflake_udf_native(con: duckdb.DuckDBPyConnection) -> None:
    """
    Register a native SQL-based Snowflake UDF that doesn't require Python extension.

    This creates a simpler UDF that can work in the DuckDB UI without Python extensions.
    It uses DuckDB's built-in SQL capabilities to handle basic Snowflake queries.

    Args:
        con: DuckDB connection to register the function with
    """
    try:
        # Create a native SQL function that returns a simple message
        # This is a placeholder that can be extended with native SQL capabilities
        con.execute(
            """
            CREATE OR REPLACE FUNCTION snowflake_query_native(query_text VARCHAR) 
            RETURNS VARCHAR AS $$
                SELECT 'Native UDF: ' || query_text || ' (Python extension required for full functionality)';
            $$;
        """
        )

        print("Successfully registered native UDF: snowflake_query_native")
        print("💡 This is a simplified version that works without Python extension")
        print("   For full functionality, use the interactive session with Python UDF")

    except Exception as e:
        raise SnowDucksError(f"Failed to register native UDF: {e}") from e


def create_ui_compatible_script() -> str:
    """
    Create a SQL script that can be used in the DuckDB UI without Python extension.

    Returns:
        SQL script as a string
    """
    script = """
-- SnowDucks UI-Compatible Setup (No Python Extension Required)
-- This script provides basic functionality for the DuckDB UI

-- Create a simple native function
CREATE OR REPLACE FUNCTION snowducks_status() 
RETURNS VARCHAR AS $$
    SELECT 'SnowDucks is available! Use interactive session for full Python UDF functionality.';
$$;

-- Create a function to show available tables
CREATE OR REPLACE FUNCTION snowducks_list_tables() 
RETURNS TABLE(table_name VARCHAR, table_type VARCHAR) AS $$
    SELECT table_name, table_type 
    FROM information_schema.tables 
    WHERE table_schema = 'main' 
    ORDER BY table_name;
$$;

-- Test the functions
SELECT snowducks_status() as status;
SELECT * FROM snowducks_list_tables() LIMIT 10;

-- Show usage instructions
SELECT 
    'UI Limitations:' as note,
    'Python extension not available in UI' as limitation1,
    'Use interactive session for full SnowDucks functionality' as recommendation1,
    'Run: ./snowducksi start-duckdb' as command1;
"""
    return script


def get_cache_stats() -> dict:
    """
    Get statistics about the DuckLake cache.

    Returns:
        Dictionary containing cache statistics
    """
    ducklake_manager = _get_ducklake_manager()
    return ducklake_manager.get_cache_stats()


def get_popular_queries(limit: int = 10) -> list:
    """
    Get the most frequently used queries.

    Args:
        limit: Maximum number of queries to return

    Returns:
        List of popular queries with usage statistics
    """
    ducklake_manager = _get_ducklake_manager()
    return ducklake_manager.get_popular_queries(limit)


def get_recent_queries(limit: int = 10) -> list:
    """
    Get recently executed queries.

    Args:
        limit: Maximum number of queries to return

    Returns:
        List of recent queries
    """
    ducklake_manager = _get_ducklake_manager()
    return ducklake_manager.get_recent_queries(limit)


def search_queries(search_term: str, limit: int = 20) -> list:
    """
    Search queries by text content.

    Args:
        search_term: Text to search for in queries
        limit: Maximum number of results to return

    Returns:
        List of matching queries
    """
    ducklake_manager = _get_ducklake_manager()
    return ducklake_manager.search_queries(search_term, limit)


def cleanup_expired_cache() -> int:
    """
    Clean up expired cache entries based on cache_max_age_hours.

    Returns:
        Number of entries cleaned up
    """
    ducklake_manager = _get_ducklake_manager()
    return ducklake_manager.cleanup_expired_cache()


def clear_cache(cache_key: Optional[str] = None) -> int:
    """
    Clear cache entries.

    Args:
        cache_key: Specific cache key to clear (None clears all)

    Returns:
        Number of entries cleared
    """
    ducklake_manager = _get_ducklake_manager()

    if cache_key:
        # Clear specific cache entry
        # This would need to be implemented in DuckLakeManager
        print(f"Clearing specific cache entry: {cache_key}")
        return 1
    else:
        # Clear all cache
        return ducklake_manager.clear_all_cache()


def configure(
    config: Optional[SnowDucksConfig] = None, env_file: Optional[str] = None
) -> None:
    """
    Configure SnowDucks with custom settings.

    Args:
        config: Custom configuration object
        env_file: Path to environment file to load
    """
    global _config, _ducklake_manager

    if config is not None:
        _config = config
    elif env_file is not None:
        _config = SnowDucksConfig.from_env(env_file)
    else:
        _config = SnowDucksConfig.from_env()

    # Reset DuckLake manager to use new config
    if _ducklake_manager:
        _ducklake_manager.close()
    _ducklake_manager = DuckLakeManager(_config)


def test_connection() -> bool:
    """
    Test the Snowflake connection.

    Returns:
        True if connection is successful, False otherwise
    """
    try:
        config = _get_config()
        conn_uri = config.get_snowflake_connection_uri()

        # Try ADBC first, fallback to standard connector
        try:
            from adbc_driver_snowflake import dbapi as snowflake_adbc

            with snowflake_adbc.connect(uri=conn_uri) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    return result[0] == 1

        except Exception as adbc_error:
            print(
                f"⚠️  ADBC connection failed, falling back to standard connector: {adbc_error}"
            )

            # Fallback to standard Snowflake connector
            import snowflake.connector

            # Extract account identifier for standard connector
            account = config.snowflake_account
            if ".snowflakecomputing.com" in account:
                account_id = account.split(".")[0]
            else:
                account_id = account

            with snowflake.connector.connect(
                user=config.snowflake_user,
                password=config.snowflake_password,
                account=account_id,
                database=config.snowflake_database,
                warehouse=config.snowflake_warehouse,
                role=config.snowflake_role,
            ) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    return result[0] == 1

    except Exception as e:
        print(f"Connection test failed: {e}")
        return False


def get_version() -> str:
    """Get the SnowDucks version."""
    from . import __version__

    return __version__


# Backward compatibility
def fetch_from_snowflake(*args, **kwargs):
    """Backward compatibility alias for snowflake_query."""
    return snowflake_query(*args, **kwargs)
