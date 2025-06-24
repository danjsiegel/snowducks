"""
DuckLake manager for SnowDucks - handles DuckLake database operations and cache recency.
"""

import time
import fcntl
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, TextIO
import duckdb

from .config import SnowDucksConfig
from .exceptions import DuckLakeError
from .utils import (
    generate_normalized_query_hash,
    parse_query_metadata,
)


class DuckLakeManager:
    """Manages DuckLake database operations for SnowDucks."""

    def __init__(self, config: SnowDucksConfig):
        self.config = config
        self.duckdb_connection: Optional[duckdb.DuckDBPyConnection] = None
        self.ducklake_attached = False
        self.lock_file: Optional[TextIO] = None
        self.lock_fd: Optional[int] = None

        # Initialize DuckLake
        self._init_ducklake()

    def _init_ducklake(self) -> None:
        """Initialize DuckLake connection and metadata tables."""
        try:
            # Acquire lock before connecting to database
            self._acquire_lock()
            # Always use in-memory DuckDB for Postgres metadata
            self.duckdb_connection = duckdb.connect(":memory:")
            # Install DuckLake and Postgres extensions
            self.duckdb_connection.execute("INSTALL ducklake")
            self.duckdb_connection.execute("LOAD ducklake")
            self.duckdb_connection.execute("INSTALL postgres")
            self.duckdb_connection.execute("LOAD postgres")
            # Attach DuckLake using the configuration
            ducklake_attach_string = self.config.get_ducklake_attach_string()
            data_path = str(self.config.ducklake_data_path)
            self.duckdb_connection.execute(
                f"ATTACH '{ducklake_attach_string}' AS snowducks_ducklake "
                f"(DATA_PATH '{data_path}')"
            )
            self.ducklake_attached = True
            # Create metadata tables if they don't exist (don't drop existing ones)
            self._create_metadata_tables()
            # Commit the initialization
            self.duckdb_connection.commit()
        except Exception as e:
            # Release lock on error
            self._release_lock()
            raise DuckLakeError(f"Failed to initialize DuckLake: {e}") from e

    def _get_schema_name(self) -> str:
        return "snowducks_ducklake"

    def _drop_metadata_tables(self) -> None:
        """Drop existing metadata tables to ensure clean schema."""
        if not self.duckdb_connection:
            return
        try:
            schema_name = self._get_schema_name()
            self.duckdb_connection.execute(
                f"DROP TABLE IF EXISTS {schema_name}.snowducks_queries"
            )
            self.duckdb_connection.execute(
                f"DROP TABLE IF EXISTS {schema_name}.snowducks_cache_metadata"
            )
            self.duckdb_connection.execute(
                f"DROP TABLE IF EXISTS {schema_name}.snowducks_users"
            )
        except Exception as e:
            print(f"Warning: Failed to drop metadata tables: {e}")

    def _create_metadata_tables(self) -> None:
        """Create metadata tables for tracking queries and cache information."""
        if not self.duckdb_connection:
            return
        # Determine the schema name based on the configuration
        schema_name = self._get_schema_name()

        # Create queries table
        self.duckdb_connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.snowducks_queries (
                query_hash VARCHAR,
                query_text TEXT NOT NULL,
                first_seen TIMESTAMP NOT NULL,
                last_used TIMESTAMP NOT NULL,
                usage_count INTEGER DEFAULT 1,
                avg_execution_time_ms INTEGER,
                total_rows_fetched BIGINT,
                created_by VARCHAR,
                last_refresh TIMESTAMP,
                cache_max_age_hours INTEGER
            )
        """
        )

        # Create query_metadata table for storing LIMIT and other query details
        self.duckdb_connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.snowducks_query_metadata (
                query_hash VARCHAR,
                original_query TEXT NOT NULL,
                query_without_limit TEXT NOT NULL,
                limit_value INTEGER,
                has_limit BOOLEAN NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
        """
        )

        # Create cache_metadata table
        self.duckdb_connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.snowducks_cache_metadata (
                query_hash VARCHAR,
                table_name VARCHAR NOT NULL,
                snapshot_id BIGINT,
                file_count INTEGER,
                total_size_bytes BIGINT,
                row_count BIGINT,
                created_at TIMESTAMP NOT NULL,
                expires_at TIMESTAMP,
                created_by VARCHAR
            )
        """
        )

        # Create users table
        self.duckdb_connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.snowducks_users (
                user_id VARCHAR,
                first_seen TIMESTAMP NOT NULL,
                last_seen TIMESTAMP NOT NULL,
                total_queries INTEGER DEFAULT 0,
                environment VARCHAR
            )
        """
        )

    def _generate_query_hash(self, query_text: str) -> str:
        """Generate a hash for the query text using shared utility (including LIMIT)."""
        return generate_normalized_query_hash(query_text)

    def _parse_query_metadata(self, query_text: str) -> Dict[str, Any]:
        """Parse query text and extract metadata including LIMIT information."""
        return parse_query_metadata(query_text)

    def _get_user_id(self) -> str:
        """Get current user ID."""
        import getpass

        try:
            return getpass.getuser()
        except Exception:
            return "unknown"

    def _is_cache_fresh(self, query_hash: str) -> bool:
        """Check if cached data is fresh based on cache_max_age_hours."""
        if self.config.cache_force_refresh or not self.duckdb_connection:
            return False

        schema_name = self._get_schema_name()
        result = self.duckdb_connection.execute(
            f"""
            SELECT last_refresh, cache_max_age_hours
            FROM {schema_name}.snowducks_queries
            WHERE query_hash = ?
        """,
            [query_hash],
        ).fetchone()

        if not result:
            return False

        last_refresh, max_age_hours = result
        if not last_refresh:
            return False

        # Ensure last_refresh is timezone-aware
        if last_refresh.tzinfo is None:
            last_refresh = last_refresh.replace(tzinfo=timezone.utc)

        # Check if data is within the max age window
        max_age = timedelta(hours=max_age_hours)
        return datetime.now(timezone.utc) - last_refresh < max_age

    def _table_exists(self, fq_table_name: str) -> bool:
        """Check if a table exists in DuckLake."""
        if not self.duckdb_connection:
            return False
        schema_name, table_name = fq_table_name.split(".", 1)

        # Check in the expected schema first
        result = self.duckdb_connection.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = ? AND table_name = ?
        """,
            [schema_name, table_name],
        ).fetchone()

        if result and result[0] and result[0] > 0:
            return True

        # Fallback: check in main schema (for old cached tables)
        result = self.duckdb_connection.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = ?
        """,
            [table_name],
        ).fetchone()

        return bool(result and result[0] and result[0] > 0)

    def get_cached_table_name(self, query_text: str) -> Optional[str]:
        """Get the cached table name for a query if it exists and is fresh."""
        query_hash = self._generate_query_hash(query_text)
        schema_name = self._get_schema_name()
        fq_table_name = f"{schema_name}.{query_hash}"

        # Check if table exists and cache is fresh
        table_exists = self._table_exists(fq_table_name)
        cache_fresh = self._is_cache_fresh(query_hash)

        if not table_exists or not cache_fresh:
            return None

        # Update last_used timestamp for cache hit
        self._update_cache_usage(query_hash)

        return fq_table_name

    def _check_limit_metadata(self, query_hash: str, limit_value: int) -> bool:
        """Check if the cached query has the same LIMIT value."""
        if not self.duckdb_connection:
            return False
        schema_name = self._get_schema_name()
        result = self.duckdb_connection.execute(
            f"""
            SELECT limit_value FROM {schema_name}.snowducks_query_metadata
            WHERE query_hash = ?
        """,
            [query_hash],
        ).fetchone()

        if not result:
            return False

        cached_limit = result[0]
        return cached_limit == limit_value if cached_limit else False

    def _update_cache_usage(self, query_hash: str) -> None:
        """Update the last_used timestamp and usage count for a cache hit."""
        if not self.duckdb_connection:
            return
        now = datetime.now(timezone.utc)
        schema_name = self._get_schema_name()
        self.duckdb_connection.execute(
            f"""
            UPDATE {schema_name}.snowducks_queries
            SET last_used = ?, usage_count = usage_count + 1
            WHERE query_hash = ?
        """,
            [now, query_hash],
        )
        self.duckdb_connection.commit()

    def create_cached_table(
        self,
        query_text: str,
        data_path: str,
        row_count: int,
        execution_time_ms: Optional[int] = None,
    ) -> str:
        """Create a cached table in DuckLake from a Parquet file."""
        if not self.duckdb_connection:
            raise DuckLakeError("DuckDB connection not available")

        query_hash = self._generate_query_hash(query_text)
        table_name = query_hash
        schema_name = self._get_schema_name()
        fq_table_name = f"{schema_name}.{table_name}"

        # Parse query metadata
        metadata = self._parse_query_metadata(query_text)

        try:
            # Create table from Parquet file using a more robust approach
            create_sql = f"""
                CREATE OR REPLACE TABLE {fq_table_name} AS
                SELECT * FROM read_parquet('{data_path}')
            """
            self.duckdb_connection.execute(create_sql)

            # Get table statistics
            file_info = self.duckdb_connection.execute(
                f"""
                SELECT COUNT(*) as file_count
                FROM glob('{data_path}')
            """
            ).fetchone()
            total_size = None

            # Record query metadata
            now = datetime.now(timezone.utc)
            user_id = self._get_user_id()

            # Delete existing query record and insert new one
            self.duckdb_connection.execute(
                f"DELETE FROM {schema_name}.snowducks_queries WHERE query_hash = ?",
                [query_hash],
            )
            self.duckdb_connection.execute(
                f"""
                INSERT INTO {schema_name}.snowducks_queries
                (query_hash, query_text, first_seen, last_used, usage_count,
                 avg_execution_time_ms, total_rows_fetched, created_by,
                 last_refresh, cache_max_age_hours)
                VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
            """,
                [
                    query_hash,
                    metadata["query_without_limit"],
                    now,
                    now,
                    execution_time_ms,
                    row_count,
                    user_id,
                    now,
                    self.config.cache_max_age_hours,
                ],
            )

            # Store query metadata including LIMIT information
            self.duckdb_connection.execute(
                f"DELETE FROM {schema_name}.snowducks_query_metadata "
                "WHERE query_hash = ?",
                [query_hash],
            )
            self.duckdb_connection.execute(
                f"""
                INSERT INTO {schema_name}.snowducks_query_metadata
                (query_hash, original_query, query_without_limit,
                 limit_value, has_limit, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                [
                    query_hash,
                    metadata["original_query"],
                    metadata["query_without_limit"],
                    metadata["limit_value"],
                    metadata["has_limit"],
                    now,
                ],
            )

            # Delete existing cache metadata and insert new one
            self.duckdb_connection.execute(
                f"DELETE FROM {schema_name}.snowducks_cache_metadata "
                "WHERE query_hash = ?",
                [query_hash],
            )

            # Calculate expiration time based on cache_max_age_hours
            if self.config.cache_max_age_hours > 0:
                expires_at = now + timedelta(hours=self.config.cache_max_age_hours)
            else:
                expires_at = None

            file_count = file_info[0] if file_info else 0
            self.duckdb_connection.execute(
                f"""
                INSERT INTO {schema_name}.snowducks_cache_metadata
                (query_hash, table_name, file_count, total_size_bytes,
                 row_count, created_at, expires_at, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                [
                    query_hash,
                    fq_table_name,
                    file_count,
                    total_size,
                    row_count,
                    now,
                    expires_at,
                    user_id,
                ],
            )

            # Update user stats
            self._update_user_stats(user_id, now)

            # Commit all metadata changes
            self.duckdb_connection.commit()

            return fq_table_name

        except Exception as e:
            raise DuckLakeError(f"Failed to create cached table: {e}") from e

    def _update_user_stats(self, user_id: str, now: datetime) -> None:
        """Update user statistics."""
        if not self.duckdb_connection:
            return
        schema_name = self._get_schema_name()
        # Delete existing user record and insert new one
        self.duckdb_connection.execute(
            f"DELETE FROM {schema_name}.snowducks_users WHERE user_id = ?",
            [user_id],
        )
        self.duckdb_connection.execute(
            f"""
            INSERT INTO {schema_name}.snowducks_users
            (user_id, first_seen, last_seen, total_queries, environment)
            VALUES (?, ?, ?, 1, ?)
        """,
            [user_id, now, now, self.config.deployment_mode],
        )

    def get_popular_queries(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get the most frequently used queries."""
        if not self.duckdb_connection:
            return []
        schema_name = self._get_schema_name()
        result = self.duckdb_connection.execute(
            f"""
            SELECT query_hash, query_text, usage_count,
                   avg_execution_time_ms, last_used
            FROM {schema_name}.snowducks_queries
            ORDER BY usage_count DESC, last_used DESC
            LIMIT ?
        """,
            [limit],
        ).fetchall()

        return [
            {
                "query_hash": row[0],
                "query_text": row[1],
                "usage_count": row[2],
                "avg_execution_time_ms": row[3],
                "last_used": row[4],
            }
            for row in result
        ]

    def get_recent_queries(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recently executed queries."""
        if not self.duckdb_connection:
            return []
        schema_name = self._get_schema_name()
        result = self.duckdb_connection.execute(
            f"""
            SELECT query_hash, query_text, usage_count, last_used
            FROM {schema_name}.snowducks_queries
            ORDER BY last_used DESC
            LIMIT ?
        """,
            [limit],
        ).fetchall()

        return [
            {
                "query_hash": row[0],
                "query_text": row[1],
                "usage_count": row[2],
                "last_used": row[3],
            }
            for row in result
        ]

    def search_queries(self, search_term: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Search queries by text."""
        if not self.duckdb_connection:
            return []
        schema_name = self._get_schema_name()
        result = self.duckdb_connection.execute(
            f"""
            SELECT query_hash, query_text, usage_count, last_used
            FROM {schema_name}.snowducks_queries
            WHERE query_text LIKE ?
            ORDER BY last_used DESC
            LIMIT ?
        """,
            [f"%{search_term}%", limit],
        ).fetchall()

        return [
            {
                "query_hash": row[0],
                "query_text": row[1],
                "usage_count": row[2],
                "last_used": row[3],
            }
            for row in result
        ]

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get statistics about the cache."""
        if not self.duckdb_connection:
            return {}
        schema_name = self._get_schema_name()
        result = self.duckdb_connection.execute(
            f"""
            SELECT COUNT(*), SUM(row_count), SUM(total_size_bytes)
            FROM {schema_name}.snowducks_cache_metadata
        """
        ).fetchone()
        if not result:
            return {"table_count": 0, "total_rows": 0, "total_size_bytes": 0}
        return {
            "table_count": result[0] if result[0] is not None else 0,
            "total_rows": result[1] if result[1] is not None else 0,
            "total_size_bytes": result[2] if result[2] is not None else 0,
        }

    def cleanup_expired_cache(self) -> int:
        """Clean up expired cache entries based on cache_max_age_hours."""
        if not self.duckdb_connection:
            return 0
        schema_name = self._get_schema_name()
        cutoff_time = datetime.now(timezone.utc) - timedelta(
            hours=self.config.cache_max_age_hours
        )

        # Get expired query hashes
        expired_queries = self.duckdb_connection.execute(
            f"""
            SELECT query_hash, table_name
            FROM {schema_name}.snowducks_cache_metadata
            WHERE expires_at IS NOT NULL AND expires_at < ?
        """,
            [cutoff_time],
        ).fetchall()

        cleaned_count = 0
        for query_hash, table_name in expired_queries:
            try:
                # Drop the cached table
                self.duckdb_connection.execute(f"DROP TABLE IF EXISTS {table_name}")

                # Remove metadata
                self.duckdb_connection.execute(
                    f"DELETE FROM {schema_name}.snowducks_cache_metadata "
                    "WHERE query_hash = ?",
                    [query_hash],
                )

                cleaned_count += 1
            except Exception as e:
                print(
                    f"Warning: Failed to clean up expired cache for {query_hash}: {e}"
                )

        return cleaned_count

    def clear_all_cache(self) -> int:
        """Clear all cached tables and metadata."""
        if not self.duckdb_connection:
            return 0
        schema_name = self._get_schema_name()

        # Get all cached table names
        cached_tables = self.duckdb_connection.execute(
            f"""
            SELECT table_name FROM {schema_name}.snowducks_cache_metadata
        """
        ).fetchall()

        cleaned_count = 0
        for (table_name,) in cached_tables:
            try:
                # Drop the cached table
                self.duckdb_connection.execute(f"DROP TABLE IF EXISTS {table_name}")
                cleaned_count += 1
            except Exception as e:
                print(f"Warning: Failed to drop table {table_name}: {e}")

        # Clear metadata
        self.duckdb_connection.execute(
            f"DELETE FROM {schema_name}.snowducks_cache_metadata"
        )

        return cleaned_count

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self.duckdb_connection:
            self.duckdb_connection.close()
            self.duckdb_connection = None

        # Release the lock
        self._release_lock()

    def _acquire_lock(self) -> None:
        """Acquire a file-based lock to ensure single-process access."""
        lock_path = self.config.ducklake_metadata_path.parent / "snowducks.lock"

        # Create lock file if it doesn't exist
        self.lock_file = open(lock_path, "w")
        if self.lock_file is None:
            raise DuckLakeError("Failed to create lock file")

        self.lock_fd = self.lock_file.fileno()

        # Try to acquire exclusive lock with timeout
        max_attempts = 30  # 30 seconds timeout
        for attempt in range(max_attempts):
            try:
                if self.lock_fd is not None:
                    fcntl.flock(self.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    return  # Lock acquired successfully
            except (OSError, IOError):
                if attempt < max_attempts - 1:
                    time.sleep(1)  # Wait 1 second before retrying
                else:
                    raise DuckLakeError(
                        "Could not acquire database lock after "
                        f"{max_attempts} seconds. Another process may be "
                        "using the database."
                    )

    def _release_lock(self) -> None:
        """Release the file-based lock."""
        if self.lock_fd is not None:
            try:
                fcntl.flock(self.lock_fd, fcntl.LOCK_UN)
            except (OSError, IOError):
                pass  # Ignore errors when releasing lock

        if self.lock_file is not None:
            try:
                self.lock_file.close()
            except (OSError, IOError):
                pass  # Ignore errors when closing file

        self.lock_fd = None
        self.lock_file = None

    def get_table_schema(self, table_name: str) -> List[Dict[str, str]]:
        """Get schema information for a table as a list of column definitions."""
        if not self.duckdb_connection:
            return []
        try:
            schema_name = self._get_schema_name()
            fq_table_name = f"{schema_name}.{table_name}"

            # Check if table exists
            if not self._table_exists(fq_table_name):
                # Try without schema prefix
                fq_table_name = table_name
                if not self._table_exists(fq_table_name):
                    raise DuckLakeError(f"Table {table_name} does not exist")

            # Get schema information
            result = self.duckdb_connection.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = ?
                ORDER BY ordinal_position
            """,
                [table_name.split(".")[-1]],
            ).fetchall()

            schema = []
            for column_name, data_type in result:
                schema.append({"name": column_name, "type": data_type.upper()})

            return schema

        except Exception as e:
            raise DuckLakeError(
                f"Failed to get schema for table {table_name}: {e}"
            ) from e

    def clear_cache_by_hash(self, query_hash: str) -> int:
        """Clear cache for a specific query hash."""
        if not self.duckdb_connection:
            return 0
        schema_name = self._get_schema_name()
        # Get the full list of tables to drop for a specific query hash
        tables_to_drop = self.duckdb_connection.execute(
            f"SELECT table_name FROM {schema_name}.snowducks_cache_metadata "
            "WHERE query_hash = ?",
            [query_hash],
        ).fetchall()
        for (table_to_drop,) in tables_to_drop:
            try:
                self.duckdb_connection.execute(
                    f'DROP TABLE IF EXISTS "{table_to_drop}"'
                )
                if self.config.deployment_mode == "local":
                    # For local mode, also delete the Parquet file
                    parquet_file = (
                        self.config.ducklake_data_path / f"{table_to_drop}.parquet"
                    )
                    if parquet_file.exists():
                        parquet_file.unlink()
            except Exception as e:
                print(f"Warning: could not drop table {table_to_drop}: {e}")

        # Delete all metadata related to this query hash
        self.duckdb_connection.execute(
            f"DELETE FROM {schema_name}.snowducks_cache_metadata WHERE query_hash = ?",
            [query_hash],
        )
        self.duckdb_connection.commit()
        return len(tables_to_drop)

    def get_table_schema_from_query(self, original_query: str) -> List[Dict[str, str]]:
        """Get schema from Snowflake by executing the query with LIMIT 0."""
        try:
            # Get config for Snowflake connection
            config = SnowDucksConfig.from_env()

            # Query Snowflake directly via ADBC for schema only
            from adbc_driver_snowflake import dbapi as snowflake_adbc
            import urllib.parse

            # URL-encode the password to handle special characters like #
            if not config.snowflake_password:
                raise DuckLakeError("Snowflake password is required")
            encoded_password = urllib.parse.quote(config.snowflake_password, safe="")

            # Build URI according to ADBC documentation format
            # Format: user:password@account/database?param1=value1&paramN=valueN
            conn_uri = (
                f"{config.snowflake_user}:{encoded_password}@"
                f"{config.snowflake_account}/{config.snowflake_database}?"
                f"warehouse={config.snowflake_warehouse}&"
                f"role={config.snowflake_role}"
            )

            with snowflake_adbc.connect(uri=conn_uri) as conn:
                with conn.cursor() as cursor:
                    # Execute the query with LIMIT 0 to get schema only (no data)
                    limited_query = f"SELECT * FROM ({original_query}) LIMIT 0"
                    cursor.execute(limited_query)

                    # Get column information from cursor description
                    schema = []
                    if cursor.description:
                        for col in cursor.description:
                            col_name = col[0]
                            col_type = col[1]

                            # Map Snowflake types to DuckDB types
                            duckdb_type = "VARCHAR"  # Default
                            if col_type in [1, 2, 3]:  # Numeric types
                                duckdb_type = "DOUBLE"
                            elif col_type == 4:  # Float
                                duckdb_type = "DOUBLE"
                            elif col_type == 5:  # String
                                duckdb_type = "VARCHAR"
                            elif col_type == 6:  # Date
                                duckdb_type = "DATE"
                            elif col_type == 7:  # Time
                                duckdb_type = "TIME"
                            elif col_type == 8:  # Timestamp
                                duckdb_type = "TIMESTAMP"
                            elif col_type == 9:  # Boolean
                                duckdb_type = "BOOLEAN"
                            elif col_type == 10:  # Binary
                                duckdb_type = "BLOB"
                            elif col_type == 11:  # Decimal
                                duckdb_type = "DECIMAL"
                            elif col_type == 12:  # Array
                                duckdb_type = "VARCHAR"  # Convert arrays to strings
                            elif col_type == 13:  # Object
                                duckdb_type = "VARCHAR"  # Convert objects to strings
                            elif col_type == 14:  # Variant
                                duckdb_type = "VARCHAR"  # Convert variants to strings

                            schema.append({"name": col_name, "type": duckdb_type})

                return schema

        except Exception as e:
            # Return a default schema on error
            import sys

            print(f"Error in get_table_schema_from_query: {e}", file=sys.stderr)
            return [{"name": "message", "type": "VARCHAR"}]


def get_table_schema_from_query(original_query: str) -> List[Dict[str, str]]:
    """Get schema from Snowflake by executing the query with LIMIT 0."""
    try:
        # Get config for Snowflake connection
        config = SnowDucksConfig.from_env()

        # Query Snowflake directly via ADBC for schema only
        from adbc_driver_snowflake import dbapi as snowflake_adbc
        import urllib.parse

        # URL-encode the password to handle special characters like #
        if not config.snowflake_password:
            raise DuckLakeError("Snowflake password is required")
        encoded_password = urllib.parse.quote(config.snowflake_password, safe="")

        # Build URI according to ADBC documentation format
        # Format: user:password@account/database?param1=value1&paramN=valueN
        conn_uri = (
            f"{config.snowflake_user}:{encoded_password}@"
            f"{config.snowflake_account}/{config.snowflake_database}?"
            f"warehouse={config.snowflake_warehouse}&"
            f"role={config.snowflake_role}"
        )

        with snowflake_adbc.connect(uri=conn_uri) as conn:
            with conn.cursor() as cursor:
                # Execute the query with LIMIT 0 to get schema only (no data)
                limited_query = f"SELECT * FROM ({original_query}) LIMIT 0"
                cursor.execute(limited_query)

                # Get column information from cursor description
                schema = []
                if cursor.description:
                    for col in cursor.description:
                        col_name = col[0]
                        col_type = col[1]

                        # Map Snowflake types to DuckDB types
                        duckdb_type = "VARCHAR"  # Default
                        if col_type in [1, 2, 3]:  # Numeric types
                            duckdb_type = "DOUBLE"
                        elif col_type == 4:  # Float
                            duckdb_type = "DOUBLE"
                        elif col_type == 5:  # String
                            duckdb_type = "VARCHAR"
                        elif col_type == 6:  # Date
                            duckdb_type = "DATE"
                        elif col_type == 7:  # Time
                            duckdb_type = "TIME"
                        elif col_type == 8:  # Timestamp
                            duckdb_type = "TIMESTAMP"
                        elif col_type == 9:  # Boolean
                            duckdb_type = "BOOLEAN"
                        elif col_type == 10:  # Binary
                            duckdb_type = "BLOB"
                        elif col_type == 11:  # Decimal
                            duckdb_type = "DECIMAL"
                        elif col_type == 12:  # Array
                            duckdb_type = "VARCHAR"  # Convert arrays to strings
                        elif col_type == 13:  # Object
                            duckdb_type = "VARCHAR"  # Convert objects to strings
                        elif col_type == 14:  # Variant
                            duckdb_type = "VARCHAR"  # Convert variants to strings

                        schema.append({"name": col_name, "type": duckdb_type})

                return schema

    except Exception as e:
        # Return a default schema on error
        import sys

        print(f"Error in get_table_schema_from_query: {e}", file=sys.stderr)
        return [{"name": "message", "type": "VARCHAR"}]


def get_table_schema(table_name: str) -> List[Dict[str, str]]:
    """Standalone function to get table schema for CLI use."""
    try:
        # Get config for Snowflake connection
        config = SnowDucksConfig.from_env()

        # Query Snowflake directly via ADBC for schema only
        import adbc_driver_snowflake.dbapi as snowflake

        # Connect to Snowflake
        conn = snowflake.connect(
            account=config.snowflake_account,
            user=config.snowflake_user,
            password=config.snowflake_password,
            database=config.snowflake_database,
            warehouse=config.snowflake_warehouse,
            role=config.snowflake_role,
        )

        # For now, we need the original query to get schema
        # We'll need to store this mapping or pass it as a parameter
        # For now, return a default schema
        conn.close()

        return [{"name": "result", "type": "VARCHAR"}]

    except Exception:
        # Return a default schema on error
        return [{"name": "message", "type": "VARCHAR"}]
