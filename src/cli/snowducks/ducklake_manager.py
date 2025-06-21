"""
DuckLake manager for SnowDucks - handles DuckLake database operations and cache recency.
"""

import json
import time
import fcntl
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import duckdb
import sqlite3

from .config import SnowDucksConfig
from .exceptions import DuckLakeError, QueryError
from .utils import generate_normalized_query_hash, generate_query_hash_without_limit, parse_query_metadata


class DuckLakeManager:
    """Manages DuckLake database operations for SnowDucks."""
    
    def __init__(self, config: SnowDucksConfig):
        self.config = config
        self.duckdb_connection: Optional[duckdb.DuckDBPyConnection] = None
        self.ducklake_attached = False
        self.lock_file = None
        self.lock_fd = None
        
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
            self.duckdb_connection.execute(f"ATTACH '{ducklake_attach_string}' AS snowducks_ducklake (DATA_PATH '{data_path}')")
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
        try:
            schema_name = self._get_schema_name()
            self.duckdb_connection.execute(f"DROP TABLE IF EXISTS {schema_name}.snowducks_queries")
            self.duckdb_connection.execute(f"DROP TABLE IF EXISTS {schema_name}.snowducks_cache_metadata")
            self.duckdb_connection.execute(f"DROP TABLE IF EXISTS {schema_name}.snowducks_users")
        except Exception as e:
            print(f"Warning: Failed to drop metadata tables: {e}")
    
    def _create_metadata_tables(self) -> None:
        """Create metadata tables for tracking queries and cache information."""
        # Determine the schema name based on the configuration
        schema_name = self._get_schema_name()
        
        # Create queries table
        self.duckdb_connection.execute(f"""
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
        """)
        
        # Create query_metadata table for storing LIMIT and other query details
        self.duckdb_connection.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.snowducks_query_metadata (
                query_hash VARCHAR,
                original_query TEXT NOT NULL,
                query_without_limit TEXT NOT NULL,
                limit_value INTEGER,
                has_limit BOOLEAN NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
        """)
        
        # Create cache_metadata table
        self.duckdb_connection.execute(f"""
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
        """)
        
        # Create users table
        self.duckdb_connection.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.snowducks_users (
                user_id VARCHAR,
                first_seen TIMESTAMP NOT NULL,
                last_seen TIMESTAMP NOT NULL,
                total_queries INTEGER DEFAULT 0,
                environment VARCHAR
            )
        """)
    
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
        except:
            return "unknown"
    
    def _is_cache_fresh(self, query_hash: str) -> bool:
        """Check if cached data is fresh based on cache_max_age_hours."""
        if self.config.cache_force_refresh:
            return False
        
        schema_name = self._get_schema_name()
        result = self.duckdb_connection.execute(f"""
            SELECT last_refresh, cache_max_age_hours 
            FROM {schema_name}.snowducks_queries 
            WHERE query_hash = ?
        """, [query_hash]).fetchone()
        
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
        schema_name, table_name = fq_table_name.split('.', 1)
        
        # Check in the expected schema first
        result = self.duckdb_connection.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = ? AND table_name = ?
        """, [schema_name, table_name]).fetchone()
        
        if result[0] > 0:
            return True
        
        # Fallback: check in main schema (for old cached tables)
        result = self.duckdb_connection.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'main' AND table_name = ?
        """, [table_name]).fetchone()
        
        return result[0] > 0
    
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
        schema_name = self._get_schema_name()
        result = self.duckdb_connection.execute(f"""
            SELECT limit_value FROM {schema_name}.snowducks_query_metadata 
            WHERE query_hash = ?
        """, [query_hash]).fetchone()
        
        if not result:
            return False
        
        cached_limit = result[0]
        return cached_limit == limit_value if cached_limit else False
    
    def _update_cache_usage(self, query_hash: str) -> None:
        """Update the last_used timestamp and usage count for a cache hit."""
        now = datetime.now(timezone.utc)
        schema_name = self._get_schema_name()
        self.duckdb_connection.execute(f"""
            UPDATE {schema_name}.snowducks_queries 
            SET last_used = ?, usage_count = usage_count + 1
            WHERE query_hash = ?
        """, [now, query_hash])
        self.duckdb_connection.commit()
    
    def create_cached_table(self, query_text: str, data_path: str, row_count: int, 
                          execution_time_ms: Optional[int] = None) -> str:
        """Create a cached table in DuckLake from a Parquet file."""
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
            file_info = self.duckdb_connection.execute(f"""
                SELECT COUNT(*) as file_count
                FROM glob('{data_path}')
            """).fetchone()
            total_size = None
            
            # Record query metadata
            now = datetime.now(timezone.utc)
            user_id = self._get_user_id()
            
            # Delete existing query record and insert new one
            self.duckdb_connection.execute(f"DELETE FROM {schema_name}.snowducks_queries WHERE query_hash = ?", [query_hash])
            self.duckdb_connection.execute(f"""
                INSERT INTO {schema_name}.snowducks_queries 
                (query_hash, query_text, first_seen, last_used, usage_count, 
                 avg_execution_time_ms, total_rows_fetched, created_by, 
                 last_refresh, cache_max_age_hours)
                VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
            """, [
                query_hash, metadata['query_without_limit'], now, now, execution_time_ms, 
                row_count, user_id, now, self.config.cache_max_age_hours
            ])
            
            # Store query metadata including LIMIT information
            self.duckdb_connection.execute(f"DELETE FROM {schema_name}.snowducks_query_metadata WHERE query_hash = ?", [query_hash])
            self.duckdb_connection.execute(f"""
                INSERT INTO {schema_name}.snowducks_query_metadata
                (query_hash, original_query, query_without_limit, limit_value, has_limit, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [
                query_hash, metadata['original_query'], metadata['query_without_limit'],
                metadata['limit_value'], metadata['has_limit'], now
            ])
            
            # Delete existing cache metadata and insert new one
            self.duckdb_connection.execute(f"DELETE FROM {schema_name}.snowducks_cache_metadata WHERE query_hash = ?", [query_hash])
            self.duckdb_connection.execute(f"""
                INSERT INTO {schema_name}.snowducks_cache_metadata
                (query_hash, table_name, file_count, total_size_bytes, 
                 row_count, created_at, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                query_hash, fq_table_name, file_info[0], total_size, 
                row_count, now, user_id
            ])
            
            # Update user stats
            self._update_user_stats(user_id, now)
            
            # Commit all metadata changes
            self.duckdb_connection.commit()
            
            return fq_table_name
            
        except Exception as e:
            raise DuckLakeError(f"Failed to create cached table: {e}") from e
    
    def _update_user_stats(self, user_id: str, now: datetime) -> None:
        """Update user statistics."""
        schema_name = self._get_schema_name()
        # Delete existing user record and insert new one
        self.duckdb_connection.execute(f"DELETE FROM {schema_name}.snowducks_users WHERE user_id = ?", [user_id])
        self.duckdb_connection.execute(f"""
            INSERT INTO {schema_name}.snowducks_users (user_id, first_seen, last_seen, total_queries, environment)
            VALUES (?, ?, ?, 1, ?)
        """, [user_id, now, now, self.config.deployment_mode])
    
    def get_popular_queries(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get the most frequently used queries."""
        schema_name = self._get_schema_name()
        result = self.duckdb_connection.execute(f"""
            SELECT query_hash, query_text, usage_count, avg_execution_time_ms, last_used
            FROM {schema_name}.snowducks_queries 
            ORDER BY usage_count DESC, last_used DESC
            LIMIT ?
        """, [limit]).fetchall()
        
        return [
            {
                'query_hash': row[0],
                'query_text': row[1],
                'usage_count': row[2],
                'avg_execution_time_ms': row[3],
                'last_used': row[4]
            }
            for row in result
        ]
    
    def get_recent_queries(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recently executed queries."""
        schema_name = self._get_schema_name()
        result = self.duckdb_connection.execute(f"""
            SELECT query_hash, query_text, usage_count, last_used
            FROM {schema_name}.snowducks_queries 
            ORDER BY last_used DESC
            LIMIT ?
        """, [limit]).fetchall()
        
        return [
            {
                'query_hash': row[0],
                'query_text': row[1],
                'usage_count': row[2],
                'last_used': row[3]
            }
            for row in result
        ]
    
    def search_queries(self, search_term: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Search queries by text content."""
        schema_name = self._get_schema_name()
        result = self.duckdb_connection.execute(f"""
            SELECT query_hash, query_text, usage_count, last_used
            FROM {schema_name}.snowducks_queries 
            WHERE query_text ILIKE ?
            ORDER BY usage_count DESC, last_used DESC
            LIMIT ?
        """, [f'%{search_term}%', limit]).fetchall()
        
        return [
            {
                'query_hash': row[0],
                'query_text': row[1],
                'usage_count': row[2],
                'last_used': row[3]
            }
            for row in result
        ]
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        schema_name = self._get_schema_name()
        
        # Query stats
        query_count = self.duckdb_connection.execute(f"SELECT COUNT(*) FROM {schema_name}.snowducks_queries").fetchone()[0]
        total_usage = self.duckdb_connection.execute(f"SELECT SUM(usage_count) FROM {schema_name}.snowducks_queries").fetchone()[0] or 0
        
        # Cache stats
        cache_count = self.duckdb_connection.execute(f"SELECT COUNT(*) FROM {schema_name}.snowducks_cache_metadata").fetchone()[0]
        total_size = self.duckdb_connection.execute(f"SELECT SUM(total_size_bytes) FROM {schema_name}.snowducks_cache_metadata").fetchone()[0] or 0
        
        # User stats
        user_count = self.duckdb_connection.execute(f"SELECT COUNT(*) FROM {schema_name}.snowducks_users").fetchone()[0]
        
        return {
            'ducklake_metadata_path': str(self.config.ducklake_metadata_path),
            'ducklake_data_path': str(self.config.ducklake_data_path),
            'total_queries': query_count,
            'total_query_executions': total_usage,
            'total_cache_entries': cache_count,
            'total_cache_size_bytes': total_size,
            'total_users': user_count,
            'deployment_mode': self.config.deployment_mode,
            'cache_max_age_hours': self.config.cache_max_age_hours,
            'cache_force_refresh': self.config.cache_force_refresh
        }
    
    def cleanup_expired_cache(self) -> int:
        """Clean up expired cache entries based on cache_max_age_hours."""
        if self.config.cache_max_age_hours <= 0:
            return 0
        
        schema_name = self._get_schema_name()
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=self.config.cache_max_age_hours)
        
        # Get expired query hashes
        expired_queries = self.duckdb_connection.execute(f"""
            SELECT query_hash FROM {schema_name}.snowducks_queries 
            WHERE last_refresh < ?
        """, [cutoff_time]).fetchall()
        
        cleaned_count = 0
        for (query_hash,) in expired_queries:
            try:
                # Drop the cached table
                fq_table_name = f"{schema_name}.{query_hash}"
                self.duckdb_connection.execute(f"DROP TABLE IF EXISTS {fq_table_name}")
                
                # Remove metadata
                self.duckdb_connection.execute(f"DELETE FROM {schema_name}.snowducks_cache_metadata WHERE query_hash = ?", [query_hash])
                
                cleaned_count += 1
            except Exception as e:
                print(f"Warning: Failed to clean up expired cache for {query_hash}: {e}")
        
        return cleaned_count
    
    def clear_all_cache(self) -> int:
        """Clear all cached tables and metadata."""
        schema_name = self._get_schema_name()
        
        # Get all cached table names
        cached_tables = self.duckdb_connection.execute(f"""
            SELECT table_name FROM {schema_name}.snowducks_cache_metadata
        """).fetchall()
        
        cleaned_count = 0
        for (table_name,) in cached_tables:
            try:
                # Drop the cached table
                self.duckdb_connection.execute(f"DROP TABLE IF EXISTS {table_name}")
                cleaned_count += 1
            except Exception as e:
                print(f"Warning: Failed to drop table {table_name}: {e}")
        
        # Clear metadata
        self.duckdb_connection.execute(f"DELETE FROM {schema_name}.snowducks_cache_metadata")
        
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
        self.lock_file = open(lock_path, 'w')
        self.lock_fd = self.lock_file.fileno()
        
        # Try to acquire exclusive lock with timeout
        max_attempts = 30  # 30 seconds timeout
        for attempt in range(max_attempts):
            try:
                fcntl.flock(self.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return  # Lock acquired successfully
            except (OSError, IOError):
                if attempt < max_attempts - 1:
                    time.sleep(1)  # Wait 1 second before retrying
                else:
                    raise DuckLakeError(
                        f"Could not acquire database lock after {max_attempts} seconds. "
                        "Another process may be using the database."
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