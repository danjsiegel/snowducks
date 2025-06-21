"""
SnowDucks - A powerful Python library that seamlessly bridges Snowflake and DuckDB.

This library provides a "magic function" that allows you to query Snowflake data
directly from DuckDB with intelligent multi-tiered caching.
"""

__version__ = "0.1.0"
__author__ = "SnowDucks Contributors"
__email__ = "contributors@snowducks.dev"

from .core import register_snowflake_udf, snowflake_query, fetch_from_snowflake, clear_cache, get_cache_stats, configure, test_connection, get_popular_queries, get_recent_queries, search_queries, cleanup_expired_cache
from .config import SnowDucksConfig
from .exceptions import SnowDucksError, ConnectionError, CacheError, ConfigError, DuckLakeError

__all__ = [
    "register_snowflake_udf",
    "snowflake_query",
    "fetch_from_snowflake",  # Backward compatibility
    "clear_cache",
    "get_cache_stats",
    "configure",
    "test_connection",
    "get_popular_queries",
    "get_recent_queries",
    "search_queries",
    "cleanup_expired_cache",
    "SnowDucksConfig",
    "SnowDucksError",
    "ConnectionError",
    "CacheError",
    "ConfigError",
    "DuckLakeError",
] 