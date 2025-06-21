"""
Custom exceptions for the SnowDucks library.
"""


class SnowDucksError(Exception):
    """Base exception for all SnowDucks errors."""
    pass


class ConnectionError(SnowDucksError):
    """Raised when there's an issue connecting to Snowflake."""
    pass


class CacheError(SnowDucksError):
    """Raised when there's an issue with the caching system."""
    pass


class ConfigError(SnowDucksError):
    """Raised when there's an issue with configuration."""
    pass


class AuthenticationError(SnowDucksError):
    """Raised when there's an authentication issue with Snowflake."""
    pass


class PermissionError(SnowDucksError):
    """Raised when the user doesn't have permission for an operation."""
    pass


class QueryError(SnowDucksError):
    """Raised when there's an issue with the SQL query."""
    pass


class CatalogError(SnowDucksError):
    """Raised when there's an issue with the catalog database."""
    pass


class DuckLakeError(SnowDucksError):
    """Raised when there's an issue with DuckLake operations."""
    pass 