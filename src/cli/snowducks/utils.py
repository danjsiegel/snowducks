"""
Shared utilities for SnowDucks caching system.
This module contains functions that should be consistent between CLI and UI components.
"""

import hashlib
import re
from typing import Optional, Tuple, Dict, Any


def extract_limit_from_query(query_text: str) -> Tuple[str, Optional[int]]:
    """
    Extract LIMIT clause from query and return query without LIMIT and the limit value.

    This ensures that the same query with different LIMIT values gets the same hash,
    but we can track the LIMIT separately in metadata.

    Args:
        query_text: The SQL query text

    Returns:
        Tuple of (query_without_limit, limit_value)
    """
    # Normalize the query first
    normalized = normalize_query_text(query_text)

    # Pattern to match LIMIT clause at the end of the query
    # This handles: LIMIT 1000, LIMIT 1000 OFFSET 200, etc.
    limit_pattern = r"\blimit\s+(\d+)(?:\s+offset\s+(\d+))?\s*$"

    match = re.search(limit_pattern, normalized, re.IGNORECASE)
    if match:
        limit_value = int(match.group(1))
        # Remove the entire LIMIT clause from the query
        query_without_limit = re.sub(
            limit_pattern, "", normalized, flags=re.IGNORECASE
        ).strip()
        return query_without_limit, limit_value
    else:
        return normalized, None


def generate_query_hash(query_text: str) -> str:
    """
    Generate a consistent hash for query text.

    This function must be identical between CLI and UI components to ensure
    cache compatibility and avoid duplicate caching efforts.

    Args:
        query_text: The SQL query text to hash

    Returns:
        A table-safe hash string with 't_' prefix
    """
    # Use SHA256 for collision resistance
    hash_value = hashlib.sha256(query_text.encode()).hexdigest()[:16]
    # Ensure the table name starts with a letter to avoid SQL syntax errors
    return f"t_{hash_value}"


def normalize_query_text(query_text: str) -> str:
    """
    Normalize query text before hashing to ensure consistent caching.

    This removes whitespace differences and normalizes case to prevent
    duplicate caches for essentially the same query.

    Args:
        query_text: Raw query text

    Returns:
        Normalized query text
    """
    # Remove extra whitespace and normalize to lowercase
    normalized = " ".join(query_text.strip().split()).lower()
    return normalized


def generate_normalized_query_hash(query_text: str) -> str:
    """
    Generate a hash for normalized query text.

    This is the recommended way to generate cache keys as it ensures
    consistent caching regardless of formatting differences.

    Args:
        query_text: The SQL query text to hash

    Returns:
        A table-safe hash string with 't_' prefix
    """
    normalized = normalize_query_text(query_text)
    return generate_query_hash(normalized)


def generate_query_hash_without_limit(query_text: str) -> str:
    """
    Generate a hash for query text without LIMIT clause.

    This ensures that queries with different LIMIT values get the same hash,
    allowing the cache to be shared while tracking LIMIT separately.

    Args:
        query_text: The SQL query text to hash

    Returns:
        A table-safe hash string with 't_' prefix
    """
    query_without_limit, _ = extract_limit_from_query(query_text)
    return generate_query_hash(query_without_limit)


def parse_query_metadata(query_text: str) -> Dict[str, Any]:
    """
    Parse query text and extract metadata including LIMIT information.

    Args:
        query_text: The SQL query text

    Returns:
        Dictionary containing parsed metadata
    """
    query_without_limit, limit_value = extract_limit_from_query(query_text)

    metadata = {
        "original_query": query_text,
        "query_without_limit": query_without_limit,
        "limit_value": limit_value,
        "has_limit": limit_value is not None,
        "query_hash": generate_query_hash(query_without_limit),
    }

    return metadata


def is_valid_cache_table_name(table_name: str) -> bool:
    """
    Check if a table name is a valid SnowDucks cache table.

    Args:
        table_name: The table name to check

    Returns:
        True if it's a valid cache table name
    """
    return (
        table_name.startswith("t_")
        and len(table_name) == 18  # t_ + 16 hex chars
        and table_name[2:].isalnum()  # hex chars only
        and table_name[2:].islower()
    )  # lowercase hex only


def extract_hash_from_table_name(table_name: str) -> Optional[str]:
    """
    Extract the hash portion from a cache table name.

    Args:
        table_name: The table name (e.g., "t_a1b2c3d4e5f6g7h8")

    Returns:
        The hash portion (e.g., "a1b2c3d4e5f6g7h8") or None if invalid
    """
    if not is_valid_cache_table_name(table_name):
        return None
    return table_name[2:]  # Remove "t_" prefix
