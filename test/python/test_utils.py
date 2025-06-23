"""
Tests for shared utilities module.
"""

from snowducks.utils import (
    generate_query_hash,
    normalize_query_text,
    generate_normalized_query_hash,
    generate_query_hash_without_limit,
    extract_limit_from_query,
    parse_query_metadata,
    is_valid_cache_table_name,
    extract_hash_from_table_name,
)


class TestHashingFunctions:
    """Test the hashing functions for consistency and correctness."""

    def test_generate_query_hash_basic(self):
        """Test basic hash generation."""
        query = "SELECT * FROM users"
        hash_result = generate_query_hash(query)

        assert hash_result.startswith("t_")
        assert len(hash_result) == 18  # t_ + 16 hex chars
        assert hash_result[2:].isalnum()  # hex chars only

        # Should be deterministic
        assert generate_query_hash(query) == hash_result

    def test_normalize_query_text(self):
        """Test query text normalization."""
        query1 = "SELECT * FROM users"
        query2 = "  select  *  from  users  "
        query3 = "SELECT * FROM USERS"

        normalized1 = normalize_query_text(query1)
        normalized2 = normalize_query_text(query2)
        normalized3 = normalize_query_text(query3)

        # All should be normalized to the same string
        expected = "select * from users"
        assert normalized1 == expected
        assert normalized2 == expected
        assert normalized3 == expected

    def test_generate_normalized_query_hash(self):
        """Test normalized hash generation."""
        query1 = "SELECT * FROM users"
        query2 = "  select  *  from  users  "
        query3 = "SELECT * FROM USERS"

        # All should generate the same hash
        hash1 = generate_normalized_query_hash(query1)
        hash2 = generate_normalized_query_hash(query2)
        hash3 = generate_normalized_query_hash(query3)

        assert hash1 == hash2 == hash3
        assert hash1.startswith("t_")
        assert len(hash1) == 18

    def test_generate_query_hash_with_limit_differs(self):
        """Test that queries with different LIMITs have different hashes."""
        query1 = "SELECT * FROM users LIMIT 1000"
        query2 = "SELECT * FROM users LIMIT 500"
        hash1 = generate_normalized_query_hash(query1)
        hash2 = generate_normalized_query_hash(query2)
        assert hash1 != hash2
        assert hash1.startswith("t_")
        assert hash2.startswith("t_")
        assert len(hash1) == 18
        assert len(hash2) == 18

    def test_generate_query_hash_with_and_without_limit_differs(self):
        """Test that query with LIMIT and without LIMIT have different hashes."""
        query1 = "SELECT * FROM users LIMIT 1000"
        query2 = "SELECT * FROM users"
        hash1 = generate_normalized_query_hash(query1)
        hash2 = generate_normalized_query_hash(query2)
        assert hash1 != hash2

    def test_generate_query_hash_with_limit_same(self):
        """Test that same query with same LIMIT has same hash."""
        query1 = "SELECT * FROM users LIMIT 1000"
        query2 = "  select  *  from  users  limit  1000  "
        hash1 = generate_normalized_query_hash(query1)
        hash2 = generate_normalized_query_hash(query2)
        assert hash1 == hash2

    def test_generate_query_hash_without_limit_same(self):
        """Test that same query without LIMIT has same hash."""
        query1 = "SELECT * FROM users"
        query2 = "  select  *  from  users  "
        hash1 = generate_normalized_query_hash(query1)
        hash2 = generate_normalized_query_hash(query2)
        assert hash1 == hash2

    def test_generate_query_hash_with_limit_and_where(self):
        """Test that different queries with LIMIT have different hashes."""
        query1 = "SELECT * FROM users LIMIT 1000"
        query2 = "SELECT * FROM users WHERE active = true LIMIT 1000"
        hash1 = generate_normalized_query_hash(query1)
        hash2 = generate_normalized_query_hash(query2)
        assert hash1 != hash2


class TestLimitAwareHashing:
    """Test the LIMIT-aware hashing functionality."""

    def test_extract_limit_from_query_basic(self):
        """Test basic LIMIT extraction."""
        query = "SELECT * FROM users LIMIT 1000"
        query_without_limit, limit_value = extract_limit_from_query(query)

        assert query_without_limit == "select * from users"
        assert limit_value == 1000

    def test_extract_limit_from_query_with_offset(self):
        """Test LIMIT extraction with OFFSET."""
        query = "SELECT * FROM users LIMIT 1000 OFFSET 200"
        query_without_limit, limit_value = extract_limit_from_query(query)

        assert query_without_limit == "select * from users"
        assert limit_value == 1000

    def test_extract_limit_from_query_no_limit(self):
        """Test query without LIMIT."""
        query = "SELECT * FROM users"
        query_without_limit, limit_value = extract_limit_from_query(query)

        assert query_without_limit == "select * from users"
        assert limit_value is None

    def test_extract_limit_from_query_case_insensitive(self):
        """Test LIMIT extraction is case insensitive."""
        query1 = "SELECT * FROM users LIMIT 1000"
        query2 = "SELECT * FROM users limit 1000"
        query3 = "SELECT * FROM users Limit 1000"

        for query in [query1, query2, query3]:
            query_without_limit, limit_value = extract_limit_from_query(query)
            assert query_without_limit == "select * from users"
            assert limit_value == 1000

    def test_generate_query_hash_without_limit_same_query_different_limits(self):
        """Test that same query with different LIMITs gets same hash."""
        query1 = "SELECT * FROM users LIMIT 1000"
        query2 = "SELECT * FROM users LIMIT 500"
        query3 = "SELECT * FROM users LIMIT 2000"

        hash1 = generate_query_hash_without_limit(query1)
        hash2 = generate_query_hash_without_limit(query2)
        hash3 = generate_query_hash_without_limit(query3)

        # All should have the same hash
        assert hash1 == hash2 == hash3
        assert hash1.startswith("t_")
        assert len(hash1) == 18

    def test_generate_query_hash_without_limit_different_queries(self):
        """Test that different queries get different hashes."""
        query1 = "SELECT * FROM users LIMIT 1000"
        query2 = "SELECT * FROM orders LIMIT 1000"
        query3 = "SELECT id, name FROM users LIMIT 1000"

        hash1 = generate_query_hash_without_limit(query1)
        hash2 = generate_query_hash_without_limit(query2)
        hash3 = generate_query_hash_without_limit(query3)

        # All should have different hashes
        assert hash1 != hash2
        assert hash1 != hash3
        assert hash2 != hash3

    def test_parse_query_metadata_basic(self):
        """Test query metadata parsing."""
        query = "SELECT * FROM users LIMIT 1000"
        metadata = parse_query_metadata(query)

        assert metadata["original_query"] == query
        assert metadata["query_without_limit"] == "select * from users"
        assert metadata["limit_value"] == 1000
        assert metadata["has_limit"] is True
        assert metadata["query_hash"].startswith("t_")
        assert len(metadata["query_hash"]) == 18

    def test_parse_query_metadata_no_limit(self):
        """Test query metadata parsing without LIMIT."""
        query = "SELECT * FROM users"
        metadata = parse_query_metadata(query)

        assert metadata["original_query"] == query
        assert metadata["query_without_limit"] == "select * from users"
        assert metadata["limit_value"] is None
        assert metadata["has_limit"] is False
        assert metadata["query_hash"].startswith("t_")
        assert len(metadata["query_hash"]) == 18

    def test_parse_query_metadata_complex_query(self):
        """Test query metadata parsing with complex query."""
        query = """
            SELECT u.id, u.name, o.order_date
            FROM users u
            JOIN orders o ON u.id = o.user_id
            WHERE o.status = 'completed'
            ORDER BY o.order_date DESC
            LIMIT 500 OFFSET 100
        """
        metadata = parse_query_metadata(query)

        expected_without_limit = (
            "select u.id, u.name, o.order_date from users u "
            "join orders o on u.id = o.user_id "
            "where o.status = 'completed' order by o.order_date desc"
        )
        assert metadata["original_query"] == query
        assert metadata["query_without_limit"] == expected_without_limit
        assert metadata["limit_value"] == 500
        assert metadata["has_limit"] is True


class TestTableNameValidation:
    """Test table name validation functions."""

    def test_is_valid_cache_table_name_valid(self):
        """Test valid cache table names."""
        valid_names = ["t_a1b2c3d4e5f6g7h8", "t_1234567890abcdef", "t_abcdef1234567890"]

        for name in valid_names:
            assert is_valid_cache_table_name(name) is True

    def test_is_valid_cache_table_name_invalid(self):
        """Test invalid cache table names."""
        invalid_names = [
            "table_name",  # No t_ prefix
            "t_a1b2c3d4e5f6g7h",  # Too short
            "t_a1b2c3d4e5f6g7h8i",  # Too long
            "t_a1b2c3d4e5f6g7h8g",  # Invalid hex char (g is valid, but too long)
            "T_a1b2c3d4e5f6g7h8",  # Wrong case
            "t_a1b2c3d4e5f6g7h8_extra",  # Extra characters
            "t_a1b2c3d4e5f6g7h8z",  # Invalid hex char (z)
            "t_a1b2c3d4e5f6g7h8G",  # Invalid hex char (uppercase G)
        ]

        for name in invalid_names:
            assert is_valid_cache_table_name(name) is False

    def test_extract_hash_from_table_name_valid(self):
        """Test hash extraction from valid table names."""
        table_name = "t_a1b2c3d4e5f6g7h8"
        hash_value = extract_hash_from_table_name(table_name)

        assert hash_value == "a1b2c3d4e5f6g7h8"

    def test_extract_hash_from_table_name_invalid(self):
        """Test hash extraction from invalid table names."""
        invalid_names = ["table_name", "t_a1b2c3d4e5f6g7h", "t_a1b2c3d4e5f6g7h8i"]

        for name in invalid_names:
            assert extract_hash_from_table_name(name) is None
