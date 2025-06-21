"""
Tests for the core module.
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from snowducks.core import (
    snowflake_query,
    _validate_single_query,
    _has_limit_clause,
    register_snowflake_udf,
    configure,
    test_connection,
    _config,
    _ducklake_manager
)
from snowducks.exceptions import QueryError, PermissionError, ConnectionError, ConfigError, SnowDucksError
from snowducks.config import SnowDucksConfig


class TestCoreFunctions:
    """Test cases for core functions."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    @pytest.fixture
    def mock_config(self, temp_dir):
        """Create a mock configuration."""
        return SnowDucksConfig(
            snowflake_user="test_user",
            snowflake_password="test_password",
            snowflake_account="test_account",
            snowflake_database="test_database",
            snowflake_warehouse="test_warehouse",
            snowflake_role="test_role",
            ducklake_metadata_path=temp_dir / "metadata.ducklake",
            ducklake_data_path=temp_dir / "data",
            allow_unlimited_egress=False,
            default_row_limit=1000,
        )

    @pytest.fixture
    def mock_ducklake_manager(self):
        """Create a mock DuckLake manager."""
        manager = MagicMock()
        manager.get_cached_table_name.return_value = None
        manager.create_cached_table.return_value = "test_table_hash"
        return manager

    def test_validate_single_query_valid(self):
        """Test single query validation with valid queries."""
        valid_queries = [
            "SELECT * FROM table1",
            "SELECT * FROM table1 WHERE id = 1",
            "SELECT * FROM table1;",  # Single semicolon at end is OK
            "SELECT * FROM table1 WHERE name = 'John; Smith'",  # Semicolon in string
            "SELECT * FROM table1 WHERE name = \"John; Smith\"",  # Semicolon in double quotes
        ]
        
        for query in valid_queries:
            # Should not raise an exception
            _validate_single_query(query)

    def test_validate_single_query_multiple_statements(self):
        """Test single query validation with multiple statements."""
        invalid_queries = [
            "SELECT * FROM table1; SELECT * FROM table2",
            "SELECT 1; SELECT 2; SELECT 3",
            "CREATE TABLE test (id INT); INSERT INTO test VALUES (1)",
        ]
        
        for query in invalid_queries:
            with pytest.raises(QueryError, match="Multiple SQL statements detected"):
                _validate_single_query(query)

    def test_validate_single_query_with_comments(self):
        """Test single query validation with comments."""
        # Query with comments that contain semicolons
        query = """
        SELECT * FROM table1 -- This is a comment with ; semicolon
        WHERE id = 1 /* Another comment with ; semicolon */
        """
        # Should not raise an exception
        _validate_single_query(query)

    def test_has_limit_clause(self):
        """Test LIMIT clause detection."""
        # Queries with LIMIT
        limit_queries = [
            "SELECT * FROM table1 LIMIT 100",
            "SELECT * FROM table1 WHERE id > 0 LIMIT 50",
            "SELECT * FROM table1 ORDER BY id LIMIT 1000",
            "SELECT * FROM table1 LIMIT 100;",
        ]
        
        for query in limit_queries:
            assert _has_limit_clause(query) is True
        
        # Queries without LIMIT
        no_limit_queries = [
            "SELECT * FROM table1",
            "SELECT * FROM table1 WHERE id > 0",
            "SELECT * FROM table1 ORDER BY id",
            "SELECT * FROM table1 WHERE name LIKE '%limit%'",  # 'limit' in string
        ]
        
        for query in no_limit_queries:
            assert _has_limit_clause(query) is False

    def test_has_limit_clause_with_comments(self):
        """Test LIMIT clause detection with comments."""
        # Query with 'limit' in comments
        query = """
        SELECT * FROM table1 -- This query has no limit
        WHERE id > 0 /* Another comment mentioning limit */
        """
        assert _has_limit_clause(query) is False
        
        # Query with actual LIMIT clause
        query_with_limit = """
        SELECT * FROM table1 -- This query has a limit
        WHERE id > 0 LIMIT 100 /* End comment */
        """
        assert _has_limit_clause(query_with_limit) is True

    def test_snowflake_query_multiple_statements(self, mock_config, mock_ducklake_manager):
        """Test query with multiple statements."""
        # Set up global config
        import snowducks.core
        snowducks.core._config = mock_config
        snowducks.core._ducklake_manager = mock_ducklake_manager

        with pytest.raises(QueryError, match="Multiple SQL statements detected"):
            snowflake_query("SELECT * FROM table1; SELECT * FROM table2")

    def test_snowflake_query_no_config(self):
        """Test query without configuration."""
        # Reset global config
        import snowducks.core
        snowducks.core._config = None
        snowducks.core._ducklake_manager = None

        with pytest.raises(ConnectionError):
            snowflake_query("SELECT * FROM test_table")

    def test_snowflake_query_basic(self, mock_config, mock_ducklake_manager):
        """Test basic Snowflake query execution."""
        # Set up global config
        import snowducks.core
        snowducks.core._config = mock_config
        snowducks.core._ducklake_manager = mock_ducklake_manager

        # Mock the DuckLake manager to return a cached table
        mock_ducklake_manager.get_cached_table_name.return_value = "test_table_hash"
        mock_ducklake_manager.create_cached_table.return_value = "test_table_hash"

        # Mock the ADBC connection and Arrow reader
        mock_arrow_table = MagicMock()
        mock_arrow_table.schema = MagicMock()
        
        mock_reader = MagicMock()
        mock_reader.read_all.return_value = mock_arrow_table
        
        mock_cursor = MagicMock()
        mock_cursor.fetch_arrow_reader.return_value = mock_reader
        
        mock_connection = MagicMock()
        mock_connection.__enter__.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        with patch('adbc_driver_snowflake.dbapi.connect') as mock_connect:
            mock_connect.return_value = mock_connection
            
            with patch('pyarrow.parquet.write_table') as mock_write_table:
                with patch('pyarrow.parquet.read_metadata') as mock_read_metadata:
                    mock_read_metadata.return_value.num_rows = 100
                    
                    result = snowflake_query("SELECT * FROM test_table")
                    
                    assert isinstance(result, tuple)
                    assert len(result) == 2
                    assert result[0] == "test_table_hash"
                    assert result[1] in ["hit", "miss"]
                    # Should not call connect because it's a cache hit
                    mock_connect.assert_not_called()

    def test_snowflake_query_with_limit(self, mock_config, mock_ducklake_manager):
        """Test Snowflake query with automatic LIMIT addition."""
        # Set up global config
        import snowducks.core
        snowducks.core._config = mock_config
        snowducks.core._ducklake_manager = mock_ducklake_manager

        # Mock the DuckLake manager
        mock_ducklake_manager.get_cached_table_name.return_value = None
        mock_ducklake_manager.create_cached_table.return_value = "test_table_hash"

        # Mock the ADBC connection and Arrow reader
        mock_arrow_table = MagicMock()
        mock_arrow_table.schema = MagicMock()
        
        mock_reader = MagicMock()
        mock_reader.read_all.return_value = mock_arrow_table
        
        mock_cursor = MagicMock()
        mock_cursor.fetch_arrow_reader.return_value = mock_reader
        
        mock_connection = MagicMock()
        mock_connection.__enter__.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        with patch('adbc_driver_snowflake.dbapi.connect') as mock_connect:
            mock_connect.return_value = mock_connection
            
            with patch('pyarrow.parquet.write_table') as mock_write_table:
                with patch('pyarrow.parquet.read_metadata') as mock_read_metadata:
                    mock_read_metadata.return_value.num_rows = 100
                    
                    result = snowflake_query("SELECT * FROM test_table")
                    
                    # Should have called execute with LIMIT clause
                    mock_cursor.execute.assert_called_once()
                    call_args = mock_cursor.execute.call_args[0][0]
                    assert "LIMIT 1000" in call_args

    def test_snowflake_query_unlimited_egress_disabled(self, mock_config, mock_ducklake_manager):
        """Test query with unlimited egress disabled."""
        # Set up global config
        import snowducks.core
        snowducks.core._config = mock_config
        snowducks.core._ducklake_manager = mock_ducklake_manager

        mock_config.allow_unlimited_egress = False

        # Mock the DuckLake manager
        mock_ducklake_manager.get_cached_table_name.return_value = None
        mock_ducklake_manager.create_cached_table.return_value = "test_table_hash"

        # Mock the ADBC connection and Arrow reader
        mock_arrow_table = MagicMock()
        mock_arrow_table.schema = MagicMock()
        
        mock_reader = MagicMock()
        mock_reader.read_all.return_value = mock_arrow_table
        
        mock_cursor = MagicMock()
        mock_cursor.fetch_arrow_reader.return_value = mock_reader
        
        mock_connection = MagicMock()
        mock_connection.__enter__.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        with patch('adbc_driver_snowflake.dbapi.connect') as mock_connect:
            mock_connect.return_value = mock_connection
            
            with patch('pyarrow.parquet.write_table') as mock_write_table:
                with patch('pyarrow.parquet.read_metadata') as mock_read_metadata:
                    mock_read_metadata.return_value.num_rows = 100
                    
                    result = snowflake_query("SELECT * FROM test_table")
                    
                    # Should have called execute with LIMIT clause
                    mock_cursor.execute.assert_called_once()
                    call_args = mock_cursor.execute.call_args[0][0]
                    assert "LIMIT 1000" in call_args

    def test_snowflake_query_unlimited_egress_enabled(self, mock_config, mock_ducklake_manager):
        """Test query with unlimited egress enabled."""
        # Set up global config
        import snowducks.core
        snowducks.core._config = mock_config
        snowducks.core._ducklake_manager = mock_ducklake_manager

        mock_config.allow_unlimited_egress = True

        # Mock the DuckLake manager
        mock_ducklake_manager.get_cached_table_name.return_value = None
        mock_ducklake_manager.create_cached_table.return_value = "test_table_hash"

        # Mock the ADBC connection and Arrow reader
        mock_arrow_table = MagicMock()
        mock_arrow_table.schema = MagicMock()
        
        mock_reader = MagicMock()
        mock_reader.read_all.return_value = mock_arrow_table
        
        mock_cursor = MagicMock()
        mock_cursor.fetch_arrow_reader.return_value = mock_reader
        
        mock_connection = MagicMock()
        mock_connection.__enter__.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        with patch('adbc_driver_snowflake.dbapi.connect') as mock_connect:
            mock_connect.return_value = mock_connection
            
            with patch('pyarrow.parquet.write_table') as mock_write_table:
                with patch('pyarrow.parquet.read_metadata') as mock_read_metadata:
                    mock_read_metadata.return_value.num_rows = 100
                    
                    # Test with explicit limit=-1 to bypass the default limit
                    result = snowflake_query("SELECT * FROM test_table", limit=-1)
                    
                    # Should have called execute without LIMIT clause
                    mock_cursor.execute.assert_called_once()
                    call_args = mock_cursor.execute.call_args[0][0]
                    assert "LIMIT" not in call_args

    def test_test_connection_success(self, mock_config):
        """Test successful connection test."""
        # Set up global config
        import snowducks.core
        snowducks.core._config = mock_config
        
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [1]
        
        mock_connection = MagicMock()
        mock_connection.__enter__.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        
        with patch('adbc_driver_snowflake.dbapi.connect') as mock_connect:
            mock_connect.return_value = mock_connection

            assert test_connection() is True
            mock_connect.assert_called_once()
            mock_cursor.execute.assert_called_once_with("SELECT 1")

    def test_test_connection_failure(self, mock_config):
        """Test failed connection test."""
        # Set up global config
        import snowducks.core
        snowducks.core._config = mock_config
        
        with patch('adbc_driver_snowflake.dbapi.connect') as mock_connect:
            mock_connect.side_effect = Exception("Connection failed")

            assert test_connection() is False

    def test_register_snowflake_udf_basic(self, mock_config, mock_ducklake_manager):
        """Test basic UDF registration."""
        # Set up global config
        import snowducks.core
        snowducks.core._config = mock_config
        snowducks.core._ducklake_manager = mock_ducklake_manager

        # Mock DuckDB connection
        mock_connection = MagicMock()
        
        # Should register the UDF successfully
        register_snowflake_udf(mock_connection)
        
        # Should have called create_function
        mock_connection.create_function.assert_called_once()

    def test_register_snowflake_udf_failure(self, mock_config, mock_ducklake_manager):
        """Test UDF registration failure."""
        # Set up global config
        import snowducks.core
        snowducks.core._config = mock_config
        snowducks.core._ducklake_manager = mock_ducklake_manager

        # Mock DuckDB connection that raises an error
        mock_connection = MagicMock()
        mock_connection.create_function.side_effect = Exception("Registration failed")
        
        with pytest.raises(SnowDucksError, match="Failed to register UDF"):
            register_snowflake_udf(mock_connection) 