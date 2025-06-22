"""
Tests for the CLI module.
"""

import os
import tempfile
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
from io import StringIO

import pytest

from snowducks.cli import main
from snowducks.exceptions import QueryError, ConfigError


class TestCLI:
    """Test cases for CLI functions."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    @pytest.fixture
    def mock_config(self, temp_dir):
        """Create a mock configuration."""
        from snowducks.config import SnowDucksConfig
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

    @patch('snowducks.cli.configure')
    @patch('snowducks.core.snowflake_query')
    def test_get_schema_success(self, mock_query, mock_configure, mock_config):
        """Test successful schema extraction."""
        mock_configure.return_value = mock_config
        mock_query.return_value = ("test_table_hash", "miss")
        
        # Capture stdout
        from io import StringIO
        import sys
        old_stdout = sys.stdout
        sys.stdout = captured_output = StringIO()
        
        try:
            # Mock sys.argv for get-schema command
            with patch('sys.argv', ['snowducks', 'get-schema', 'test_table_hash', 'SELECT id, name FROM test_table']):
                with patch('snowducks.ducklake_manager.get_table_schema_from_query') as mock_get_schema:
                    mock_get_schema.return_value = [{"name": "id", "type": "INTEGER"}, {"name": "name", "type": "VARCHAR"}]
                    main()
            
            # Get the output
            output = captured_output.getvalue()
            
            # Parse the JSON output
            result = json.loads(output)
            
            assert result["status"] == "success"
            assert result["table_name"] == "test_table_hash"
            assert "schema" in result
            assert isinstance(result["schema"], list)
            
        finally:
            sys.stdout = old_stdout

    @patch('snowducks.cli.configure')
    @patch('snowducks.core.snowflake_query')
    def test_get_schema_failure(self, mock_query, mock_configure, mock_config):
        """Test schema extraction failure."""
        mock_configure.return_value = mock_config
        mock_query.side_effect = QueryError("Test error")
        
        # Capture stdout
        from io import StringIO
        import sys
        old_stdout = sys.stdout
        sys.stdout = captured_output = StringIO()
        
        try:
            # Mock sys.argv for get-schema command
            with patch('sys.argv', ['snowducks', 'get-schema', 'test_table_hash', 'SELECT * FROM nonexistent_table']):
                with patch('snowducks.ducklake_manager.get_table_schema_from_query') as mock_get_schema:
                    mock_get_schema.side_effect = QueryError("Test error")
                    main()
            
            # Get the output
            output = captured_output.getvalue()
            
            # Parse the JSON output
            result = json.loads(output)
            
            assert result["status"] == "error"
            assert "error" in result
            assert "Test error" in result["error"]
            
        finally:
            sys.stdout = old_stdout

    @patch('snowducks.cli.configure')
    @patch('snowducks.core.snowflake_query')
    def test_query_command_success(self, mock_query, mock_configure, mock_config):
        """Test successful query execution."""
        mock_configure.return_value = mock_config
        mock_query.return_value = ("test_table_hash", "miss")
        
        # Capture stdout
        from io import StringIO
        import sys
        old_stdout = sys.stdout
        sys.stdout = captured_output = StringIO()
        
        try:
            # Mock sys.argv for query command
            with patch('sys.argv', ['snowducks', 'query', '--query', 'SELECT id, name FROM test_table']):
                main()
            
            # Get the output
            output = captured_output.getvalue()
            
            # The query command prints to stdout, not JSON
            assert "Cache table: test_table_hash" in output
            assert "Status: miss" in output
            
        finally:
            sys.stdout = old_stdout

    @patch('snowducks.cli.configure')
    @patch('snowducks.core.snowflake_query')
    def test_query_command_failure(self, mock_query, mock_configure, mock_config):
        """Test query execution failure."""
        mock_configure.return_value = mock_config
        mock_query.side_effect = QueryError("Test error")
        
        # Capture stdout
        from io import StringIO
        import sys
        old_stdout = sys.stdout
        sys.stdout = captured_output = StringIO()
        
        try:
            # Mock sys.argv for query command
            with patch('sys.argv', ['snowducks', 'query', '--query', 'SELECT * FROM nonexistent_table']):
                with pytest.raises(SystemExit):
                    main()
            
            # Get the output
            output = captured_output.getvalue()
            
            assert "Error: Test error" in output
            
        finally:
            sys.stdout = old_stdout

    def test_main_get_schema(self):
        """Test main function with get-schema command."""
        # Capture stdout
        from io import StringIO
        import sys
        old_stdout = sys.stdout
        sys.stdout = captured_output = StringIO()
        
        try:
            # Mock sys.argv
            with patch('sys.argv', ['snowducks', 'get-schema', 'test_table', 'SELECT * FROM test']):
                with patch('snowducks.ducklake_manager.get_table_schema_from_query') as mock_get_schema:
                    mock_get_schema.return_value = [{"name": "id", "type": "INTEGER"}]
                    main()
                    
                    # Check that the function was called
                    mock_get_schema.assert_called_once_with('SELECT * FROM test')
                    
        finally:
            sys.stdout = old_stdout

    def test_main_query(self):
        """Test main function with query command."""
        # Capture stdout
        from io import StringIO
        import sys
        old_stdout = sys.stdout
        sys.stdout = captured_output = StringIO()
        
        try:
            # Mock sys.argv
            with patch('sys.argv', ['snowducks', 'query', '--query', 'SELECT * FROM test']):
                with patch('snowducks.core.snowflake_query') as mock_query:
                    mock_query.return_value = ("test_table_hash", "miss")
                    main()
                    
                    # Check that the function was called
                    mock_query.assert_called_once()
                    
        finally:
            sys.stdout = old_stdout

    def test_main_invalid_command(self):
        """Test main function with invalid command."""
        # Capture stdout (the error message goes to stdout, not stderr)
        from io import StringIO
        import sys
        old_stdout = sys.stdout
        sys.stdout = captured_output = StringIO()
        
        try:
            # Mock sys.argv
            with patch('sys.argv', ['snowducks', 'invalid-command']):
                main()
                
                # Check error message
                output = captured_output.getvalue()
                assert "Unknown command" in output
                
        finally:
            sys.stdout = old_stdout

    def test_main_missing_arguments(self):
        """Test main function with missing arguments."""
        # Capture stderr
        from io import StringIO
        import sys
        old_stderr = sys.stderr
        sys.stderr = captured_stderr = StringIO()
        
        try:
            # Mock sys.argv
            with patch('sys.argv', ['snowducks']):
                main()
                
                # Check help message
                stderr_output = captured_stderr.getvalue()
                # The help should be printed to stdout, not stderr
                
        finally:
            sys.stderr = old_stderr

    def test_main_help(self):
        """Test main function with help."""
        # Capture stdout
        from io import StringIO
        import sys
        old_stdout = sys.stdout
        sys.stdout = captured_output = StringIO()
        
        try:
            # Mock sys.argv with no arguments (should show help)
            with patch('sys.argv', ['snowducks']):
                main()
                
                # Check help message
                output = captured_output.getvalue()
                assert "SnowDucks CLI" in output
                assert "Commands:" in output
                
        finally:
            sys.stdout = old_stdout


class TestCLIIntegration:
    """Integration tests for CLI with mocked dependencies."""

    @pytest.fixture
    def temp_env(self):
        """Create temporary environment with .env file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            env_file = temp_path / ".env"
            env_content = f"""
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=snowducks_metadata
POSTGRES_USER=snowducks_user
POSTGRES_PASSWORD=snowducks_password
POSTGRES_SCHEMA=snowducks
SNOWFLAKE_USER=test_user
SNOWFLAKE_PASSWORD=test_password
SNOWFLAKE_ACCOUNT=test_account
SNOWFLAKE_DATABASE=test_database
SNOWFLAKE_WAREHOUSE=test_warehouse
SNOWFLAKE_ROLE=test_role
DUCKLAKE_METADATA_PATH={temp_path}/metadata.ducklake
DUCKLAKE_DATA_PATH={temp_path}/data
"""
            env_file.write_text(env_content)
            yield temp_path

    @patch('snowducks.core.snowflake_query')
    @patch('snowducks.cli.configure')
    def test_cli_with_mocked_config(self, mock_configure, mock_query, temp_env):
        """Test CLI with mocked configuration loading."""
        # Mock the configuration
        from snowducks.config import SnowDucksConfig
        mock_config = SnowDucksConfig(
            snowflake_user="test_user",
            snowflake_password="test_password",
            snowflake_account="test_account",
            snowflake_database="test_database",
            snowflake_warehouse="test_warehouse",
            snowflake_role="test_role",
            ducklake_metadata_path=temp_env / "metadata.ducklake",
            ducklake_data_path=temp_env / "data",
            allow_unlimited_egress=False,
            default_row_limit=1000,
        )
        mock_configure.return_value = mock_config
        mock_query.return_value = ("test_table_hash", "miss")
        
        # Capture stdout
        from io import StringIO
        import sys
        old_stdout = sys.stdout
        sys.stdout = captured_output = StringIO()
        
        try:
            # Change to the temp directory
            old_cwd = os.getcwd()
            os.chdir(temp_env)
            
            # Mock sys.argv for query command
            with patch('sys.argv', ['snowducks', 'query', '--query', 'SELECT 1 as test']):
                main()
            
            # Get the output
            output = captured_output.getvalue()
            
            assert "Cache table: test_table_hash" in output
            assert "Status: miss" in output
            
        finally:
            sys.stdout = old_stdout
            os.chdir(old_cwd)

    @patch('snowducks.core.snowflake_query')
    @patch('snowducks.cli.configure')
    def test_cli_with_cache_hit(self, mock_configure, mock_query, temp_env):
        """Test CLI with cache hit scenario."""
        # Mock the configuration
        from snowducks.config import SnowDucksConfig
        mock_config = SnowDucksConfig(
            snowflake_user="test_user",
            snowflake_password="test_password",
            snowflake_account="test_account",
            snowflake_database="test_database",
            snowflake_warehouse="test_warehouse",
            snowflake_role="test_role",
            ducklake_metadata_path=temp_env / "metadata.ducklake",
            ducklake_data_path=temp_env / "data",
            allow_unlimited_egress=False,
            default_row_limit=1000,
        )
        mock_configure.return_value = mock_config
        mock_query.return_value = ("test_table_hash", "hit")
        
        # Capture stdout
        from io import StringIO
        import sys
        old_stdout = sys.stdout
        sys.stdout = captured_output = StringIO()
        
        try:
            # Change to the temp directory
            old_cwd = os.getcwd()
            os.chdir(temp_env)
            
            # Mock sys.argv for query command
            with patch('sys.argv', ['snowducks', 'query', '--query', 'SELECT 1 as test']):
                main()
            
            # Get the output
            output = captured_output.getvalue()
            
            assert "Cache table: test_table_hash" in output
            assert "Status: hit" in output
            
        finally:
            sys.stdout = old_stdout
            os.chdir(old_cwd)


class TestCLIErrorHandling:
    """Test error handling in CLI."""

    @patch('snowducks.core.snowflake_query')
    @patch('snowducks.cli.configure')
    def test_configuration_error(self, mock_configure, mock_query):
        """Test CLI with configuration error."""
        # The error should happen during snowflake_query, not configure
        mock_configure.return_value = None  # configure succeeds
        mock_query.side_effect = ConfigError("Configuration error")
        
        # Capture stdout
        from io import StringIO
        import sys
        old_stdout = sys.stdout
        sys.stdout = captured_output = StringIO()
        
        try:
            # Mock sys.argv for query command
            with patch('sys.argv', ['snowducks', 'query', '--query', 'SELECT 1 as test']):
                with pytest.raises(SystemExit):
                    main()
            
            # Get the output
            output = captured_output.getvalue()
            
            assert "Error: Configuration error" in output
            
        finally:
            sys.stdout = old_stdout

    @patch('snowducks.core.snowflake_query')
    @patch('snowducks.cli.configure')
    def test_connection_error(self, mock_configure, mock_query):
        """Test CLI with connection error."""
        from snowducks.exceptions import ConnectionError
        # The error should happen during snowflake_query, not configure
        mock_configure.return_value = None  # configure succeeds
        mock_query.side_effect = ConnectionError("Connection failed")
        
        # Capture stdout
        from io import StringIO
        import sys
        old_stdout = sys.stdout
        sys.stdout = captured_output = StringIO()
        
        try:
            # Mock sys.argv for query command
            with patch('sys.argv', ['snowducks', 'query', '--query', 'SELECT 1 as test']):
                with pytest.raises(SystemExit):
                    main()
            
            # Get the output
            output = captured_output.getvalue()
            
            assert "Error: Connection failed" in output
            
        finally:
            sys.stdout = old_stdout

    def test_get_schema_missing_arguments(self):
        """Test get-schema command with missing arguments."""
        # Capture stdout
        from io import StringIO
        import sys
        old_stdout = sys.stdout
        sys.stdout = captured_output = StringIO()
        
        try:
            # Mock sys.argv with missing arguments
            with patch('sys.argv', ['snowducks', 'get-schema', 'test_table']):
                with pytest.raises(SystemExit):
                    main()
            
            # Get the output
            output = captured_output.getvalue()
            
            assert "Error: Table name and original query required" in output
            
        finally:
            sys.stdout = old_stdout


class TestCLIArgumentParsing:
    """Test CLI argument parsing."""

    def test_get_schema_with_quoted_query(self):
        """Test get-schema command with quoted query."""
        # Capture stdout
        from io import StringIO
        import sys
        old_stdout = sys.stdout
        sys.stdout = captured_output = StringIO()
        
        try:
            # Mock sys.argv with quoted query
            with patch('sys.argv', ['snowducks', 'get-schema', 'test_table', 'SELECT * FROM "users" WHERE name = \'John\'']):
                with patch('snowducks.ducklake_manager.get_table_schema_from_query') as mock_get_schema:
                    mock_get_schema.return_value = [{"name": "id", "type": "INTEGER"}]
                    main()
                    
                    # Check that the function was called with the correct query
                    mock_get_schema.assert_called_once_with('SELECT * FROM "users" WHERE name = \'John\'')
                    
        finally:
            sys.stdout = old_stdout

    def test_query_with_special_characters(self):
        """Test query command with special characters."""
        # Capture stdout
        from io import StringIO
        import sys
        old_stdout = sys.stdout
        sys.stdout = captured_output = StringIO()
        
        try:
            # Mock sys.argv with special characters
            with patch('sys.argv', ['snowducks', 'query', '--query', 'SELECT * FROM users WHERE name LIKE \'%test%\'']):
                with patch('snowducks.core.snowflake_query') as mock_query:
                    mock_query.return_value = ("test_table_hash", "miss")
                    main()
                    
                    # Check that the function was called with the correct query
                    mock_query.assert_called_once()
                    call_args = mock_query.call_args
                    assert call_args[1]['query_text'] == 'SELECT * FROM users WHERE name LIKE \'%test%\''
                    
        finally:
            sys.stdout = old_stdout 