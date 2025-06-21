"""
Tests for the configuration module.
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from snowducks.config import SnowDucksConfig
from snowducks.exceptions import ConfigError


class TestSnowDucksConfig:
    """Test cases for SnowDucksConfig."""

    def test_from_env_missing_required_params(self):
        """Test that missing required parameters raise ConfigError."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ConfigError, match="Missing required environment variables"):
                SnowDucksConfig.from_env(load_env_file=False)

    def test_from_env_password_auth(self):
        """Test configuration with password authentication."""
        env_vars = {
            "SNOWFLAKE_USER": "test_user",
            "SNOWFLAKE_PASSWORD": "test_password",
            "SNOWFLAKE_ACCOUNT": "test_account",
            "SNOWFLAKE_DATABASE": "test_database",
            "SNOWFLAKE_WAREHOUSE": "test_warehouse",
            "SNOWFLAKE_ROLE": "test_role",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = SnowDucksConfig.from_env()

            assert config.snowflake_user == "test_user"
            assert config.snowflake_password == "test_password"
            assert config.snowflake_account == "test_account"
            assert config.snowflake_database == "test_database"
            assert config.snowflake_warehouse == "test_warehouse"
            assert config.snowflake_role == "test_role"
            assert config.deployment_mode == "local"

    def test_from_env_key_pair_auth(self):
        """Test configuration with key pair authentication."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.p8', delete=False) as temp_key:
            temp_key.write("test private key content")
            temp_key.flush()
            temp_key_path = temp_key.name

        env_vars = {
            "SNOWFLAKE_USER": "test_user",
            "SNOWFLAKE_PRIVATE_KEY_PATH": temp_key_path,
            "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE": "test_passphrase",
            "SNOWFLAKE_ACCOUNT": "test_account",
            "SNOWFLAKE_DATABASE": "test_database",
            "SNOWFLAKE_WAREHOUSE": "test_warehouse",
            "SNOWFLAKE_ROLE": "test_role",
        }

        try:
            with patch.dict(os.environ, env_vars, clear=True):
                config = SnowDucksConfig.from_env(load_env_file=False)
                assert config.snowflake_user == "test_user"
                assert config.snowflake_private_key_path == temp_key_path, f"Expected {temp_key_path}, got {config.snowflake_private_key_path}"
                assert config.snowflake_private_key_passphrase == "test_passphrase"
        finally:
            os.unlink(temp_key_path)

    def test_from_env_sso_auth(self):
        """Test configuration with SSO authentication."""
        env_vars = {
            "SNOWFLAKE_USER": "test_user",
            "SNOWFLAKE_AUTHENTICATOR": "externalbrowser",
            "SNOWFLAKE_ACCOUNT": "test_account",
            "SNOWFLAKE_DATABASE": "test_database",
            "SNOWFLAKE_WAREHOUSE": "test_warehouse",
            "SNOWFLAKE_ROLE": "test_role",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = SnowDucksConfig.from_env()

            assert config.snowflake_user == "test_user"
            assert config.snowflake_authenticator == "externalbrowser"
            assert config.snowflake_account == "test_account"

    def test_from_env_no_auth_method(self):
        """Test that missing authentication method raises ConfigError."""
        env_vars = {
            "SNOWFLAKE_USER": "test_user",
            "SNOWFLAKE_ACCOUNT": "test_account",
            "SNOWFLAKE_DATABASE": "test_database",
            "SNOWFLAKE_WAREHOUSE": "test_warehouse",
            "SNOWFLAKE_ROLE": "test_role",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(ConfigError, match="No authentication method found"):
                SnowDucksConfig.from_env(load_env_file=False)

    def test_from_env_optional_params(self):
        """Test configuration with optional parameters."""
        env_vars = {
            "SNOWFLAKE_USER": "test_user",
            "SNOWFLAKE_PASSWORD": "test_password",
            "SNOWFLAKE_ACCOUNT": "test_account",
            "SNOWFLAKE_DATABASE": "test_database",
            "SNOWFLAKE_WAREHOUSE": "test_warehouse",
            "SNOWFLAKE_ROLE": "test_role",
            "DUCKLAKE_METADATA_PATH": "/custom/metadata.ducklake",
            "DUCKLAKE_DATA_PATH": "/custom/data",
            "S3_BUCKET": "test-bucket",
            "AWS_REGION": "us-west-2",
            "ALLOW_UNLIMITED_EGRESS": "TRUE",
            "DEFAULT_ROW_LIMIT": "5000",
            "CACHE_MAX_AGE_HOURS": "48",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = SnowDucksConfig.from_env()

            assert config.ducklake_metadata_path == Path("/custom/metadata.ducklake")
            assert config.ducklake_data_path == Path("/custom/data")
            assert config.s3_bucket == "test-bucket"
            assert config.aws_region == "us-west-2"
            assert config.allow_unlimited_egress is True
            assert config.default_row_limit == 5000
            assert config.cache_max_age_hours == 48

    def test_validate_password_auth(self):
        """Test validation with password authentication."""
        config = SnowDucksConfig(
            snowflake_user="test_user",
            snowflake_password="test_password",
            snowflake_account="test_account",
            snowflake_database="test_database",
            snowflake_warehouse="test_warehouse",
            snowflake_role="test_role",
        )
        
        # Should not raise any exception
        config.validate()

    def test_validate_key_pair_auth(self):
        """Test validation with key pair authentication."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.p8') as temp_key:
            temp_key.write("test private key content")
            temp_key.flush()

            config = SnowDucksConfig(
                snowflake_user="test_user",
                snowflake_private_key_path=temp_key.name,
                snowflake_account="test_account",
                snowflake_database="test_database",
                snowflake_warehouse="test_warehouse",
                snowflake_role="test_role",
            )
            
            # Should not raise any exception
            config.validate()

    def test_validate_sso_auth(self):
        """Test validation with SSO authentication."""
        config = SnowDucksConfig(
            snowflake_user="test_user",
            snowflake_authenticator="externalbrowser",
            snowflake_account="test_account",
            snowflake_database="test_database",
            snowflake_warehouse="test_warehouse",
            snowflake_role="test_role",
        )
        
        # Should not raise any exception
        config.validate()

    def test_validate_no_auth_method(self):
        """Test validation with no authentication method."""
        with pytest.raises(ConfigError, match="No valid authentication method configured"):
            SnowDucksConfig(
                snowflake_user="test_user",
                snowflake_account="test_account",
                snowflake_database="test_database",
                snowflake_warehouse="test_warehouse",
                snowflake_role="test_role",
            )

    def test_validate_invalid_key_path(self):
        """Test validation with invalid private key path."""
        with pytest.raises(ConfigError, match="Private key file not found"):
            SnowDucksConfig(
                snowflake_user="test_user",
                snowflake_private_key_path="/nonexistent/key.p8",
                snowflake_account="test_account",
                snowflake_database="test_database",
                snowflake_warehouse="test_warehouse",
                snowflake_role="test_role",
            )

    def test_validate_s3_bucket_no_region(self):
        """Test validation with S3 bucket but no region."""
        with pytest.raises(ConfigError, match="AWS_REGION is required"):
            SnowDucksConfig(
                snowflake_user="test_user",
                snowflake_password="test_password",
                snowflake_account="test_account",
                snowflake_database="test_database",
                snowflake_warehouse="test_warehouse",
                snowflake_role="test_role",
                s3_bucket="test-bucket",
                aws_region="",  # Empty region
            )

    def test_validate_negative_cache_age(self):
        """Test validation with negative cache age."""
        with pytest.raises(ConfigError, match="CACHE_MAX_AGE_HOURS must be non-negative"):
            SnowDucksConfig(
                snowflake_user="test_user",
                snowflake_password="test_password",
                snowflake_account="test_account",
                snowflake_database="test_database",
                snowflake_warehouse="test_warehouse",
                snowflake_role="test_role",
                cache_max_age_hours=-1,
            )

    def test_get_snowflake_connection_uri_password(self):
        """Test Snowflake connection URI generation with password auth."""
        config = SnowDucksConfig.for_testing(
            snowflake_user="test_user",
            snowflake_password="test_password",
            snowflake_account="test_account",
            snowflake_database="test_database",
            snowflake_warehouse="test_warehouse",
            snowflake_role="test_role",
        )

        uri = config.get_snowflake_connection_uri()
        # ADBC format: snowflake://account/?params
        assert "snowflake://test_account/" in uri
        assert "adbc.snowflake.sql.db=test_database" in uri
        assert "adbc.snowflake.sql.warehouse=test_warehouse" in uri
        assert "adbc.snowflake.sql.role=test_role" in uri

    def test_get_snowflake_connection_uri_key_pair(self):
        """Test Snowflake connection URI generation with key pair auth."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.p8') as temp_key:
            temp_key.write("test private key content")
            temp_key.flush()

            config = SnowDucksConfig.for_testing(
                snowflake_user="test_user",
                snowflake_private_key_path=temp_key.name,
                snowflake_private_key_passphrase="passphrase",
                snowflake_account="test_account",
                snowflake_database="test_database",
                snowflake_warehouse="test_warehouse",
                snowflake_role="test_role",
            )

            uri = config.get_snowflake_connection_uri()
            # ADBC format: snowflake://account/?params
            assert "snowflake://test_account/" in uri
            assert "adbc.snowflake.auth.private_key_path=" in uri
            assert "adbc.snowflake.auth.private_key_passphrase=passphrase" in uri

    def test_get_ducklake_attach_string_local(self):
        """Test DuckLake attach string generation for local deployment."""
        config = SnowDucksConfig(
            snowflake_user="test_user",
            snowflake_password="test_password",
            snowflake_account="test_account",
            snowflake_database="test_database",
            snowflake_warehouse="test_warehouse",
            snowflake_role="test_role",
            deployment_mode="local",
            postgres_host="localhost",
            postgres_port=5432,
            postgres_database="snowducks_metadata",
            postgres_user="snowducks_user",
            postgres_password="snowducks_password",
        )
        
        attach_string = config.get_ducklake_attach_string()
        assert attach_string.startswith("ducklake:postgres:")
        assert "dbname=snowducks_metadata" in attach_string
        assert "host=localhost" in attach_string
        assert "user=snowducks_user" in attach_string
        assert "password=snowducks_password" in attach_string

    def test_get_ducklake_attach_string_cloud(self):
        """Test DuckLake attach string generation for cloud deployment."""
        config = SnowDucksConfig(
            snowflake_user="test_user",
            snowflake_password="test_password",
            snowflake_account="test_account",
            snowflake_database="test_database",
            snowflake_warehouse="test_warehouse",
            snowflake_role="test_role",
            deployment_mode="cloud",
            s3_bucket="test-bucket",
            postgres_host="localhost",
            postgres_port=5432,
            postgres_database="snowducks_metadata",
            postgres_user="snowducks_user",
            postgres_password="snowducks_password",
        )
        
        attach_string = config.get_ducklake_attach_string()
        assert attach_string.startswith("ducklake:postgres:")
        assert "dbname=snowducks_metadata" in attach_string
        assert "host=localhost" in attach_string
        assert "user=snowducks_user" in attach_string
        assert "password=snowducks_password" in attach_string

    def test_for_local_development(self):
        """Test local development configuration factory."""
        config = SnowDucksConfig.for_local_development(
            snowflake_user="test_user",
            snowflake_password="test_password",
            snowflake_account="test_account",
            snowflake_database="test_database",
            snowflake_warehouse="test_warehouse",
            snowflake_role="test_role",
        )
        
        assert config.deployment_mode == "local"
        assert config.s3_bucket is None

    def test_for_cloud_deployment(self):
        """Test cloud deployment configuration factory."""
        config = SnowDucksConfig.for_cloud_deployment(
            s3_bucket="test-bucket",
            snowflake_user="test_user",
            snowflake_password="test_password",
            snowflake_account="test_account",
            snowflake_database="test_database",
            snowflake_warehouse="test_warehouse",
            snowflake_role="test_role",
        )
        
        assert config.deployment_mode == "cloud"
        assert config.s3_bucket == "test-bucket" 