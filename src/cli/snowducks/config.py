"""
Configuration management for SnowDucks using DuckLake.
"""

import os
import platform
from pathlib import Path
from typing import Optional, Dict, Any, Literal
from dataclasses import dataclass, field
from dotenv import load_dotenv
import urllib.parse

from .exceptions import ConfigError


@dataclass
class SnowDucksConfig:
    """Configuration for SnowDucks library using DuckLake."""

    # Snowflake connection parameters
    snowflake_user: str
    snowflake_account: str
    snowflake_database: str
    snowflake_warehouse: str
    snowflake_role: str

    # Authentication method
    snowflake_password: Optional[str] = None
    snowflake_private_key_path: Optional[str] = None
    snowflake_private_key_passphrase: Optional[str] = None
    snowflake_authenticator: str = "snowflake"

    # DuckLake configuration
    ducklake_metadata_path: Path = field(
        default_factory=lambda: Path.home() / ".snowducks" / "metadata.ducklake"
    )
    ducklake_data_path: Path = field(
        default_factory=lambda: Path.home() / ".snowducks" / "data"
    )

    # PostgreSQL configuration for DuckLake metadata
    postgres_host: Optional[str] = None
    postgres_port: int = 5432
    postgres_database: Optional[str] = None
    postgres_user: Optional[str] = None
    postgres_password: Optional[str] = None
    postgres_schema: str = "snowducks"

    # S3 configuration for shared DuckLake
    s3_bucket: Optional[str] = None
    aws_region: str = "us-east-1"
    s3_endpoint_url: Optional[str] = None

    # Deployment mode
    deployment_mode: Literal["local", "cloud"] = "local"

    # Cache recency configuration
    cache_max_age_hours: int = 24  # How old cached data can be before requiring refresh
    cache_force_refresh: bool = False  # Force refresh all queries

    # Cost control
    allow_unlimited_egress: bool = False
    default_row_limit: int = 1000

    @classmethod
    def from_env(
        cls, env_file: Optional[str] = None, load_env_file: bool = True
    ) -> "SnowDucksConfig":
        """Create configuration from environment variables."""
        if load_env_file:
            if env_file:
                load_dotenv(env_file)
            else:
                # Load .env file from current directory by default
                load_dotenv()

        # Required parameters
        required_params = {
            "snowflake_user": os.getenv("SNOWFLAKE_USER"),
            "snowflake_account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "snowflake_database": os.getenv("SNOWFLAKE_DATABASE"),
            "snowflake_warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "snowflake_role": os.getenv("SNOWFLAKE_ROLE"),
        }

        # Check for missing required parameters
        missing_params = [k for k, v in required_params.items() if not v]
        if missing_params:
            raise ConfigError(
                f"Missing required environment variables: {', '.join(missing_params)}"
            )

        # Authentication method detection
        auth_method = cls._detect_auth_method()

        # Environment detection
        deployment_mode = cls._detect_deployment_mode()

        config = cls(
            snowflake_user=required_params["snowflake_user"],
            snowflake_account=required_params["snowflake_account"],
            snowflake_database=required_params["snowflake_database"],
            snowflake_warehouse=required_params["snowflake_warehouse"],
            snowflake_role=required_params["snowflake_role"],
            deployment_mode=deployment_mode,
            **auth_method,
        )

        # Optional parameters
        if os.getenv("SNOWFLAKE_AUTHENTICATOR"):
            config.snowflake_authenticator = os.getenv("SNOWFLAKE_AUTHENTICATOR")

        # DuckLake paths
        if os.getenv("DUCKLAKE_METADATA_PATH"):
            config.ducklake_metadata_path = Path(os.getenv("DUCKLAKE_METADATA_PATH"))

        if os.getenv("DUCKLAKE_DATA_PATH"):
            config.ducklake_data_path = Path(os.getenv("DUCKLAKE_DATA_PATH"))

        # S3 configuration
        if os.getenv("S3_BUCKET"):
            config.s3_bucket = os.getenv("S3_BUCKET")

        if os.getenv("AWS_REGION"):
            config.aws_region = os.getenv("AWS_REGION")

        if os.getenv("S3_ENDPOINT_URL"):
            config.s3_endpoint_url = os.getenv("S3_ENDPOINT_URL")

        # PostgreSQL configuration
        if os.getenv("POSTGRES_HOST"):
            config.postgres_host = os.getenv("POSTGRES_HOST")

        if os.getenv("POSTGRES_PORT"):
            config.postgres_port = int(os.getenv("POSTGRES_PORT"))

        if os.getenv("POSTGRES_DATABASE"):
            config.postgres_database = os.getenv("POSTGRES_DATABASE")

        if os.getenv("POSTGRES_USER"):
            config.postgres_user = os.getenv("POSTGRES_USER")

        if os.getenv("POSTGRES_PASSWORD"):
            config.postgres_password = os.getenv("POSTGRES_PASSWORD")

        if os.getenv("POSTGRES_SCHEMA"):
            config.postgres_schema = os.getenv("POSTGRES_SCHEMA")

        # Cache recency
        if os.getenv("CACHE_MAX_AGE_HOURS"):
            config.cache_max_age_hours = int(os.getenv("CACHE_MAX_AGE_HOURS"))

        if os.getenv("CACHE_FORCE_REFRESH"):
            config.cache_force_refresh = (
                os.getenv("CACHE_FORCE_REFRESH").upper() == "TRUE"
            )

        # Cost controls
        if os.getenv("ALLOW_UNLIMITED_EGRESS"):
            config.allow_unlimited_egress = (
                os.getenv("ALLOW_UNLIMITED_EGRESS").upper() == "TRUE"
            )

        if os.getenv("DEFAULT_ROW_LIMIT"):
            config.default_row_limit = int(os.getenv("DEFAULT_ROW_LIMIT"))

        return config

    @staticmethod
    def _detect_auth_method() -> Dict[str, Any]:
        """Detect the authentication method from environment variables."""
        auth_config = {}

        # Check for password authentication
        if os.getenv("SNOWFLAKE_PASSWORD"):
            auth_config["snowflake_password"] = os.getenv("SNOWFLAKE_PASSWORD")
            return auth_config

        # Check for key pair authentication
        if os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"):
            auth_config["snowflake_private_key_path"] = os.getenv(
                "SNOWFLAKE_PRIVATE_KEY_PATH"
            )
            if os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"):
                auth_config["snowflake_private_key_passphrase"] = os.getenv(
                    "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"
                )
            return auth_config

        # Check for SSO authentication
        if os.getenv("SNOWFLAKE_AUTHENTICATOR") == "externalbrowser":
            auth_config["snowflake_authenticator"] = "externalbrowser"
            return auth_config

        # Default to password authentication
        raise ConfigError(
            "No authentication method found. Please set one of: "
            "SNOWFLAKE_PASSWORD, SNOWFLAKE_PRIVATE_KEY_PATH, or SNOWFLAKE_AUTHENTICATOR=externalbrowser"
        )

    @staticmethod
    def _detect_deployment_mode() -> Literal["local", "cloud"]:
        """Detect if running in cloud environment."""
        # Check for cloud environment indicators
        cloud_indicators = [
            os.getenv("AWS_EXECUTION_ENV"),  # Lambda
            os.getenv("ECS_CONTAINER_METADATA_URI"),  # ECS
            os.getenv("KUBERNETES_SERVICE_HOST"),  # Kubernetes
            os.getenv("ECS_CONTAINER_METADATA_URI_V4"),  # ECS v4
        ]

        if any(cloud_indicators):
            return "cloud"

        # Check if running on EC2
        try:
            import requests

            # Try to access EC2 metadata service (only available on EC2)
            response = requests.get(
                "http://169.254.169.254/latest/meta-data/instance-id", timeout=1
            )
            if response.status_code == 200:
                return "cloud"
        except:
            pass

        return "local"

    @classmethod
    def for_local_development(cls, **kwargs) -> "SnowDucksConfig":
        """Create configuration optimized for local development."""
        config = cls(**kwargs)
        config.deployment_mode = "local"
        config.s3_bucket = None  # Disable S3 for local dev
        return config

    @classmethod
    def for_cloud_deployment(cls, s3_bucket: str, **kwargs) -> "SnowDucksConfig":
        """Create configuration optimized for cloud deployment."""
        config = cls(**kwargs)
        config.deployment_mode = "cloud"
        config.s3_bucket = s3_bucket
        return config

    @classmethod
    def for_testing(cls, **kwargs) -> "SnowDucksConfig":
        """Create configuration for testing without validation."""
        config = cls(**kwargs)
        # Skip validation for testing
        return config

    def get_snowflake_connection_uri(self) -> str:
        """Generate the Snowflake connection URI for ADBC."""
        # Base URI without authentication credentials
        base_uri = f"snowflake://{self.snowflake_account}/"

        # Add query parameters
        params = [
            f"adbc.snowflake.sql.db={self.snowflake_database}",
            f"adbc.snowflake.sql.warehouse={self.snowflake_warehouse}",
            f"adbc.snowflake.sql.role={self.snowflake_role}",
        ]

        if self.snowflake_authenticator != "snowflake":
            params.append(
                f"adbc.snowflake.auth.authenticator={self.snowflake_authenticator}"
            )

        if self.snowflake_private_key_path:
            params.append(
                f"adbc.snowflake.auth.private_key_path={self.snowflake_private_key_path}"
            )
            if self.snowflake_private_key_passphrase:
                params.append(
                    f"adbc.snowflake.auth.private_key_passphrase={self.snowflake_private_key_passphrase}"
                )

        return f"{base_uri}?{'&'.join(params)}"

    def get_ducklake_attach_string(self) -> str:
        """Generate the DuckLake ATTACH string for DuckLake+Postgres only."""
        if not (
            self.postgres_host
            and self.postgres_database
            and self.postgres_user
            and self.postgres_password
        ):
            raise ConfigError(
                "PostgreSQL configuration is required for DuckLake metadata."
            )
        return f"ducklake:postgres:dbname={self.postgres_database} host={self.postgres_host} user={self.postgres_user} password={self.postgres_password}"

    def get_duckdb_database_path(self) -> str:
        """Get the DuckDB database path for both CLI and extension use."""
        # Ensure the metadata directory exists
        self.ducklake_metadata_path.parent.mkdir(parents=True, exist_ok=True)
        return str(self.ducklake_metadata_path)

    def initialize_ducklake_database(self) -> None:
        """Initialize the DuckLake database with proper schema."""
        import duckdb

        # Ensure the metadata directory exists
        self.ducklake_metadata_path.parent.mkdir(parents=True, exist_ok=True)

        # Connect to the database to initialize it
        con = duckdb.connect(str(self.ducklake_metadata_path))

        # Use a unique schema name to avoid ambiguity
        con.execute("CREATE SCHEMA IF NOT EXISTS sd_metadata;")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS sd_metadata.query_cache (
                query_hash VARCHAR PRIMARY KEY,
                query_text TEXT NOT NULL,
                cache_table_name VARCHAR NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                row_count INTEGER,
                cache_hits INTEGER DEFAULT 0
            );
        """
        )

        con.close()

    def validate(self) -> None:
        """Validate the configuration."""
        # Ensure DuckLake directories exist
        self.ducklake_metadata_path.parent.mkdir(parents=True, exist_ok=True)
        self.ducklake_data_path.mkdir(parents=True, exist_ok=True)

        # Validate authentication method
        if not any(
            [
                self.snowflake_password,
                self.snowflake_private_key_path,
                self.snowflake_authenticator == "externalbrowser",
            ]
        ):
            raise ConfigError("No valid authentication method configured")

        # Validate key pair authentication
        if self.snowflake_private_key_path:
            key_path = Path(self.snowflake_private_key_path)
            if not key_path.exists():
                raise ConfigError(
                    f"Private key file not found: {self.snowflake_private_key_path}"
                )

        # Validate S3 configuration
        if self.s3_bucket and not self.aws_region:
            raise ConfigError("AWS_REGION is required when S3_BUCKET is set")

        # Validate PostgreSQL configuration
        if self.postgres_host:
            if not self.postgres_database:
                raise ConfigError(
                    "POSTGRES_DATABASE is required when POSTGRES_HOST is set"
                )
            if not self.postgres_user:
                raise ConfigError("POSTGRES_USER is required when POSTGRES_HOST is set")
            if not self.postgres_password:
                raise ConfigError(
                    "POSTGRES_PASSWORD is required when POSTGRES_HOST is set"
                )
            if self.postgres_port < 1 or self.postgres_port > 65535:
                raise ConfigError("POSTGRES_PORT must be between 1 and 65535")

        # Validate cache recency
        if self.cache_max_age_hours < 0:
            raise ConfigError("CACHE_MAX_AGE_HOURS must be non-negative")

    def __post_init__(self) -> None:
        """Post-initialization validation."""
        self.validate()
