# SnowDucks 🦆

A powerful DuckDB extension that seamlessly bridges Snowflake and DuckDB through intelligent caching and instant SQL execution.

## Overview

SnowDucks provides a unified interface for querying Snowflake data with automatic caching in Parquet format. It combines the performance of DuckDB with the scalability of Snowflake, offering:

- **🚀 Instant Query Execution**: Cached queries return results immediately
- **💾 Intelligent Caching**: Automatic Parquet file caching with smart invalidation
- **🔗 Seamless Integration**: Works as both a DuckDB extension and Python CLI
- **⚡ Performance**: Local DuckDB performance for cached data
- **🔄 Automatic Sync**: Fresh data from Snowflake when cache is stale
- **🎯 Guaranteed Data Availability**: Ensures data is fetched and cached before query execution

## Key Features

### 🎯 Guaranteed Order of Operations
SnowDucks ensures the correct order of operations:
1. **Bind Phase**: Check cache, fetch from Snowflake if needed, register table
2. **Schema Determination**: Determine table schema after data is available
3. **Execution Phase**: Execute queries against cached data

This guarantees that queries always have the right data available before execution begins.

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Snowflake     │    │   SnowDucks     │    │     DuckDB      │
│                 │    │                 │    │                 │
│ • Data Source   │◄──►│ • Query Cache   │◄──►│ • Fast Queries  │
│ • Compute       │    │ • Parquet Files │    │ • Local Storage │
│ • Scalability   │    │ • Metadata DB   │    │ • Extensions    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Quick Start

### 1. Setup

```bash
# Clone and setup
git clone https://github.com/danjsiegel/snowducks.git
cd snowducks
make init

# Configure your Snowflake credentials
cp env.example .env
# Edit .env with your Snowflake details
```

### 2. Start PostgreSQL (Recommended)

```bash
# Start PostgreSQL with Docker for metadata storage
make postgres-start
```

### 3. Use SnowDucks

**As a DuckDB Extension:**
```sql
-- Load the extension
LOAD 'build/release/extension/snowducks/snowducks.duckdb_extension';

-- Query Snowflake data (automatically cached)
SELECT * FROM snowducks_query('SELECT * FROM my_table LIMIT 1000');
```

**As a Python CLI:**
```bash
# Start the interactive DuckDB session
make cli

SELECT * FROM snowflake_query('SELECT * FROM my_table LIMIT 1000');
```

**Direct CLI Usage:**
```bash
# Run the CLI directly
./src/cli/snowducksi

# Or use Python module
python -m snowducks.cli

# Test connection
python -m snowducks.cli test

# Show cache stats
python -m snowducks.cli stats

# Clear cache
python -m snowducks.cli clear-cache

# Execute a single query
python -m snowducks.cli query --query "SELECT * FROM my_table LIMIT 100"
```

**With DuckDB UI (Recommended for Development):**
```bash
# Start DuckDB with web UI and SnowDucks extension
make ui

# This opens DuckDB with:
# - SnowDucks extension loaded
# - DuckLake extension for metadata
# - Web UI for interactive querying
# - All extensions pre-configured
```

## Features

### 🔄 Smart Caching
- **Automatic Cache Detection**: Checks for existing cached data before querying Snowflake
- **Hash-Based Naming**: Unique cache files based on query content and parameters
- **Cache Invalidation**: Configurable TTL and force refresh options
- **Parquet Format**: Efficient columnar storage for fast queries
- **Guaranteed Data Availability**: Data is fetched and cached before query execution

### 🛡️ Security & Authentication
- **Multiple Auth Methods**: Password, key pair, and SSO authentication
- **Environment Variables**: Secure credential management
- **Role-Based Access**: Respects Snowflake roles and permissions

### 🚀 Performance Optimizations
- **Local DuckDB Engine**: Cached data queries at local speeds
- **Columnar Storage**: Parquet format for efficient data access
- **Metadata Database**: PostgreSQL for tracking cache state
- **Connection Pooling**: Efficient Snowflake connection management
- **Optimized Execution Flow**: Bind phase ensures data availability before execution

### 🔧 Configuration Options
- **Deployment Modes**: Local development or cloud deployment
- **Storage Backends**: Local filesystem or S3 for cache storage
- **Database Options**: PostgreSQL or local DuckDB for metadata
- **Cost Controls**: Row limits and egress controls

## Configuration

### Environment Variables

```bash
# Required: Snowflake Configuration
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_ROLE=your_role

# Optional: PostgreSQL for metadata (recommended)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=snowducks_metadata
POSTGRES_USER=snowducks_user
POSTGRES_PASSWORD=snowducks_password

# Optional: S3 for cloud deployment
S3_BUCKET=your-snowducks-bucket
AWS_REGION=us-east-1

# Optional: Cache configuration
CACHE_MAX_AGE_HOURS=24
DEFAULT_ROW_LIMIT=1000
```

### Deployment Modes

**Local Development:**
```bash
DEPLOYMENT_MODE=local
DUCKLAKE_METADATA_PATH=~/.snowducks/metadata.ducklake
DUCKLAKE_DATA_PATH=~/.snowducks/data
```

**Cloud Deployment:**
```bash
DEPLOYMENT_MODE=cloud
S3_BUCKET=your-snowducks-bucket
AWS_REGION=us-east-1
```

## Installation

### Prerequisites

- **DuckDB**: v1.3.1 or later
- **Python**: 3.8 or later
- **PostgreSQL**: For metadata storage (optional but recommended)
- **Docker**: For PostgreSQL setup (optional)

### Build from Source

```bash
# Complete setup
make init

# Build extension only
make build

# Run tests
make test
```

### Docker Setup

```bash
# Start PostgreSQL with Docker
make postgres-start

# Access pgAdmin at http://localhost:8080
# Username: admin@snowducks.local
# Password: admin
```

## Usage Examples

### Basic Query Caching

```sql
-- First query: hits Snowflake, caches result
SELECT * FROM snowducks_query('SELECT * FROM sales WHERE date >= ''2024-01-01''');

-- Second query: uses cached data, instant response
SELECT * FROM snowducks_query('SELECT * FROM sales WHERE date >= ''2024-01-01''');
```

### Complex Analytics

```sql
-- Complex query with aggregations
SELECT 
    region,
    SUM(revenue) as total_revenue,
    COUNT(*) as order_count
FROM snowducks_query('
    SELECT region, revenue, order_id 
    FROM sales 
    WHERE date >= ''2024-01-01''
') 
GROUP BY region 
ORDER BY total_revenue DESC;
```

### Python Integration

```python
import duckdb

# Connect and load extension
con = duckdb.connect()
con.execute("LOAD 'snowducks.duckdb_extension'")

# Query with automatic caching
result = con.execute("""
    SELECT * FROM snowducks_query('
        SELECT customer_id, revenue 
        FROM sales 
        WHERE region = ''North America''
    ')
""").fetchall()
```

## API Reference

### DuckDB Extension Functions

#### `snowducks_query(query_text)`
Execute a Snowflake query with automatic caching.

**Parameters:**
- `query_text` (VARCHAR): SQL query to execute

**Returns:** Table with query results

**Example:**
```sql
SELECT * FROM snowducks_query('SELECT * FROM my_table LIMIT 100');
```

#### `snowducks_info(info_type)`
Get information about the SnowDucks extension.

**Parameters:**
- `info_type` (VARCHAR): Type of info to retrieve ('extension', 'cache', 'config')

**Returns:** Information about the specified type

**Example:**
```sql
SELECT * FROM snowducks_info('cache');
```

### Python CLI Commands

#### `query <sql>`
Execute a Snowflake query with caching.

```bash
snowducks> query SELECT * FROM sales LIMIT 10;
```

#### `cache list`
List all cached queries.

```bash
snowducks> cache list
```

#### `cache clear`
Clear all cached data.

```bash
snowducks> cache clear
```

#### `config show`
Show current configuration.

```bash
snowducks> config show
```

## Development

### Project Structure

```
snowducks/
├── src/
│   ├── snowducks_extension.cpp    # C++ DuckDB extension
│   ├── include/                   # C++ headers
│   └── cli/                       # Python CLI
│       ├── snowducks/             # Python package
│       ├── snowducksi             # CLI entry point
│       └── pyproject.toml         # Python build config
├── test/                          # Test suites
│   ├── python/                    # Python tests
│   ├── cpp/                       # C++ tests
│   └── sql/                       # SQL tests
├── duckdb/                        # Vendored DuckDB
├── extension-ci-tools/            # Build tools
└── docs/                          # Documentation
```

### Building

```bash
# Build everything
make build

# Build extension only
make build-extension

# Build Python package
make build-python
```

### Testing

```bash
# Run all tests
make test

# Run specific test suites
make test-cli      # Python tests
make test-ext      # C++ tests
make test-sql      # SQL tests
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## Troubleshooting

### Common Issues

**Extension won't load:**
```bash
# Check DuckDB version
duckdb --version

# Ensure extension is built
make build
```

**PostgreSQL connection issues:**
```bash
# Check PostgreSQL status
make postgres-status

# Restart PostgreSQL
make postgres-stop
make postgres-start
```

**Cache not working:**
```bash
# Check cache directory permissions
ls -la ~/.snowducks/

# Clear cache and retry
snowducks> cache clear
```

### Debug Mode

Enable debug logging:

```bash
# Set debug environment variable
export SNOWDUCKS_DEBUG=true

# Run with debug output
make cli
```

## Technical Details

### Order of Operations

SnowDucks implements a carefully designed order of operations to ensure data availability:

**Bind Phase**: 
- Check if cached data exists
    - If cached: Get schema from the cached table
- If NOT cached: Get schema from query parsing (via Python CLI) 

**Execution Phase**: 
- If table is NOT cached: Fetch data from Snowflake and cache it
- Execute queries against the cached table

## Future Development Ideas

Based on DuckLake's capabilities and community feedback opportunities, here are potential enhancements for future versions:

### 🗄️ Additional Storage Backends
- **Google Cloud Storage**: Native GCS support for cache storage
- **Azure Blob Storage**: Azure integration for enterprise deployments
- **MinIO/S3-Compatible**: Support for on-premises S3-compatible storage
- **Local Network Storage**: NFS/SMB support for shared cache directories

### 🗃️ Database Backend Options
- **MySQL/MariaDB**: Alternative metadata database backend
- **SQLite**: Lightweight metadata storage for embedded deployments
- **MongoDB**: Document-based metadata storage
- **Redis**: In-memory metadata for high-performance scenarios

### 🔄 Advanced Caching Features
- **Incremental Caching**: Cache only new/changed data
- **Partitioned Caching**: Cache by date ranges or other partitions
- **Compression Options**: Configurable compression for cache files
- **Cache Warming**: Pre-populate cache with frequently used queries
- **Distributed Caching**: Share cache across multiple instances

### 🔐 Enhanced Security
- **Encryption at Rest**: Encrypt cached Parquet files
- **Vault Integration**: HashiCorp Vault for credential management
- **OAuth2 Support**: Modern authentication flows
- **Audit Logging**: Track all cache operations and queries

### 📊 Monitoring & Observability
- **Metrics Export**: Prometheus metrics for monitoring
- **Query Performance Analytics**: Track query performance over time
- **Cache Hit Rate Monitoring**: Monitor cache effectiveness
- **Cost Tracking**: Track Snowflake compute costs

### 🚀 Performance Optimizations
- **Parallel Query Execution**: Execute multiple queries concurrently
- **Query Result Streaming**: Stream large results without full materialization
- **Smart Query Routing**: Route queries to optimal backend
- **Connection Pooling**: Advanced connection management

### 🔧 Configuration Enhancements
- **Dynamic Configuration**: Runtime configuration changes
- **Configuration Validation**: Validate config at startup
- **Configuration Templates**: Pre-built configs for common scenarios
- **Environment-Specific Configs**: Dev/staging/prod configurations

### 🌐 Integration Features
- **Jupyter Integration**: Native Jupyter notebook support
- **dbt Integration**: Work with dbt models and transformations
- **Airflow Integration**: Apache Airflow operators
- **Kubernetes**: Native K8s deployment support

### 📈 Enterprise Features
- **Multi-Tenant Support**: Isolated caching per tenant
- **RBAC Integration**: Role-based access control
- **Compliance Features**: GDPR, SOX compliance tools
- **Backup & Recovery**: Automated cache backup strategies

### 🎯 Community-Driven Features
- **Plugin System**: Extensible architecture for custom features
- **Query Templates**: Pre-built query templates for common use cases
- **Community Cache Sharing**: Share cache definitions across teams
- **Query Optimization Hints**: AI-powered query optimization suggestions

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.


## Acknowledgments

- **DuckDB Team**: For the excellent database engine and extension framework
- **DuckLake Contributors**: For the metadata management system
- **Snowflake**: For the powerful cloud data platform
- **Open Source Community**: For the amazing tools and libraries that make this possible

---

**Made with ❤️ by the SnowDucks community**