# SnowDucks 🦆

A powerful DuckDB extension that seamlessly bridges Snowflake and DuckDB through intelligent caching and instant SQL execution.

## Overview

SnowDucks provides a unified interface for querying Snowflake data with automatic caching in Parquet format. It combines the performance of DuckDB with the scalability of Snowflake, offering:

- **🚀 Instant Query Execution**: Cached queries return results immediately
- **💾 Intelligent Caching**: Automatic Parquet file caching with smart invalidation
- **🔗 Seamless Integration**: Works as both a DuckDB extension and Python CLI
- **⚡ Performance**: Local DuckDB performance for cached data
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

### 🏗️ Architectural Rearchitecture: Pure C++ Implementation

**The Problem I Built Myself Into:**
So I built the Python CLI stuff in isolation without really reading the DuckDB extension docs properly. I got it fully working with the DuckDB CLI as a UDF, then I tried to import the UDF to the DuckDB UI, turns out you can't actually do that. 

**What I Was Doing Wrong:**
I was trying to load a Python UDF into the DuckDB UI, but it needed to be compiled binaries (`.duckdb_extension` files) that are platform-specific and version-tied (or I'm just an idiot and I don't know what I'm talking about. Either is viable). The extension-template requires you to actually build a C++ binary that gets distributed for specific platforms (linux_amd64, osx_arm64, etc.). You can't just load Python code directly (FML) it has to go through the proper extension framework with signed binaries.

**My Better Idea: ADBC-Based Pure C++ Implementation**

#### 🎯 What I Should Have Done From The Start
- **Kill the Python Stuff**: Remove `venv/`, Python CLI, and all that Python runtime baggage
- **Go Native with ADBC**: Use Apache Arrow Database Connectivity (ADBC) to talk directly to Snowflake from C++ (no lazy shortcut of just using it from the venv)
- **One Extension to Rule Them All**: Single C++ extension that handles both data fetching and caching
- **Actually Work with DuckDB UI**: Full integration with DuckDB's native web interface and CLI (the way it's supposed to work, vs right now it's a mix of 2 code bases I merged)
Based on DuckLake's capabilities and community feedback opportunities, here are potential enhancements for future versions:

### 🗄️ Additional Storage Backends
- **Google Cloud Storage**: Native GCS support for cache storage
- **Azure Blob Storage**: Azure integration for enterprise deployments
- **MinIO/S3-Compatible**: Support for on-premises S3-compatible storage

### 🔧 Configuration Enhancements
- **Dynamic Configuration**: Runtime configuration changes
- **Configuration Validation**: Validate config at startup
- **Configuration Templates**: Pre-built configs for common scenarios
- **Environment-Specific Configs**: Dev/staging/prod configurations

### 🌐 Integration Features
- **dbt Integration**: Work with dbt models and transformations (OPEN SOURCE FUSION!) 
- **Airflow Integration**: Apache Airflow operators
- **Kubernetes**: Native K8s deployment support

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.


## Acknowledgments

- **DuckDB Team**: For the excellent database engine and extension framework
- **DuckLake Contributors**: For the metadata management system
- **Snowflake**: For the powerful cloud data platform
- **Open Source Community**: For the amazing tools and libraries that make this possible

---

**Made with ❤️ by the SnowDucks community (me)**
