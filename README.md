# SnowDucks 🦆

Intelligent query caching for Snowflake using DuckDB (DuckLake) as a local cache.

## Overview

SnowDucks provides intelligent query caching for Snowflake, storing query results locally in DuckDB for faster subsequent executions. It includes both a Python CLI and a C++ DuckDB extension.

## Features

- **Intelligent Caching**: Automatically caches Snowflake query results in local DuckDB
- **LIMIT-Aware Hashing**: Different cache entries for different LIMIT values
- **Python CLI**: Interactive command-line interface with Snowflake connectivity
- **C++ Extension**: Native DuckDB extension with utility functions
- **Consistent Hashing**: Same cache keys between Python CLI and C++ extension
- **Universal Configuration**: Single config works for both local and S3 deployments
- **Docker Support**: Cross-platform deployment with reproducible builds

## Project Structure

This project follows the DuckDB extension template structure with additional Python CLI components:

```
snowducks/
├── src/                    # Source code
│   ├── snowducks_extension.cpp  # C++ extension source
│   ├── include/           # C++ headers
│   └── cli/               # Python CLI implementation
│       ├── snowducks/     # Python package
│       ├── pyproject.toml # Python dependencies
│       └── requirements-dev.txt
├── test/                  # Tests
│   ├── sql/              # C++ extension SQL tests
│   └── python/           # Python CLI tests
├── utils/                 # Utility scripts
├── config/                # Configuration files
├── examples/              # Usage examples
├── docker/                # Docker configuration
├── Makefile               # Build and run commands
├── env.example           # Environment template
└── README.md             # This file
```

## Quick Start

### 1. Clone and Setup

```bash
# Clone the repository
git clone <repository-url>
cd snowducks

# Complete setup (venv + install + build + config)
make init
```

### 2. Configure Snowflake

```bash
# Copy and configure environment
cp env.example .env
# Edit .env with your Snowflake credentials
```

### 3. Use SnowDucks

```bash
# Start Python CLI
make cli

# Or start DuckDB with extension
make duckdb
```

## Configuration

### Environment Variables

Create a `.env` file with your Snowflake credentials:

```bash
# Required Snowflake settings
SNOWFLAKE_USER=your_username
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_ROLE=your_role

# Authentication (choose one method)
SNOWFLAKE_PASSWORD=your_password
# OR
# SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/private_key.p8
# SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=your_passphrase

# Cache settings
CACHE_MAX_AGE_HOURS=24
CACHE_FORCE_REFRESH=false
DEFAULT_ROW_LIMIT=1000

# Cost control
ALLOW_UNLIMITED_EGRESS=false
```

## Usage

### Python CLI

The CLI provides an interactive DuckDB session with Snowflake connectivity via the `snowflake_query()` function.

```bash
# Start the CLI
make cli

# In the CLI, run:
SELECT * FROM snowflake_query('SELECT * FROM your_table LIMIT 10', 1000, false);
SELECT * FROM snowflake_query('STATS', 0, false);
SELECT * FROM snowflake_query('SELECT * FROM your_table LIMIT 10', 1000, true);
```

### C++ Extension

The C++ extension provides utility functions and can be loaded into any DuckDB instance.

```bash
# Start DuckDB with extension
make duckdb

# In DuckDB, run:
LOAD 'snowducks.duckdb_extension';

-- Utility functions
SELECT snowducks_info('Hello from C++!');
SELECT snowducks_generate_cache_table_name('SELECT * FROM test LIMIT 10');
SELECT snowducks_normalize_query_text('SELECT * FROM test LIMIT 10');
SELECT snowducks_cache_stats();
```

## Building

### Prerequisites

- Python 3.9+
- C++17 compiler (for C++ extension)
- CMake 3.16+
- macOS or Linux

### Build Commands

```bash
# Complete setup
make init

# Individual steps
make venv      # Create Python virtual environment
make install   # Install DuckDB CLI and dependencies
make build     # Build C++ extension
make config    # Validate configuration

# Development
make test      # Run all tests
make test-cli  # Run Python tests only
make test-ext  # Run C++ tests only
make clean     # Clean build artifacts
```

## Architecture

### Components

1. **Python CLI** (`src/cli/`)
   - Interactive DuckDB session
   - Snowflake connectivity via ADBC
   - Query caching logic
   - `snowflake_query()` UDF

2. **C++ Extension** (`src/`)
   - Utility functions for query normalization
   - Cache table name generation
   - Consistent hashing with Python CLI
   - `snowflake_query()` function with ADBC connectivity

3. **Shared Utilities**
   - Query normalization logic
   - Hash generation for cache keys
   - Cache table naming conventions

### Cache Strategy

- **Cache Key**: Normalized query hash (includes LIMIT clause)
- **Storage**: DuckDB tables with hash-based naming
- **TTL**: No automatic expiration (manual cleanup required)
- **Consistency**: Same hashing between Python CLI and C++ extension

## Development

### Local Development

```bash
# Set up development environment
make init

# Run tests
make test

# Start development CLI
make cli
```

### Extension Development

The C++ extension is built using the DuckDB extension template. Key files:

- `src/snowducks_extension.cpp` - Main extension implementation
- `src/include/snowducks_extension.hpp` - Extension header
- `test/sql/` - SQL tests for the extension

### Adding New Functions

To add new functions to the C++ extension:

1. Add function implementation in `src/snowducks_extension.cpp`
2. Register the function in the `LoadInternal` function
3. Add tests in `test/sql/`

## Troubleshooting

### Common Issues

1. **"Snowflake connection failed"**
   - Check environment variables in `.env`
   - Verify Snowflake credentials
   - Ensure network connectivity

2. **"Extension not loaded"**
   - Ensure extension is built: `make build`
   - Check extension binary exists: `make info`

3. **"Virtual environment not found"**
   - Create virtual environment: `make venv`
   - Or run complete setup: `make init`

### Environment Variables

Required for Snowflake connectivity:
- `SNOWFLAKE_USER`: Snowflake username
- `SNOWFLAKE_PASSWORD`: Snowflake password
- `SNOWFLAKE_ACCOUNT`: Snowflake account identifier
- `SNOWFLAKE_WAREHOUSE`: Snowflake warehouse name
- `SNOWFLAKE_DATABASE`: Snowflake database name
- `SNOWFLAKE_SCHEMA`: Snowflake schema name

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

[Add your license information here]
