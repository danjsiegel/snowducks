# SnowDucks Tests

This directory contains comprehensive tests for the SnowDucks project, covering both Python and C++ components.

## DuckDB Version (Reproducibility)

**The DuckDB engine is vendored as a git submodule and pinned to version v1.3.1.**

- All C++ extension tests are always built and run against this specific version for full reproducibility.
- No system DuckDB install is required or used for C++ tests.

**To initialize or update the DuckDB submodule:**
```sh
git submodule update --init --recursive
cd duckdb && git checkout v1.3.1
```

If you ever need to re-pin or update the version, just change the checkout tag/commit in the `duckdb` directory and commit the change.

---

## Test Structure

### Python Tests (`python/`)
- **`test_cli.py`** - CLI functionality tests (fully mocked)
- **`test_core.py`** - Core module tests (fully mocked)
- **`test_config.py`** - Configuration tests (fully mocked)
- **`test_utils.py`** - Utility function tests
- **`test_ducklake_manager.py`** - DuckLake manager tests

### C++ Tests (`cpp/`)
- **`test_snowducks_extension.cpp`** - DuckDB extension tests (fully mocked)
- **`CMakeLists.txt`** - Build configuration for C++ tests (uses vendored DuckDB)

### SQL Tests (`sql/`)
- **`snowducks.test`** - SQL-level integration tests

## Running Tests

### Quick Start
```bash
# Run all tests
make test
```

### Individual Test Suites

#### Python Tests
```bash
make test-cli
```

#### C++ Tests
```bash
make test-ext
```

#### SQL Tests
```bash
make test-sql
```

## Test Philosophy

All tests are designed to be **fully mocked** and **self-contained**:
- No external dependencies (Snowflake, PostgreSQL)
- No network connections required
- Fast execution
- Reliable CI/CD integration

## Test Coverage

### Python Components
- ✅ CLI argument parsing
- ✅ Configuration loading
- ✅ Query normalization
- ✅ Cache table name generation
- ✅ Error handling
- ✅ JSON output formatting

### C++ Components
- ✅ Extension loading
- ✅ Scalar functions
- ✅ Table functions
- ✅ Environment variable handling
- ✅ Error handling
- ✅ Query processing

### Integration
- ✅ Python-C++ consistency
- ✅ SQL interface
- ✅ End-to-end workflows

## Adding New Tests

### Python Tests
1. Add test functions to appropriate test files
2. Use `@patch` decorators for mocking
3. Follow existing patterns for stdout/stderr capture
4. Use descriptive test names and docstrings

### C++ Tests
1. Add test cases to `test_snowducks_extension.cpp`
2. Use Catch2 framework
3. Mock external dependencies
4. Test both success and failure scenarios

## Continuous Integration

Tests are designed to run in CI/CD environments:
- No external service dependencies
- Fast execution (< 30 seconds total)
- Clear pass/fail indicators
- Comprehensive error reporting