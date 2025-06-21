PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=snowducks
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# SnowDucks Makefile
# Comprehensive build and setup for SnowDucks DuckDB extension and Python CLI

.PHONY: help
help:
	@echo "🦆 SnowDucks - Makefile Commands"
	@echo ""
	@echo "Setup:"
	@echo "  make init      - Complete setup (venv + install + build + config)"
	@echo "  make install   - Install DuckDB CLI and Python dependencies"
	@echo "  make build     - Build the SnowDucks extension (C++)"
	@echo "  make config    - Validate and initialize configuration"
	@echo ""
	@echo "PostgreSQL (Docker):"
	@echo "  make postgres-setup - Setup PostgreSQL with Docker (recommended)"
	@echo "  make postgres-start  - Start PostgreSQL container"
	@echo "  make postgres-stop   - Stop PostgreSQL container"
	@echo "  make postgres-status - Check PostgreSQL status"
	@echo "  make postgres-reset  - Reset PostgreSQL data (WARNING: destroys data)"
	@echo "  make postgres-logs   - Show PostgreSQL logs"
	@echo "  make postgres-test   - Test PostgreSQL setup"
	@echo ""
	@echo "Development:"
	@echo "  make test      - Run all tests (CLI and extension)"
	@echo "  make test-cli  - Run Python CLI tests only"
	@echo "  make test-ext  - Run C++ extension tests only"
	@echo "  make clean     - Clean all build artifacts"
	@echo ""
	@echo "Usage:"
	@echo "  make cli       - Start Python CLI"
	@echo "  make duckdb    - Start DuckDB with extension loaded"
	@echo ""
	@echo "Environment:"
	@echo "  Set SNOWFLAKE_* variables in .env for Snowflake connectivity"

# Complete setup - create venv, install, build, and configure
.PHONY: init
init: venv install build config
	@echo "🎉 SnowDucks setup complete!"
	@echo "Run 'make cli' to start the Python CLI or 'make duckdb' to start DuckDB with extension"

# Create and setup virtual environment
.PHONY: venv
venv:
	@echo "🐍 Creating virtual environment..."
	@if [ ! -d "venv" ]; then \
		python3 -m venv venv; \
		echo "✅ Virtual environment created"; \
	else \
		echo "✅ Virtual environment already exists"; \
	fi
	@echo "📦 Installing Python dependencies..."
	@venv/bin/pip install --upgrade pip
	@venv/bin/pip install -e src/cli/
	@venv/bin/pip install -r src/cli/requirements-dev.txt
	@echo "✅ Virtual environment setup complete"

# Install DuckDB CLI and Python dependencies
.PHONY: install
install:
	@echo "📦 Installing DuckDB CLI..."
	@if command -v brew >/dev/null 2>&1; then \
		brew install duckdb; \
	else \
		echo "⚠️  Homebrew not found. Please install DuckDB manually:"; \
		echo "   https://duckdb.org/docs/installation/"; \
	fi
	@echo "✅ DuckDB CLI installed"
	@if [ -d "venv" ]; then \
		echo "🔗 Symlinking DuckDB CLI to virtual environment..."; \
		ln -sf $$(which duckdb) venv/bin/duckdb; \
		echo "✅ DuckDB CLI symlinked to venv"; \
	fi

# Build the SnowDucks extension
.PHONY: build
build:
	@echo "🔨 Building SnowDucks extension..."
	@make
	@echo "✅ Extension built successfully"

# Validate and initialize configuration
.PHONY: config
config:
	@echo "🔧 Validating configuration..."
	@if [ ! -f .env ]; then \
		echo "❌ .env file not found. Please create one with your Snowflake credentials."; \
		echo "   Copy env.example to .env and fill in your values."; \
		exit 1; \
	fi
	@echo "✅ .env file found"
	@if [ -d "venv" ]; then \
		venv/bin/python3 -c "from src.cli.snowducks.config import SnowDucksConfig; SnowDucksConfig.from_env()" > /dev/null 2>&1 || (echo "❌ Configuration validation failed. Check your .env file." && exit 1); \
		echo "✅ Configuration validated"; \
	else \
		echo "⚠️  Virtual environment not found. Run 'make venv' first."; \
	fi

# Start Python CLI
.PHONY: cli
cli:
	@echo "🦆 Starting SnowDucks Python CLI..."
	@if [ ! -d "venv" ]; then \
		echo "❌ Virtual environment not found. Run 'make init' first."; \
		exit 1; \
	fi
	@cd src/cli && ../../venv/bin/python -m snowducks.cli

# Start DuckDB with extension loaded
.PHONY: duckdb
duckdb:
	@echo "🦆 Starting DuckDB with SnowDucks extension..."
	@if [ ! -f "build/release/duckdb" ]; then \
		echo "❌ DuckDB not built. Run 'make build' first."; \
		exit 1; \
	fi
	@./build/release/duckdb

# Run all tests
.PHONY: test
test:
	$(MAKE) test-cli
	$(MAKE) test-ext

# Test Python CLI
.PHONY: test-cli
test-cli:
	@echo "🦆 Running Python CLI tests..."
	@if [ ! -d "venv" ]; then \
		echo "❌ Virtual environment not found. Run 'make venv' first."; \
		exit 1; \
	fi
	@cd test/python && ../../venv/bin/pytest . -v

# Test C++ extension
.PHONY: test-ext
test-ext:
	@echo "🦆 Running C++ extension tests..."
	@./build/release/test/unittest test/sql/*

# Clean all build artifacts
.PHONY: clean
clean:
	@echo "🧹 Cleaning build artifacts..."
	@rm -rf build/
	@rm -rf venv/
	@find . -name "*.pyc" -delete
	@find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	@echo "✅ Clean complete"

# Show extension info
.PHONY: info
info:
	@echo "🦆 SnowDucks Extension Information"
	@echo "Extension name: snowducks"
	@echo "Extension version: $(shell grep 'EXT_VERSION_SNOWDUCKS' src/snowducks_extension.cpp | sed 's/.*"\(.*\)".*/\1/' 2>/dev/null || echo 'dev')"
	@if [ -f "build/release/extension/snowducks/snowducks.duckdb_extension" ]; then \
		echo "Extension binary: ✅ Built"; \
	else \
		echo "Extension binary: ❌ Not built (run 'make build')"; \
	fi
	@if [ -d "venv" ]; then \
		echo "Python environment: ✅ Ready"; \
	else \
		echo "Python environment: ❌ Not ready (run 'make venv')"; \
	fi

# PostgreSQL Docker Management
.PHONY: postgres-setup
postgres-setup:
	@echo "🐘 Setting up PostgreSQL with Docker..."
	@python3 utils/setup_postgresql_docker.py

.PHONY: postgres-start
postgres-start:
	@echo "🐘 Starting PostgreSQL container..."
	@docker-compose up -d postgres
	@echo "⏳ Waiting for PostgreSQL to be ready..."
	@until docker-compose exec -T postgres pg_isready -U snowducks_user -d snowducks_metadata; do \
		echo "Waiting for PostgreSQL..."; \
		sleep 2; \
	done
	@echo "✅ PostgreSQL is ready!"
	@echo "📊 Database: snowducks_metadata"
	@echo "👤 User: snowducks_user"
	@echo "🔑 Password: snowducks_password"
	@echo "🌐 Port: 5432"
	@echo "📈 pgAdmin: http://localhost:8080 (admin@snowducks.local / admin)"

.PHONY: postgres-stop
postgres-stop:
	@echo "🐘 Stopping PostgreSQL container..."
	@docker-compose down
	@echo "✅ PostgreSQL stopped"

.PHONY: postgres-status
postgres-status:
	@echo "🐘 PostgreSQL container status:"
	@docker-compose ps postgres
	@echo ""
	@echo "📊 Database connection test:"
	@docker-compose exec -T postgres pg_isready -U snowducks_user -d snowducks_metadata || echo "❌ Database not ready"

.PHONY: postgres-reset
postgres-reset:
	@echo "⚠️  WARNING: This will destroy all PostgreSQL data!"
	@read -p "Are you sure? Type 'yes' to continue: " confirm; \
	if [ "$$confirm" = "yes" ]; then \
		echo "🗑️  Removing PostgreSQL containers and volumes..."; \
		docker-compose down -v; \
		echo "✅ PostgreSQL data reset complete"; \
	else \
		echo "❌ Reset cancelled"; \
	fi

.PHONY: postgres-logs
postgres-logs:
	@echo "📋 PostgreSQL container logs:"
	@docker-compose logs postgres

.PHONY: postgres-shell
postgres-shell:
	@echo "🐘 Opening PostgreSQL shell..."
	@docker-compose exec postgres psql -U snowducks_user -d snowducks_metadata

.PHONY: postgres-test
postgres-test:
	@echo "🧪 Testing PostgreSQL setup..."
	@python3 utils/test_postgresql_setup.py