#!/bin/bash
# Test UI setup without starting the web interface

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUCKDB_PATH="$SCRIPT_DIR/ui/extension-template/build/release/duckdb"

echo "🦆 Testing UI Setup (without starting web interface)..."
echo ""

# Check if the built DuckDB exists
if [ ! -f "$DUCKDB_PATH" ]; then
    echo "❌ Error: Built DuckDB not found at $DUCKDB_PATH"
    echo "Please run './setup.sh' first to build the extension"
    exit 1
fi

echo "✅ Built DuckDB found: $DUCKDB_PATH"
echo ""

# Test the setup process
echo "Testing UI extension installation and loading..."
"$DUCKDB_PATH" test_ui.db << 'SQL'
-- Test SnowDucks extension
SELECT snowducks_info('Testing UI Setup') as snowducks_status;

-- Test LIMIT-aware hashing
SELECT 'LIMIT 5' as test_case, 
       snowducks_generate_cache_table_name('SELECT * FROM test LIMIT 5') as hash;

SELECT 'LIMIT 10' as test_case, 
       snowducks_generate_cache_table_name('SELECT * FROM test LIMIT 10') as hash;

-- Install and load UI extension
INSTALL ui;
LOAD ui;

-- Test that UI extension is loaded
SELECT 'UI extension loaded successfully' as ui_status;

-- Test cache functions
SELECT * FROM snowducks_cache_stats();

-- Test table listing
SELECT * FROM snowducks_list_tables();
SQL

echo ""
echo "✅ UI setup test complete!"
echo ""
echo "To start the actual web UI, run:"
echo "  ./start-ui.sh"
echo ""
echo "The web UI should open in your browser and you can use all SnowDucks functions!" 