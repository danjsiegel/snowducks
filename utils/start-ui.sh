#!/bin/bash
# Start DuckDB UI with SnowDucks extension loaded

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUCKDB_PATH="$SCRIPT_DIR/ui/extension-template/build/release/duckdb"

echo "🦆 Starting DuckDB UI with SnowDucks extension..."
echo "Using built DuckDB with extension pre-loaded"
echo ""

# Check if the built DuckDB exists
if [ ! -f "$DUCKDB_PATH" ]; then
    echo "❌ Error: Built DuckDB not found at $DUCKDB_PATH"
    echo "Please run './setup.sh' first to build the extension"
    exit 1
fi

echo "Starting DuckDB UI..."
echo "The SnowDucks extension is pre-loaded and ready to use!"
echo ""
echo "Available SnowDucks functions:"
echo "  - snowducks_info('message')"
echo "  - snowducks_generate_cache_table_name('query')"
echo "  - snowducks_normalize_query_text('query')"
echo "  - snowducks_is_valid_cache_table('table_name')"
echo "  - snowducks_cache_stats()"
echo "  - snowducks_list_tables()"
echo ""
echo "Example queries:"
echo "  SELECT snowducks_info('Hello from UI!');"
echo "  SELECT snowducks_generate_cache_table_name('SELECT * FROM test LIMIT 10');"
echo ""
echo "The web UI should open in your browser at http://localhost:4213/"
echo "Press Ctrl+C to stop the server when done."
echo ""

# Start DuckDB in interactive mode and run the setup commands
"$DUCKDB_PATH" test_ui.db << 'SQL'
-- Enable auto-install and auto-load for extensions
SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;

-- Install and load the UI extension
INSTALL ui;
LOAD ui;

-- Show that SnowDucks extension is working
SELECT snowducks_info('UI Setup Complete!') as status;

-- Start the UI (this will keep the process running)
CALL start_ui();
SQL
