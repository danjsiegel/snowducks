#!/bin/bash
# Start DuckDB UI in background with SnowDucks extension loaded

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUCKDB_PATH="$SCRIPT_DIR/ui/extension-template/build/release/duckdb"

echo "🦆 Starting DuckDB UI in background with SnowDucks extension..."
echo ""

# Check if the built DuckDB exists
if [ ! -f "$DUCKDB_PATH" ]; then
    echo "❌ Error: Built DuckDB not found at $DUCKDB_PATH"
    echo "Please run './setup.sh' first to build the extension"
    exit 1
fi

# Create a temporary SQL file for setup
TEMP_SQL=$(mktemp)
cat > "$TEMP_SQL" << 'SQL'
-- Enable auto-install and auto-load for extensions
SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;

-- Install and load the UI extension
INSTALL ui;
LOAD ui;

-- Show that SnowDucks extension is working
SELECT snowducks_info('Background UI Setup Complete!') as status;

-- Start the UI
CALL start_ui();
SQL

echo "✅ Starting DuckDB UI in background..."
echo "The web UI should open in your browser at http://localhost:4213/"
echo ""
echo "Available SnowDucks functions in the UI:"
echo "  - snowducks_info('message')"
echo "  - snowducks_generate_cache_table_name('query')"
echo "  - snowducks_normalize_query_text('query')"
echo "  - snowducks_is_valid_cache_table('table_name')"
echo "  - snowducks_cache_stats()"
echo "  - snowducks_list_tables()"
echo ""
echo "Example queries to try in the UI:"
echo "  SELECT snowducks_info('Hello from UI!');"
echo "  SELECT snowducks_generate_cache_table_name('SELECT * FROM test LIMIT 10');"
echo ""
echo "To stop the UI server, run: pkill -f 'duckdb.*start_ui'"
echo ""

# Start DuckDB in background
"$DUCKDB_PATH" test_ui.db < "$TEMP_SQL" &

# Clean up temp file
rm "$TEMP_SQL"

echo "🦆 DuckDB UI is running in the background!"
echo "Open http://localhost:4213/ in your browser"
echo ""
echo "Press Enter to stop the server..."
read

# Stop the background process
echo "Stopping DuckDB UI server..."
pkill -f "duckdb.*start_ui" || true
echo "✅ Server stopped" 