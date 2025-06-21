#!/bin/bash
# Manual UI start script with step-by-step instructions

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUCKDB_PATH="$SCRIPT_DIR/ui/extension-template/build/release/duckdb"

echo "🦆 Manual DuckDB UI Start with SnowDucks Extension"
echo "=================================================="
echo ""

# Check if the built DuckDB exists
if [ ! -f "$DUCKDB_PATH" ]; then
    echo "❌ Error: Built DuckDB not found at $DUCKDB_PATH"
    echo "Please run './setup.sh' first to build the extension"
    exit 1
fi

echo "✅ Built DuckDB found: $DUCKDB_PATH"
echo ""
echo "📋 Manual Steps to Start UI:"
echo "1. Start DuckDB: $DUCKDB_PATH test_ui.db"
echo "2. In DuckDB, run these commands:"
echo ""
echo "   SET autoinstall_known_extensions=1;"
echo "   SET autoload_known_extensions=1;"
echo "   INSTALL ui;"
echo "   LOAD ui;"
echo "   SELECT snowducks_info('Ready for UI!');"
echo "   CALL start_ui();"
echo ""
echo "3. The web UI will open at http://localhost:4213/"
echo "4. Press Ctrl+C in the terminal to stop the server"
echo ""
echo "🚀 Quick Start (automated):"
echo "   ./start-ui-background.sh"
echo ""
echo "🧪 Test Extension (without UI):"
echo "   ./test-extension.sh"
echo ""
echo "Press Enter to start DuckDB manually, or Ctrl+C to cancel..."
read

echo "Starting DuckDB..."
echo "Copy and paste the commands above when DuckDB starts."
echo ""

"$DUCKDB_PATH" test_ui.db 