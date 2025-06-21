#!/bin/bash
# Test SnowDucks extension functionality

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
EXTENSION_PATH="$PROJECT_ROOT/ui/extension-template/build/release/extension/snowducks/snowducks.duckdb_extension"

echo "🦆 Testing SnowDucks C++ Extension..."
echo ""

# Test the extension
duckdb test_ui.db << SQL
LOAD '$EXTENSION_PATH';

-- Test basic functionality
SELECT snowducks_info('Extension Test') as info;

-- Test LIMIT-aware hashing
SELECT 'LIMIT 5' as test_case, 
       snowducks_generate_cache_table_name('SELECT * FROM test LIMIT 5') as hash;

SELECT 'LIMIT 10' as test_case, 
       snowducks_generate_cache_table_name('SELECT * FROM test LIMIT 10') as hash;

SELECT 'LIMIT 20' as test_case, 
       snowducks_generate_cache_table_name('SELECT * FROM test LIMIT 20') as hash;

-- Test whitespace normalization
SELECT 'Normalized' as test_case,
       snowducks_generate_cache_table_name('SELECT * FROM test LIMIT 10') as hash1,
       snowducks_generate_cache_table_name('  SELECT   *   FROM   test   LIMIT   10  ') as hash2;

-- Test cache validation
SELECT 'Validation' as test_case,
       snowducks_is_valid_cache_table('t_4ff4d557133edebc') as is_valid;

-- Show cache stats
SELECT * FROM snowducks_cache_stats();
SQL

echo ""
echo "✅ Extension test complete!"
