-- SnowDucks PostgreSQL initialization script
-- This script runs when the PostgreSQL container starts for the first time

-- Create the snowducks schema
CREATE SCHEMA IF NOT EXISTS snowducks;

-- Grant permissions to the snowducks user
GRANT ALL PRIVILEGES ON SCHEMA snowducks TO snowducks_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA snowducks TO snowducks_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA snowducks TO snowducks_user;

-- Set default privileges for future tables and sequences
ALTER DEFAULT PRIVILEGES IN SCHEMA snowducks GRANT ALL ON TABLES TO snowducks_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA snowducks GRANT ALL ON SEQUENCES TO snowducks_user;

-- Create a function to check if DuckLake tables exist
CREATE OR REPLACE FUNCTION snowducks.check_ducklake_tables()
RETURNS TABLE(table_name text, exists boolean) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        t.table_name::text,
        CASE WHEN t.table_name IS NOT NULL THEN true ELSE false END
    FROM information_schema.tables t
    WHERE t.table_schema = 'snowducks' 
    AND t.table_name IN (
        'ducklake_data_file',
        'ducklake_snapshot', 
        'ducklake_snapshot_changes',
        'ducklake_table_stats',
        'ducklake_table_column_stats',
        'ducklake_file_column_statistics'
    );
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Grant execute permission on the function
GRANT EXECUTE ON FUNCTION snowducks.check_ducklake_tables() TO snowducks_user;

-- Log initialization
INSERT INTO pg_stat_statements_info (dealloc) VALUES (0) ON CONFLICT DO NOTHING; 