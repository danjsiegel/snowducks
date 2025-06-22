#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "catch.hpp"
#include <iostream>
#include <fstream>
#include <cstdlib>

using namespace duckdb;

// Helper function to create a temporary .env file
std::string create_temp_env_file() {
    std::string env_content = R"(
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=snowducks_metadata
POSTGRES_USER=snowducks_user
POSTGRES_PASSWORD=snowducks_password
POSTGRES_SCHEMA=snowducks
SNOWFLAKE_USER=test_user
SNOWFLAKE_PASSWORD=test_password
SNOWFLAKE_ACCOUNT=test_account
SNOWFLAKE_DATABASE=test_database
SNOWFLAKE_WAREHOUSE=test_warehouse
SNOWFLAKE_ROLE=test_role
DUCKLAKE_METADATA_PATH=/tmp/test_metadata.ducklake
DUCKLAKE_DATA_PATH=/tmp/test_data
)";
    
    std::string env_file = "/tmp/test_snowducks.env";
    std::ofstream file(env_file);
    file << env_content;
    file.close();
    return env_file;
}

// Helper function to set environment variables
void set_test_env_vars() {
    setenv("POSTGRES_HOST", "localhost", 1);
    setenv("POSTGRES_PORT", "5432", 1);
    setenv("POSTGRES_DATABASE", "snowducks_metadata", 1);
    setenv("POSTGRES_USER", "snowducks_user", 1);
    setenv("POSTGRES_PASSWORD", "snowducks_password", 1);
    setenv("POSTGRES_SCHEMA", "snowducks", 1);
    setenv("SNOWFLAKE_USER", "test_user", 1);
    setenv("SNOWFLAKE_PASSWORD", "test_password", 1);
    setenv("SNOWFLAKE_ACCOUNT", "test_account", 1);
    setenv("SNOWFLAKE_DATABASE", "test_database", 1);
    setenv("SNOWFLAKE_WAREHOUSE", "test_warehouse", 1);
    setenv("SNOWFLAKE_ROLE", "test_role", 1);
    setenv("DUCKLAKE_METADATA_PATH", "/tmp/test_metadata.ducklake", 1);
    setenv("DUCKLAKE_DATA_PATH", "/tmp/test_data", 1);
    setenv("HOME", "/tmp", 1);
}

TEST_CASE("SnowDucks Extension Loading", "[snowducks]") {
    DuckDB db(nullptr);
    Connection con(db);
    
    SECTION("Extension loads successfully") {
        REQUIRE_NOTHROW(con.Query("LOAD snowducks"));
        
        // Check that the extension functions are available
        auto result = con.Query("SELECT function_name FROM duckdb_functions() WHERE function_name LIKE 'snowducks_%'");
        REQUIRE(!result->HasError());
        REQUIRE(result->RowCount() > 0);
    }
}

TEST_CASE("SnowDucks Scalar Functions", "[snowducks]") {
    DuckDB db(nullptr);
    Connection con(db);
    con.Query("LOAD snowducks");
    
    SECTION("snowducks_normalize_query") {
        auto result = con.Query("SELECT snowducks_normalize_query('SELECT * FROM users LIMIT 1000')");
        REQUIRE(!result->HasError());
        REQUIRE(result->RowCount() == 1);
        REQUIRE(result->GetValue(0, 0).ToString() == "select * from users limit 1000");
    }
    
    SECTION("snowducks_cache_table_name") {
        auto result = con.Query("SELECT snowducks_cache_table_name('SELECT * FROM users LIMIT 1000')");
        REQUIRE(!result->HasError());
        REQUIRE(result->RowCount() == 1);
        std::string table_name = result->GetValue(0, 0).ToString();
        REQUIRE(table_name.substr(0, 2) == "t_");
        REQUIRE(table_name.length() == 18); // t_ + 16 hex chars
    }
    
    SECTION("snowducks_info") {
        auto result = con.Query("SELECT snowducks_info('test')");
        REQUIRE(!result->HasError());
        REQUIRE(result->RowCount() == 1);
        REQUIRE(result->GetValue(0, 0).ToString() == "Snowducks test 🦆");
    }
    
    SECTION("Consistent table name generation") {
        auto result1 = con.Query("SELECT snowducks_cache_table_name('SELECT * FROM users LIMIT 1000')");
        auto result2 = con.Query("SELECT snowducks_cache_table_name('SELECT * FROM users LIMIT 1000')");
        REQUIRE(!result1->HasError());
        REQUIRE(!result2->HasError());
        REQUIRE(result1->GetValue(0, 0).ToString() == result2->GetValue(0, 0).ToString());
    }
    
    SECTION("Different queries generate different table names") {
        auto result1 = con.Query("SELECT snowducks_cache_table_name('SELECT * FROM users LIMIT 1000')");
        auto result2 = con.Query("SELECT snowducks_cache_table_name('SELECT * FROM orders LIMIT 1000')");
        REQUIRE(!result1->HasError());
        REQUIRE(!result2->HasError());
        REQUIRE(result1->GetValue(0, 0).ToString() != result2->GetValue(0, 0).ToString());
    }
}

TEST_CASE("SnowDucks Table Function - Environment Variables", "[snowducks]") {
    DuckDB db(nullptr);
    Connection con(db);
    con.Query("LOAD snowducks");
    
    SECTION("Missing environment variables") {
        // Clear environment variables
        unsetenv("POSTGRES_HOST");
        unsetenv("POSTGRES_PORT");
        unsetenv("POSTGRES_DATABASE");
        unsetenv("POSTGRES_USER");
        unsetenv("POSTGRES_PASSWORD");
        
        auto result = con.Query("SELECT * FROM snowducks_table('SELECT 1 as test')");
        REQUIRE(result->HasError());
        REQUIRE(result->GetError().find("Missing PostgreSQL environment variables") != std::string::npos);
    }
    
    SECTION("With environment variables") {
        set_test_env_vars();
        
        // This should fail gracefully with a connection error rather than missing env vars
        auto result = con.Query("SELECT * FROM snowducks_table('SELECT 1 as test')");
        // Should either succeed or fail with a connection error, but not missing env vars
        if (result->HasError()) {
            REQUIRE(result->GetError().find("Missing PostgreSQL environment variables") == std::string::npos);
        }
    }
}

TEST_CASE("SnowDucks Table Function - Basic Functionality", "[snowducks]") {
    DuckDB db(nullptr);
    Connection con(db);
    con.Query("LOAD snowducks");
    set_test_env_vars();
    
    SECTION("Table function exists") {
        auto result = con.Query("SELECT function_name FROM duckdb_functions() WHERE function_name = 'snowducks_table'");
        REQUIRE(!result->HasError());
        REQUIRE(result->RowCount() == 1);
        REQUIRE(result->GetValue(0, 0).ToString() == "snowducks_table");
    }
    
    SECTION("Table function parameters") {
        // Test with debug parameter
        auto result = con.Query("SELECT * FROM snowducks_table('SELECT 1 as test', debug=true)");
        // Should either succeed or fail gracefully, but not crash
        if (result->HasError()) {
            REQUIRE(result->GetError().find("Missing PostgreSQL environment variables") == std::string::npos);
        }
    }
    
    SECTION("Table function with limit parameter") {
        auto result = con.Query("SELECT * FROM snowducks_table('SELECT 1 as test', limit=10)");
        // Should either succeed or fail gracefully, but not crash
        if (result->HasError()) {
            REQUIRE(result->GetError().find("Missing PostgreSQL environment variables") == std::string::npos);
        }
    }
    
    SECTION("Table function with force_refresh parameter") {
        auto result = con.Query("SELECT * FROM snowducks_table('SELECT 1 as test', force_refresh=false)");
        // Should either succeed or fail gracefully, but not crash
        if (result->HasError()) {
            REQUIRE(result->GetError().find("Missing PostgreSQL environment variables") == std::string::npos);
        }
    }
}

TEST_CASE("SnowDucks Table Function - Error Handling", "[snowducks]") {
    DuckDB db(nullptr);
    Connection con(db);
    con.Query("LOAD snowducks");
    set_test_env_vars();
    
    SECTION("Invalid query") {
        auto result = con.Query("SELECT * FROM snowducks_table('INVALID SQL QUERY')");
        // Should fail gracefully with an error message
        REQUIRE(result->HasError());
        REQUIRE(result->GetError().length() > 0);
    }
    
    SECTION("Empty query") {
        auto result = con.Query("SELECT * FROM snowducks_table('')");
        // Should fail gracefully
        REQUIRE(result->HasError());
    }
    
    SECTION("Query with multiple statements") {
        auto result = con.Query("SELECT * FROM snowducks_table('SELECT 1; SELECT 2')");
        // Should fail gracefully
        REQUIRE(result->HasError());
    }
}

TEST_CASE("SnowDucks Extension - Integration Tests", "[snowducks]") {
    DuckDB db(nullptr);
    Connection con(db);
    con.Query("LOAD snowducks");
    set_test_env_vars();
    
    SECTION("End-to-end workflow") {
        // Test the complete workflow: normalize query -> generate table name -> use table function
        auto normalize_result = con.Query("SELECT snowducks_normalize_query('SELECT 1 as test_column')");
        REQUIRE(!normalize_result->HasError());
        
        auto table_name_result = con.Query("SELECT snowducks_cache_table_name('SELECT 1 as test_column')");
        REQUIRE(!table_name_result->HasError());
        std::string table_name = table_name_result->GetValue(0, 0).ToString();
        
        // The table function should work with the same query
        auto table_result = con.Query("SELECT * FROM snowducks_table('SELECT 1 as test_column')");
        // Should either succeed or fail gracefully, but not crash
        if (table_result->HasError()) {
            REQUIRE(table_result->GetError().find("Missing PostgreSQL environment variables") == std::string::npos);
        }
    }
    
    SECTION("Query normalization consistency") {
        // Test that different whitespace patterns normalize to the same result
        auto result1 = con.Query("SELECT snowducks_normalize_query('SELECT * FROM users LIMIT 1000')");
        auto result2 = con.Query("SELECT snowducks_normalize_query('  SELECT   *   FROM   users   LIMIT   1000  ')");
        REQUIRE(!result1->HasError());
        REQUIRE(!result2->HasError());
        REQUIRE(result1->GetValue(0, 0).ToString() == result2->GetValue(0, 0).ToString());
    }
    
    SECTION("Table name generation consistency") {
        // Test that normalized queries generate the same table names
        auto result1 = con.Query("SELECT snowducks_cache_table_name('SELECT * FROM users LIMIT 1000')");
        auto result2 = con.Query("SELECT snowducks_cache_table_name('  SELECT   *   FROM   users   LIMIT   1000  ')");
        REQUIRE(!result1->HasError());
        REQUIRE(!result2->HasError());
        REQUIRE(result1->GetValue(0, 0).ToString() == result2->GetValue(0, 0).ToString());
    }
}

TEST_CASE("SnowDucks Extension - Performance Tests", "[snowducks]") {
    DuckDB db(nullptr);
    Connection con(db);
    con.Query("LOAD snowducks");
    set_test_env_vars();
    
    SECTION("Multiple function calls") {
        // Test that multiple calls to the same function work correctly
        for (int i = 0; i < 10; i++) {
            auto result = con.Query("SELECT snowducks_normalize_query('SELECT * FROM users LIMIT 1000')");
            REQUIRE(!result->HasError());
            REQUIRE(result->GetValue(0, 0).ToString() == "select * from users limit 1000");
        }
    }
    
    SECTION("Large query normalization") {
        std::string large_query = "SELECT " + std::string(1000, 'a') + " FROM users LIMIT 1000";
        auto result = con.Query("SELECT snowducks_normalize_query('" + large_query + "')");
        REQUIRE(!result->HasError());
        std::string normalized = result->GetValue(0, 0).ToString();
        REQUIRE(normalized.find("select") == 0);
        REQUIRE(normalized.find("limit 1000") != std::string::npos);
    }
}

TEST_CASE("SnowDucks Extension - Edge Cases", "[snowducks]") {
    DuckDB db(nullptr);
    Connection con(db);
    con.Query("LOAD snowducks");
    set_test_env_vars();
    
    SECTION("Special characters in queries") {
        auto result = con.Query("SELECT snowducks_normalize_query('SELECT * FROM \"users\" WHERE name = ''John; Smith''')");
        REQUIRE(!result->HasError());
        // Should handle quotes and semicolons in strings correctly
        std::string normalized = result->GetValue(0, 0).ToString();
        REQUIRE(normalized.find("john; smith") != std::string::npos);
    }
    
    SECTION("Unicode characters") {
        auto result = con.Query("SELECT snowducks_normalize_query('SELECT * FROM users WHERE name = ''José''')");
        REQUIRE(!result->HasError());
        // Should handle Unicode characters correctly
        std::string normalized = result->GetValue(0, 0).ToString();
        REQUIRE(normalized.find("josé") != std::string::npos);
    }
    
    SECTION("Very long table names") {
        std::string long_table_name = "very_long_table_name_" + std::string(100, 'a');
        auto result = con.Query("SELECT snowducks_cache_table_name('SELECT * FROM " + long_table_name + "')");
        REQUIRE(!result->HasError());
        std::string table_name = result->GetValue(0, 0).ToString();
        REQUIRE(table_name.substr(0, 2) == "t_");
        REQUIRE(table_name.length() == 18);
    }
} 