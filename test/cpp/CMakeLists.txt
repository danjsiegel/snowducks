cmake_minimum_required(VERSION 3.10)
project(snowducks_tests)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Find DuckDB
find_package(DuckDB REQUIRED)

# Find Catch2 for testing
find_package(Catch2 3 REQUIRED)

# Include directories
include_directories(${CMAKE_SOURCE_DIR}/src/include)
include_directories(${CMAKE_SOURCE_DIR}/duckdb/src/include)

# Add DuckDB as a subdirectory (vendored)
add_subdirectory(${CMAKE_SOURCE_DIR}/../../duckdb duckdb_build)

# Create test executable
add_executable(snowducks_tests
    test_snowducks_extension.cpp
)

# Link libraries
target_link_libraries(snowducks_tests
    duckdb_static
    Catch2::Catch2
    snowducks_extension
)

# Enable testing
enable_testing()

# Add test
add_test(NAME SnowDucksTests COMMAND snowducks_tests)

# Set test properties
set_tests_properties(SnowDucksTests PROPERTIES
    ENVIRONMENT "POSTGRES_HOST=localhost;POSTGRES_PORT=5432;POSTGRES_DATABASE=snowducks_metadata;POSTGRES_USER=snowducks_user;POSTGRES_PASSWORD=snowducks_password;POSTGRES_SCHEMA=snowducks;SNOWFLAKE_USER=test_user;SNOWFLAKE_PASSWORD=test_password;SNOWFLAKE_ACCOUNT=test_account;SNOWFLAKE_DATABASE=test_database;SNOWFLAKE_WAREHOUSE=test_warehouse;SNOWFLAKE_ROLE=test_role;DUCKLAKE_METADATA_PATH=/tmp/test_metadata.ducklake;DUCKLAKE_DATA_PATH=/tmp/test_data;HOME=/tmp"
) 