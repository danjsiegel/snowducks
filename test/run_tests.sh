#!/bin/bash

# Test runner script for SnowDucks
# This script runs both Python and C++ tests, including code quality checks

set -e  # Exit on any error

echo "🧪 Running SnowDucks Tests"
echo "=========================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

# Check if we're in the right directory
if [ ! -f "CMakeLists.txt" ]; then
    print_error "Please run this script from the project root directory"
    exit 1
fi

# Run Python tests (including code quality tests)
echo ""
echo "🐍 Running Python Tests..."
echo "-------------------------"

if command -v python3 &> /dev/null; then
    cd test/python
    if python3 -m pytest -v --tb=short; then
        print_status "Python tests passed"
    else
        print_error "Python tests failed"
        exit 1
    fi
    cd ../..
else
    print_warning "Python3 not found, skipping Python tests"
fi

# Run C++ tests
echo ""
echo "🔧 Running C++ Tests..."
echo "----------------------"

if command -v cmake &> /dev/null && command -v make &> /dev/null; then
    # Build the extension first
    echo "Building extension..."
    if make clean && make; then
        print_status "Extension built successfully"
    else
        print_error "Failed to build extension"
        exit 1
    fi
    
    # Run C++ tests
    cd test/cpp
    if cmake . && make && ./snowducks_tests; then
        print_status "C++ tests passed"
    else
        print_error "C++ tests failed"
        exit 1
    fi
    cd ../..
else
    print_warning "CMake or Make not found, skipping C++ tests"
fi

# Run SQL tests
echo ""
echo "🗄️  Running SQL Tests..."
echo "------------------------"

if command -v duckdb &> /dev/null; then
    cd test/sql
    if duckdb --batch < snowducks.test; then
        print_status "SQL tests passed"
    else
        print_error "SQL tests failed"
        exit 1
    fi
    cd ../..
else
    print_warning "DuckDB CLI not found, skipping SQL tests"
fi

echo ""
echo "🎉 All tests completed successfully!"
echo "===================================" 