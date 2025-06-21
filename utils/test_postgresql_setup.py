#!/usr/bin/env python3
"""
Test script to verify PostgreSQL setup for SnowDucks.

This script tests the PostgreSQL connection and DuckLake integration.
"""

import os
import sys
import subprocess
from pathlib import Path

def test_postgresql_connection():
    """Test basic PostgreSQL connection."""
    print("🧪 Testing PostgreSQL connection...")
    
    try:
        result = subprocess.run([
            'docker-compose', 'exec', '-T', 'postgres',
            'psql', '-U', 'snowducks_user', '-d', 'snowducks_metadata',
            '-c', 'SELECT version();'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ PostgreSQL connection successful")
            return True
        else:
            print(f"❌ PostgreSQL connection failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing PostgreSQL connection: {e}")
        return False

def test_snowducks_config():
    """Test SnowDucks configuration with PostgreSQL."""
    print("\n🧪 Testing SnowDucks configuration...")
    
    try:
        # Import SnowDucks config
        sys.path.insert(0, str(Path("cli")))
        from snowducks.config import SnowDucksConfig
        
        # Load configuration
        config = SnowDucksConfig.from_env()
        
        # Check if PostgreSQL is configured
        if config.postgres_host:
            print("✅ PostgreSQL configuration found")
            print(f"   Host: {config.postgres_host}")
            print(f"   Port: {config.postgres_port}")
            print(f"   Database: {config.postgres_database}")
            print(f"   User: {config.postgres_user}")
            print(f"   Schema: {config.postgres_schema}")
            return True
        else:
            print("❌ PostgreSQL configuration not found")
            return False
            
    except Exception as e:
        print(f"❌ Error testing SnowDucks configuration: {e}")
        return False

def test_ducklake_attach():
    """Test DuckLake attachment to PostgreSQL."""
    print("\n🧪 Testing DuckLake attachment...")
    
    try:
        # Test DuckLake attachment string
        sys.path.insert(0, str(Path("cli")))
        from snowducks.config import SnowDucksConfig
        
        config = SnowDucksConfig.from_env()
        attach_string = config.get_ducklake_attach_string()
        
        print(f"✅ DuckLake attach string: {attach_string}")
        
        # Test if it contains PostgreSQL
        if "postgresql://" in attach_string:
            print("✅ PostgreSQL DuckLake configuration detected")
            return True
        else:
            print("❌ PostgreSQL DuckLake configuration not detected")
            return False
            
    except Exception as e:
        print(f"❌ Error testing DuckLake attachment: {e}")
        return False

def test_snowducks_cli():
    """Test SnowDucks CLI with PostgreSQL."""
    print("\n🧪 Testing SnowDucks CLI...")
    
    try:
        # Test a simple SnowDucks command
        result = subprocess.run([
            'python3', '-m', 'snowducks.cli', 'info'
        ], capture_output=True, text=True, cwd='cli')
        
        if result.returncode == 0:
            print("✅ SnowDucks CLI works with PostgreSQL")
            return True
        else:
            print(f"❌ SnowDucks CLI failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing SnowDucks CLI: {e}")
        return False

def main():
    """Main test function."""
    print("🦆 SnowDucks PostgreSQL Setup Test")
    print("=" * 50)
    
    # Check if Docker Compose is running
    try:
        result = subprocess.run(['docker-compose', 'ps'], capture_output=True, text=True)
        if result.returncode != 0 or 'postgres' not in result.stdout:
            print("❌ PostgreSQL container not running")
            print("   Run: make postgres-start")
            return 1
    except Exception as e:
        print(f"❌ Error checking Docker Compose: {e}")
        return 1
    
    # Run tests
    tests = [
        test_postgresql_connection,
        test_snowducks_config,
        test_ducklake_attach,
        test_snowducks_cli
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
        print()
    
    # Summary
    print("=" * 50)
    print(f"📊 Test Results: {passed}/{total} passed")
    
    if passed == total:
        print("🎉 All tests passed! PostgreSQL setup is working correctly.")
        print("\n🦆 You can now use SnowDucks with PostgreSQL:")
        print("   ./snowducks cli")
        print("   ./snowducks ui")
        return 0
    else:
        print("❌ Some tests failed. Please check the setup.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 