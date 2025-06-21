#!/usr/bin/env python3
"""
Setup script for PostgreSQL metadata database for SnowDucks.

This script helps you set up a PostgreSQL database to use as the metadata store
for DuckLake, which will solve concurrency issues when multiple processes
access the same metadata.
"""

import os
import sys
import subprocess
from pathlib import Path

def check_postgresql_installed():
    """Check if PostgreSQL is installed and accessible."""
    try:
        result = subprocess.run(['psql', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ PostgreSQL found: {result.stdout.strip()}")
            return True
        else:
            print("❌ PostgreSQL not found or not accessible")
            return False
    except FileNotFoundError:
        print("❌ PostgreSQL not found. Please install PostgreSQL first.")
        return False

def create_database_and_user(host, port, database, user, password, schema):
    """Create the database, user, and schema for SnowDucks."""
    print(f"\n🦆 Setting up PostgreSQL metadata database...")
    
    # Connect as postgres superuser to create database and user
    create_commands = [
        f"CREATE DATABASE {database};",
        f"CREATE USER {user} WITH PASSWORD '{password}';",
        f"GRANT ALL PRIVILEGES ON DATABASE {database} TO {user};",
        f"\\c {database}",
        f"CREATE SCHEMA IF NOT EXISTS {schema};",
        f"GRANT ALL PRIVILEGES ON SCHEMA {schema} TO {user};",
        f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {schema} TO {user};",
        f"GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {schema} TO {user};",
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL ON TABLES TO {user};",
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL ON SEQUENCES TO {user};"
    ]
    
    # Write commands to a temporary file
    commands_file = Path("/tmp/snowducks_setup.sql")
    with open(commands_file, 'w') as f:
        f.write('\n'.join(create_commands))
    
    try:
        # Execute the setup commands
        result = subprocess.run([
            'psql', '-h', host, '-p', str(port), '-U', 'postgres', '-f', str(commands_file)
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Database and user created successfully!")
            return True
        else:
            print(f"❌ Failed to create database: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error setting up database: {e}")
        return False
    finally:
        # Clean up temporary file
        if commands_file.exists():
            commands_file.unlink()

def test_connection(host, port, database, user, password):
    """Test the connection to the PostgreSQL database."""
    print(f"\n🧪 Testing connection to PostgreSQL...")
    
    # Set environment variables for psql
    env = os.environ.copy()
    env['PGPASSWORD'] = password
    
    try:
        result = subprocess.run([
            'psql', '-h', host, '-p', str(port), '-U', user, '-d', database,
            '-c', 'SELECT version();'
        ], capture_output=True, text=True, env=env)
        
        if result.returncode == 0:
            print("✅ Connection successful!")
            return True
        else:
            print(f"❌ Connection failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing connection: {e}")
        return False

def generate_env_config(host, port, database, user, password, schema):
    """Generate the environment configuration."""
    config = f"""
# PostgreSQL configuration for SnowDucks DuckLake metadata
POSTGRES_HOST={host}
POSTGRES_PORT={port}
POSTGRES_DATABASE={database}
POSTGRES_USER={user}
POSTGRES_PASSWORD={password}
POSTGRES_SCHEMA={schema}
"""
    return config

def main():
    """Main setup function."""
    print("🦆 SnowDucks PostgreSQL Metadata Setup")
    print("=" * 50)
    
    # Check if PostgreSQL is installed
    if not check_postgresql_installed():
        print("\n📋 To install PostgreSQL:")
        print("  macOS: brew install postgresql")
        print("  Ubuntu: sudo apt-get install postgresql postgresql-contrib")
        print("  CentOS: sudo yum install postgresql postgresql-server")
        print("  Windows: Download from https://www.postgresql.org/download/windows/")
        return 1
    
    # Get configuration from user
    print("\n📝 Please provide PostgreSQL configuration:")
    
    host = input("Host (default: localhost): ").strip() or "localhost"
    port = input("Port (default: 5432): ").strip() or "5432"
    database = input("Database name (default: snowducks_metadata): ").strip() or "snowducks_metadata"
    user = input("Username (default: snowducks_user): ").strip() or "snowducks_user"
    password = input("Password: ").strip()
    schema = input("Schema (default: snowducks): ").strip() or "snowducks"
    
    if not password:
        print("❌ Password is required!")
        return 1
    
    # Create database and user
    if not create_database_and_user(host, port, database, user, password, schema):
        return 1
    
    # Test connection
    if not test_connection(host, port, database, user, password):
        return 1
    
    # Generate configuration
    config = generate_env_config(host, port, database, user, password, schema)
    
    print(f"\n✅ Setup complete!")
    print(f"\n📋 Add the following to your .env file:")
    print(config)
    
    # Offer to append to .env file
    env_file = Path(".env")
    if env_file.exists():
        append = input(f"\n📝 Append to existing .env file? (y/N): ").strip().lower()
        if append == 'y':
            with open(env_file, 'a') as f:
                f.write(config)
            print("✅ Configuration appended to .env file!")
    
    print(f"\n🎉 PostgreSQL metadata database is ready!")
    print(f"   You can now run SnowDucks with PostgreSQL metadata storage.")
    print(f"   This will solve concurrency issues when multiple processes access the same metadata.")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 