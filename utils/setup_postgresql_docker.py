#!/usr/bin/env python3
"""
Setup script for Docker PostgreSQL metadata database for SnowDucks.

This script configures the environment to use PostgreSQL in Docker
for DuckLake metadata storage, which solves concurrency issues.
"""

import os
import sys
import subprocess
from pathlib import Path

def check_docker():
    """Check if Docker is installed and running."""
    try:
        result = subprocess.run(['docker', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ Docker found: {result.stdout.strip()}")
            return True
        else:
            print("❌ Docker not found or not accessible")
            return False
    except FileNotFoundError:
        print("❌ Docker not found. Please install Docker first.")
        return False

def check_docker_compose():
    """Check if Docker Compose is available."""
    try:
        result = subprocess.run(['docker-compose', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ Docker Compose found: {result.stdout.strip()}")
            return True
        else:
            print("❌ Docker Compose not found")
            return False
    except FileNotFoundError:
        print("❌ Docker Compose not found. Please install Docker Compose first.")
        return False

def start_postgresql():
    """Start PostgreSQL using Docker Compose."""
    print("\n🐘 Starting PostgreSQL container...")
    
    try:
        # Start PostgreSQL container
        result = subprocess.run(['docker-compose', 'up', '-d', 'postgres'], 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ PostgreSQL container started")
            return True
        else:
            print(f"❌ Failed to start PostgreSQL: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error starting PostgreSQL: {e}")
        return False

def wait_for_postgresql():
    """Wait for PostgreSQL to be ready."""
    print("⏳ Waiting for PostgreSQL to be ready...")
    
    max_attempts = 30
    attempt = 0
    
    while attempt < max_attempts:
        try:
            result = subprocess.run([
                'docker-compose', 'exec', '-T', 'postgres', 
                'pg_isready', '-U', 'snowducks_user', '-d', 'snowducks_metadata'
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                print("✅ PostgreSQL is ready!")
                return True
                
        except Exception:
            pass
        
        attempt += 1
        print(f"Waiting for PostgreSQL... (attempt {attempt}/{max_attempts})")
        subprocess.run(['sleep', '2'])
    
    print("❌ PostgreSQL failed to start within timeout")
    return False

def update_env_file():
    """Update .env file with PostgreSQL configuration."""
    env_file = Path(".env")
    
    if not env_file.exists():
        print("❌ .env file not found. Please create one first.")
        return False
    
    # Read current .env file
    with open(env_file, 'r') as f:
        content = f.read()
    
    # PostgreSQL configuration
    postgres_config = """# PostgreSQL configuration for DuckLake metadata
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=snowducks_metadata
POSTGRES_USER=snowducks_user
POSTGRES_PASSWORD=snowducks_password
POSTGRES_SCHEMA=snowducks
"""
    
    # Check if PostgreSQL config already exists
    if "POSTGRES_HOST" in content:
        print("✅ PostgreSQL configuration already exists in .env file")
        return True
    
    # Append PostgreSQL configuration
    with open(env_file, 'a') as f:
        f.write(f"\n{postgres_config}")
    
    print("✅ PostgreSQL configuration added to .env file")
    return True

def test_connection():
    """Test the connection to PostgreSQL."""
    print("\n🧪 Testing PostgreSQL connection...")
    
    try:
        result = subprocess.run([
            'docker-compose', 'exec', '-T', 'postgres',
            'psql', '-U', 'snowducks_user', '-d', 'snowducks_metadata',
            '-c', 'SELECT version();'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ PostgreSQL connection successful!")
            return True
        else:
            print(f"❌ Connection failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing connection: {e}")
        return False

def main():
    """Main setup function."""
    print("🦆 SnowDucks Docker PostgreSQL Setup")
    print("=" * 50)
    
    # Check prerequisites
    if not check_docker():
        print("\n📋 To install Docker:")
        print("  macOS: https://docs.docker.com/desktop/mac/install/")
        print("  Ubuntu: https://docs.docker.com/engine/install/ubuntu/")
        print("  Windows: https://docs.docker.com/desktop/windows/install/")
        return 1
    
    if not check_docker_compose():
        print("\n📋 Docker Compose is usually included with Docker Desktop.")
        print("  For standalone installation: https://docs.docker.com/compose/install/")
        return 1
    
    # Check if docker-compose.yml exists
    if not Path("docker-compose.yml").exists():
        print("❌ docker-compose.yml not found. Please run this from the SnowDucks root directory.")
        return 1
    
    # Start PostgreSQL
    if not start_postgresql():
        return 1
    
    # Wait for PostgreSQL to be ready
    if not wait_for_postgresql():
        return 1
    
    # Update .env file
    if not update_env_file():
        return 1
    
    # Test connection
    if not test_connection():
        return 1
    
    print(f"\n🎉 Docker PostgreSQL setup complete!")
    print(f"\n📋 PostgreSQL is now running with:")
    print(f"   Database: snowducks_metadata")
    print(f"   User: snowducks_user")
    print(f"   Password: snowducks_password")
    print(f"   Port: 5432")
    print(f"   pgAdmin: http://localhost:8080 (admin@snowducks.local / admin)")
    
    print(f"\n🦆 You can now run SnowDucks with PostgreSQL metadata storage:")
    print(f"   ./snowducks cli")
    print(f"   ./snowducks ui")
    
    print(f"\n📋 Useful commands:")
    print(f"   make postgres-status  - Check PostgreSQL status")
    print(f"   make postgres-stop    - Stop PostgreSQL")
    print(f"   make postgres-reset   - Reset PostgreSQL data")
    print(f"   make postgres-logs    - View PostgreSQL logs")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 