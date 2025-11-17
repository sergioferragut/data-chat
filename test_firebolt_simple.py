#!/usr/bin/env python3
"""
Simple test script for Firebolt connection and basic queries.
Tests connection, authentication, and simple SQL queries.
"""

import os
import sys
from firebolt.client.auth import ClientCredentials
from firebolt.db import connect

def main():
    """Simple test of Firebolt connection and queries."""
    
    print("=" * 80)
    print("Simple Firebolt Connection Test")
    print("=" * 80)
    print()
    
    # Load configuration from environment variables
    firebolt_id = os.getenv("FIREBOLT_ID")
    firebolt_secret = os.getenv("FIREBOLT_SECRET")
    engine_name = os.getenv("FIREBOLT_ENGINE_NAME", "system")
    database = os.getenv("FIREBOLT_DATABASE", "data_chat_demo")
    account_name = os.getenv("FIREBOLT_ACCOUNT_NAME", "developer")
    
    # Determine API endpoint - use staging endpoint if MCP API URL indicates staging
    mcp_api_url = os.getenv("FIREBOLT_MCP_API_URL", "")
    api_endpoint = None
    if mcp_api_url:
        # Check if this is a staging environment
        if "staging" in mcp_api_url.lower():
            # Use the staging API endpoint format
            api_endpoint = "https://api.staging.firebolt.io"
        else:
            # Extract from MCP API URL for other environments
            base_url = mcp_api_url.split("?")[0]
            if not base_url.startswith("http"):
                api_endpoint = f"https://{base_url}"
            else:
                api_endpoint = base_url
    
    print("Configuration:")
    print(f"  Client ID: {firebolt_id[:10]}..." if firebolt_id else "  Client ID: NOT SET")
    print(f"  Engine: {engine_name}")
    print(f"  Database: {database}")
    print(f"  Account: {account_name}")
    if api_endpoint:
        print(f"  API Endpoint: {api_endpoint}")
    print()
    
    # Validate required parameters
    if not firebolt_id or not firebolt_secret:
        print("❌ Error: FIREBOLT_ID and FIREBOLT_SECRET must be set")
        sys.exit(1)
    
    try:
        # Create authentication
        print("Creating authentication credentials...")
        auth = ClientCredentials(
            client_id=firebolt_id,
            client_secret=firebolt_secret
        )
        print("✓ Authentication object created")
        print()
        
        # Connect to Firebolt
        print("Connecting to Firebolt...")
        connection_params = {
            "auth": auth,
            "engine_name": engine_name,
            "database": database,
            "account_name": account_name
        }
        if api_endpoint:
            connection_params["api_endpoint"] = api_endpoint
        
        connection = connect(**connection_params)
        print("✓ Connected successfully")
        print()
        
        # Run a simple query
        print("Running test query: SELECT 1 as test_value")
        print("-" * 80)
        cursor = connection.cursor()
        cursor.execute("SELECT 1 as test_value")
        result = cursor.fetchone()
        print(f"✓ Query executed successfully")
        print(f"  Result: {result}")
        print()
        
        # Run another query to show database info
        print("Running query: SHOW DATABASES")
        print("-" * 80)
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()
        print(f"✓ Query executed successfully")
        print(f"  Found {len(databases)} database(s)")
        if len(databases) > 0:
            print(f"  First database: {databases[0][0] if databases[0] else 'N/A'}")
        print()
        
        # Test query on the current database
        print(f"Running query: SHOW TABLES (in database '{database}')")
        print("-" * 80)
        try:
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            print(f"✓ Query executed successfully")
            print(f"  Found {len(tables)} table(s)")
            if len(tables) > 0:
                print(f"  First table: {tables[0][0] if tables[0] else 'N/A'}")
        except Exception as e:
            print(f"⚠ Query failed (this is OK if database is empty): {e}")
        print()
        
        # Close connection
        cursor.close()
        connection.close()
        print("✓ Connection closed")
        print()
        
        print("=" * 80)
        print("✓ All tests passed! Connection and queries work correctly.")
        print("=" * 80)
        
    except Exception as e:
        error_str = str(e)
        print(f"\n❌ Error during testing: {e}")
        
        # Provide helpful guidance for common errors
        if "AuthenticationError" in error_str or "Failed to authenticate" in error_str:
            print("\n" + "=" * 80)
            print("AUTHENTICATION ERROR")
            print("=" * 80)
            print("\nThe Firebolt SDK is unable to authenticate with the provided credentials.")
            print("\nPossible causes:")
            print("1. Client ID and Secret don't match the Firebolt environment/domain")
            print("2. The API endpoint URL format may be incorrect")
            print("3. The credentials may be for a different Firebolt account/environment")
            print("\nTo fix:")
            print("- Verify your FIREBOLT_ID and FIREBOLT_SECRET are correct")
            print("- Check if you need different credentials for the staging environment")
            print("- Contact Firebolt support if credentials are correct but still failing")
        elif "does not exist" in error_str and "Engine" in error_str:
            print("\n" + "=" * 80)
            print("ENGINE NOT FOUND")
            print("=" * 80)
            print("\nThe specified engine does not exist or you don't have permission to use it.")
            print("\nTo fix:")
            print(f"- Verify the engine '{engine_name}' exists in your Firebolt account")
            print("- Check that you have permission to use this engine")
            print("- Update FIREBOLT_ENGINE_NAME in envvars.sh to use a valid engine")
        else:
            import traceback
            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

