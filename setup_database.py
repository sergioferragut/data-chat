#!/usr/bin/env python3
"""
Setup script for Firebolt database.
Reads configuration from envvars.sh and executes setup_ddl.sql with parameterized values.
"""

import os
import sys
import re
import argparse
from pathlib import Path
from firebolt.client.auth import ClientCredentials
from firebolt.db import connect


def load_envvars(envvars_path="envvars.sh"):
    """Load environment variables from envvars.sh file."""
    env_vars = {}
    envvars_file = Path(envvars_path)
    
    if not envvars_file.exists():
        print(f"Error: {envvars_path} not found")
        sys.exit(1)
    
    # Read and parse envvars.sh
    with open(envvars_file, 'r') as f:
        for line in f:
            line = line.strip()
            # Skip comments and empty lines
            if not line or line.startswith('#'):
                continue
            
            # Parse export VAR="value" or export VAR='value'
            match = re.match(r'export\s+(\w+)="([^"]*)"', line)
            if match:
                var_name, var_value = match.groups()
                env_vars[var_name] = var_value
            else:
                # Try single quotes
                match = re.match(r"export\s+(\w+)='([^']*)'", line)
                if match:
                    var_name, var_value = match.groups()
                    env_vars[var_name] = var_value
    
    return env_vars


def read_sql_file(sql_path="setup_ddl.sql"):
    """Read the SQL file."""
    sql_file = Path(sql_path)
    
    if not sql_file.exists():
        print(f"Error: {sql_path} not found")
        sys.exit(1)
    
    with open(sql_file, 'r') as f:
        return f.read()


def parameterize_sql(sql_content, params):
    """Replace parameterized values in SQL."""
    # Replace DATABASE name
    sql_content = re.sub(
        r'CREATE DATABASE IF NOT EXISTS \w+;',
        f'CREATE DATABASE IF NOT EXISTS {params["FIREBOLT_DATABASE"]};',
        sql_content
    )
    sql_content = re.sub(
        r'USE DATABASE \w+;',
        f'USE DATABASE {params["FIREBOLT_DATABASE"]};',
        sql_content
    )
    
    # Replace ENGINE name
    sql_content = re.sub(
        r'CREATE ENGINE IF NOT EXISTS \w+;',
        f'CREATE ENGINE IF NOT EXISTS {params["FIREBOLT_ENGINE_NAME"]};',
        sql_content
    )
    sql_content = re.sub(
        r'USE ENGINE \w+;',
        f'USE ENGINE {params["FIREBOLT_ENGINE_NAME"]};',
        sql_content
    )
    
    # Replace S3 URL in ext_pdf_content table
    # Find the URL = '...' line and replace it
    s3_parquet_uri = params.get("S3_TARGET_PARQUET_URI", "")
    if s3_parquet_uri:
        # Ensure it ends with / if it's a directory
        if not s3_parquet_uri.endswith('/'):
            s3_parquet_uri = s3_parquet_uri + '/'
        
        sql_content = re.sub(
            r"URL = 's3://[^']+'",
            f"URL = '{s3_parquet_uri}'",
            sql_content
        )
    
    # Replace AWS credentials in LOCATION creation
    aws_access_key = params.get("AWS_ACCESS_KEY_ID", "")
    aws_secret_key = params.get("AWS_SECRET_ACCESS_KEY", "")
    aws_session_token = params.get("AWS_SESSION_TOKEN", "")
    
    if aws_access_key:
        sql_content = re.sub(
            r"AWS_ACCESS_KEY_ID='[^']*'",
            f"AWS_ACCESS_KEY_ID='{aws_access_key}'",
            sql_content
        )
    
    if aws_secret_key:
        sql_content = re.sub(
            r"AWS_SECRET_ACCESS_KEY='[^']*'",
            f"AWS_SECRET_ACCESS_KEY='{aws_secret_key}'",
            sql_content
        )
    
    if aws_session_token:
        sql_content = re.sub(
            r"AWS_SESSION_TOKEN='[^']*'",
            f"AWS_SESSION_TOKEN='{aws_session_token}'",
            sql_content
        )
    
    return sql_content


def split_sql_statements(sql_content):
    """Split SQL content into individual statements."""
    # Remove comments (-- style) but preserve structure
    lines = []
    for line in sql_content.split('\n'):
        # Remove inline comments, but keep the line if it has content before the comment
        if '--' in line:
            comment_pos = line.index('--')
            # Check if it's not inside a string
            before_comment = line[:comment_pos]
            if "'" not in before_comment or before_comment.count("'") % 2 == 0:
                line = before_comment.rstrip()
        lines.append(line)
    
    sql_content = '\n'.join(lines)
    
    # Split by semicolon, handling multi-line statements
    statements = []
    current_statement = []
    in_string = False
    string_char = None
    
    for line in sql_content.split('\n'):
        stripped = line.strip()
        if not stripped and not current_statement:
            continue
        
        # Track string literals to avoid splitting on semicolons inside strings
        for char in line:
            if char in ("'", '"') and (not in_string or char == string_char):
                in_string = not in_string
                if in_string:
                    string_char = char
                else:
                    string_char = None
        
        current_statement.append(line)
        
        # Check if line ends with semicolon and we're not inside a string
        if stripped.endswith(';') and not in_string:
            statement = '\n'.join(current_statement).strip()
            if statement and not statement.startswith('--'):
                statements.append(statement)
            current_statement = []
            in_string = False
            string_char = None
    
    # Add any remaining statement
    if current_statement:
        statement = '\n'.join(current_statement).strip()
        if statement and not statement.startswith('--'):
            statements.append(statement)
    
    return statements


def execute_sql_statements(connection, statements, verbose=False):
    """Execute SQL statements one by one."""
    cursor = connection.cursor()
    
    for i, statement in enumerate(statements, 1):
        # Skip empty statements
        if not statement.strip():
            continue
        
        # Skip comments-only statements
        if statement.strip().startswith('--'):
            continue
        
        try:
            if verbose:
                # Show first 100 chars of statement
                preview = statement[:100].replace('\n', ' ')
                if len(statement) > 100:
                    preview += "..."
                print(f"  [{i}/{len(statements)}] Executing: {preview}")
            
            cursor.execute(statement)
            
            if verbose:
                print(f"      ✓ Success")
        
        except Exception as e:
            print(f"      ✗ Error executing statement {i}:")
            print(f"         {str(e)}")
            # Show the problematic statement
            print(f"         Statement: {statement[:200]}...")
            raise


def cleanup_database(env_vars, api_endpoint=None):
    """Clean up all database objects including database and engine."""
    print("=" * 80)
    print("  Firebolt Database Cleanup")
    print("=" * 80)
    print()
    
    database = env_vars["FIREBOLT_DATABASE"]
    engine = env_vars["FIREBOLT_ENGINE_NAME"]
    account_name = env_vars.get("FIREBOLT_ACCOUNT_NAME", "developer")
    llm_location = env_vars.get("FIREBOLT_LLM_LOCATION", "llm_api")
    
    print(f"  Database: {database}")
    print(f"  Engine: {engine}")
    print(f"  Account: {account_name}")
    print()
    
    # Connect to Firebolt - first try with the database/engine
    # If that fails (because they don't exist), connect to system engine
    print("Connecting to Firebolt...")
    try:
        auth = ClientCredentials(
            client_id=env_vars["FIREBOLT_ID"],
            client_secret=env_vars["FIREBOLT_SECRET"]
        )
        
        # Try connecting to the database first
        connection_params = {
            "auth": auth,
            "engine_name": engine,
            "database": database,
            "account_name": account_name
        }
        
        if api_endpoint:
            connection_params["api_endpoint"] = api_endpoint
        
        try:
            connection = connect(**connection_params)
            print("✓ Connected to database")
        except Exception:
            # If database/engine doesn't exist, connect to system engine
            print("  Database/engine not found, connecting to system engine...")
            connection_params["engine_name"] = "system"
            connection_params["database"] = "information_schema"
            connection = connect(**connection_params)
            print("✓ Connected to system engine")
        
        print()
    except Exception as e:
        print(f"✗ Error connecting to Firebolt: {e}")
        sys.exit(1)
    
    cursor = connection.cursor()
    
    # Try to use the database if it exists
    try:
        cursor.execute(f"USE DATABASE {database};")
        using_database = True
    except:
        using_database = False
    
    # Try to use the engine if it exists
    try:
        cursor.execute(f"USE ENGINE {engine};")
        using_engine = True
    except:
        using_engine = False
        # Switch to system engine for dropping database/engine
        try:
            cursor.execute("USE ENGINE system;")
        except:
            pass
    
    print("Executing cleanup statements...")
    print("-" * 80)
    
    # Count total cleanup steps
    total_steps = 7
    
    # 1. Drop indexes (if they exist)
    step = 1
    if using_database:
        try:
            print(f"  [{step}/{total_steps}] Executing: DROP INDEX IF EXISTS pdf_semantic_index;")
            cursor.execute("DROP INDEX IF EXISTS pdf_semantic_index;")
            print(f"      ✓ Success")
        except Exception as e:
            error_msg = str(e).lower()
            if "does not exist" in error_msg or "not found" in error_msg:
                print(f"      ⚠ Index does not exist (skipping)")
            else:
                print(f"      ✗ Error: {str(e)}")
    
    # 2. Drop tables (if they exist)
    if using_database:
        for table_name in ["pdf_semantic_knowledge", "flights"]:
            step += 1
            try:
                print(f"  [{step}/{total_steps}] Executing: DROP TABLE IF EXISTS {table_name};")
                cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
                print(f"      ✓ Success")
            except Exception as e:
                error_msg = str(e).lower()
                if "does not exist" in error_msg or "not found" in error_msg:
                    print(f"      ⚠ Table does not exist (skipping)")
                else:
                    print(f"      ✗ Error: {str(e)}")
    else:
        step += 2  # Skip both tables if database doesn't exist
    
    # 3. Drop external tables (if they exist)
    step += 1
    if using_database:
        try:
            print(f"  [{step}/{total_steps}] Executing: DROP TABLE IF EXISTS ext_pdf_content;")
            cursor.execute("DROP TABLE IF EXISTS ext_pdf_content;")
            print(f"      ✓ Success")
        except Exception as e:
            error_msg = str(e).lower()
            if "does not exist" in error_msg or "not found" in error_msg:
                print(f"      ⚠ External table does not exist (skipping)")
            else:
                print(f"      ✗ Error: {str(e)}")
    
    # 4. Drop locations (if they exist)
    step += 1
    if using_database:
        try:
            print(f"  [{step}/{total_steps}] Executing: DROP LOCATION IF EXISTS {llm_location};")
            cursor.execute(f"DROP LOCATION IF EXISTS {llm_location};")
            print(f"      ✓ Success")
        except Exception as e:
            error_msg = str(e).lower()
            if "does not exist" in error_msg or "not found" in error_msg:
                print(f"      ⚠ Location does not exist (skipping)")
            else:
                print(f"      ✗ Error: {str(e)}")
    
    # 5. Switch to system engine for dropping database/engine
    step += 1
    print(f"  [{step}/{total_steps}] Switching to system engine...")
    try:
        cursor.execute("USE ENGINE system;")
        cursor.execute("USE DATABASE information_schema;")
        print(f"      ✓ Success")
    except Exception as e:
        print(f"      ⚠ Warning: {str(e)}")
    
    # 6. Drop database (if it exists)
    step += 1
    try:
        print(f"  [{step}/{total_steps}] Executing: DROP DATABASE IF EXISTS {database};")
        cursor.execute(f"DROP DATABASE IF EXISTS {database};")
        print(f"      ✓ Success")
    except Exception as e:
        error_msg = str(e).lower()
        if "does not exist" in error_msg or "not found" in error_msg:
            print(f"      ⚠ Database does not exist (skipping)")
        else:
            print(f"      ✗ Error: {str(e)}")
    
    # 7. Drop engine (if it exists)
    step += 1
    try:
        print(f"  [{step}/{total_steps}] Executing: DROP ENGINE IF EXISTS {engine};")
        cursor.execute(f"DROP ENGINE IF EXISTS {engine};")
        print(f"      ✓ Success")
    except Exception as e:
        error_msg = str(e).lower()
        if "does not exist" in error_msg or "not found" in error_msg:
            print(f"      ⚠ Engine does not exist (skipping)")
        else:
            print(f"      ✗ Error: {str(e)}")
    
    print("-" * 80)
    print("✓ Cleanup completed")
    print()
    
    connection.close()
    
    print("=" * 80)
    print("  Database cleanup completed!")
    print("=" * 80)


def main():
    """Main function to set up or clean up the database."""
    parser = argparse.ArgumentParser(
        description="Setup or cleanup Firebolt database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python setup_database.py              # Setup database
  python setup_database.py --cleanup    # Cleanup all database objects
        """
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Clean up all database objects including database and engine"
    )
    
    args = parser.parse_args()
    
    # Load environment variables
    print("Loading configuration from envvars.sh...")
    try:
        env_vars = load_envvars()
        print("✓ Configuration loaded")
    except Exception as e:
        print(f"✗ Error loading configuration: {e}")
        sys.exit(1)
    
    # Determine API endpoint
    mcp_api_url = env_vars.get("FIREBOLT_MCP_API_URL", "")
    api_endpoint = None
    if mcp_api_url:
        if "staging" in mcp_api_url.lower():
            api_endpoint = "https://api.staging.firebolt.io"
        else:
            base_url = mcp_api_url.split("?")[0]
            if not base_url.startswith("http"):
                api_endpoint = f"https://{base_url}"
            else:
                api_endpoint = base_url
    
    # Handle cleanup mode
    if args.cleanup:
        # For cleanup, we only need basic credentials
        required_vars = [
            "FIREBOLT_ID",
            "FIREBOLT_SECRET",
            "FIREBOLT_DATABASE",
            "FIREBOLT_ENGINE_NAME"
        ]
        
        missing_vars = [var for var in required_vars if not env_vars.get(var)]
        if missing_vars:
            print(f"✗ Error: Missing required environment variables: {', '.join(missing_vars)}")
            sys.exit(1)
        
        cleanup_database(env_vars, api_endpoint)
        return
    
    # Setup mode - validate all required variables
    required_vars = [
        "FIREBOLT_ID",
        "FIREBOLT_SECRET",
        "FIREBOLT_DATABASE",
        "FIREBOLT_ENGINE_NAME",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN"
    ]
    
    missing_vars = [var for var in required_vars if not env_vars.get(var)]
    if missing_vars:
        print(f"✗ Error: Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)
    
    print("=" * 80)
    print("  Firebolt Database Setup")
    print("=" * 80)
    print()
    
    print(f"  Database: {env_vars['FIREBOLT_DATABASE']}")
    print(f"  Engine: {env_vars['FIREBOLT_ENGINE_NAME']}")
    print(f"  Account: {env_vars.get('FIREBOLT_ACCOUNT_NAME', 'developer')}")
    if env_vars.get('S3_TARGET_PARQUET_URI'):
        print(f"  S3 Parquet URI: {env_vars['S3_TARGET_PARQUET_URI']}")
    print()
    
    # Read SQL file
    print("Reading setup_ddl.sql...")
    try:
        sql_content = read_sql_file()
        print("✓ SQL file read")
    except Exception as e:
        print(f"✗ Error reading SQL file: {e}")
        sys.exit(1)
    
    # Parameterize SQL
    print("Parameterizing SQL...")
    try:
        parameterized_sql = parameterize_sql(sql_content, env_vars)
        print("✓ SQL parameterized")
    except Exception as e:
        print(f"✗ Error parameterizing SQL: {e}")
        sys.exit(1)
    
    # Split into statements
    print("Parsing SQL statements...")
    try:
        statements = split_sql_statements(parameterized_sql)
        print(f"✓ Found {len(statements)} SQL statement(s)")
    except Exception as e:
        print(f"✗ Error parsing SQL: {e}")
        sys.exit(1)
    
    print()
    
    # Connect to Firebolt
    print("Connecting to Firebolt...")
    try:
        auth = ClientCredentials(
            client_id=env_vars["FIREBOLT_ID"],
            client_secret=env_vars["FIREBOLT_SECRET"]
        )
        
        connection_params = {
            "auth": auth,
            "engine_name": env_vars["FIREBOLT_ENGINE_NAME"],
            "database": env_vars["FIREBOLT_DATABASE"],
            "account_name": env_vars.get("FIREBOLT_ACCOUNT_NAME", "developer")
        }
        
        if api_endpoint:
            connection_params["api_endpoint"] = api_endpoint
        
        connection = connect(**connection_params)
        print("✓ Connected successfully")
        print()
    except Exception as e:
        print(f"✗ Error connecting to Firebolt: {e}")
        sys.exit(1)
    
    # Execute SQL statements
    print("Executing SQL statements...")
    print("-" * 80)
    try:
        execute_sql_statements(connection, statements, verbose=True)
        print("-" * 80)
        print("✓ All SQL statements executed successfully")
    except Exception as e:
        print("-" * 80)
        print(f"✗ Error executing SQL: {e}")
        connection.close()
        sys.exit(1)
    
    # Close connection
    connection.close()
    print()
    print("=" * 80)
    print("  Database setup completed successfully!")
    print("=" * 80)


if __name__ == "__main__":
    main()

