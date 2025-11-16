#!/usr/bin/env python3
"""Fix DBeaver connection issues with DuckDB databases."""

import duckdb
import os

def fix_dbeaver_connection():
    """Fix DBeaver connection by ensuring proper database structure."""
    
    # Database paths
    raw_db = 'data/cybersec_health_raw.duckdb'
    dbt_db = 'data/cybersec_health_dbt.duckdb'
    
    print("Fixing DBeaver connection issues...")
    
    # Fix raw database
    if os.path.exists(raw_db):
        try:
            conn = duckdb.connect(raw_db)
            # Create main schema if it doesn't exist
            conn.execute("CREATE SCHEMA IF NOT EXISTS main")
            conn.close()
            print(f"Fixed {raw_db}")
        except Exception as e:
            print(f"Error fixing {raw_db}: {e}")
    
    # Fix dbt database
    if os.path.exists(dbt_db):
        try:
            conn = duckdb.connect(dbt_db)
            # Create main schema if it doesn't exist
            conn.execute("CREATE SCHEMA IF NOT EXISTS main")
            conn.close()
            print(f"Fixed {dbt_db}")
        except Exception as e:
            print(f"Error fixing {dbt_db}: {e}")
    
    print("\nDBeaver Connection Instructions:")
    print("1. Close DBeaver completely")
    print("2. Reopen DBeaver")
    print("3. Create new DuckDB connections:")
    print(f"   - Raw DB: {os.path.abspath(raw_db)}")
    print(f"   - dbt DB: {os.path.abspath(dbt_db)}")
    print("4. Use 'main' as the default schema")

if __name__ == "__main__":
    fix_dbeaver_connection()