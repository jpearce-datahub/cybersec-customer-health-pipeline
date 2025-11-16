import duckdb
import os

# Create DuckDB database for raw data
db_path = 'data/cybersec_health_raw.duckdb'
conn = duckdb.connect(db_path)

# Load CSV files and create tables
data_files = [
    'customers.csv',
    'support_tickets.csv', 
    'security_incidents.csv',
    'product_usage.csv',
    'customer_feedback.csv',
    'contract_events.csv'
]

for file in data_files:
    file_path = f'data/raw/{file}'
    if os.path.exists(file_path):
        table_name = file.replace('.csv', '')
        conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{file_path}')")
        print(f"Created table: {table_name}")

print("Raw data database setup complete.")
print("Raw database: data/cybersec_health_raw.duckdb")
print("Run 'dbt run' to build models in: data/cybersec_health_dbt.duckdb")
conn.close()