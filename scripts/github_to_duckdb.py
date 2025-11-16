import duckdb

# GitHub raw file URLs
base_url = "https://raw.githubusercontent.com/jpearce-datahub/cybersec-customer-health-pipeline/main/data/raw/"
files = [
    "customers.csv",
    "security_incidents.csv", 
    "support_tickets.csv",
    "product_usage.csv",
    "customer_feedback.csv"
]

conn = duckdb.connect('data/processed/cybersec_health.duckdb')

for file in files:
    try:
        url = base_url + file
        table_name = file.replace('.csv', '')
        
        # Create table directly from GitHub URL
        conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{url}')")
        print(f"Created table: {table_name}")
        
    except Exception as e:
        print(f"Failed to create {table_name}: {e}")

print("\nPipeline complete! Refresh DBeaver to see tables.")
conn.close()