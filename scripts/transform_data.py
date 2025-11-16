import duckdb
import pandas as pd

# Create DuckDB connection
conn = duckdb.connect('data/processed/cybersec_health.duckdb')

# Create staging tables
print("Creating staging tables...")

# Customers staging
conn.execute("""
CREATE OR REPLACE TABLE stg_customers AS
SELECT
    customer_id,
    customer_name,
    industry,
    company_size,
    contract_start_date::date as contract_start_date,
    contract_end_date::date as contract_end_date,
    monthly_recurring_revenue::decimal(10,2) as mrr,
    account_manager
FROM read_csv_auto('data/raw/customers.csv')
""")

# Security incidents staging
conn.execute("""
CREATE OR REPLACE TABLE stg_security_incidents AS
SELECT
    incident_id,
    customer_id,
    incident_type,
    severity,
    detection_time::timestamp as detection_time,
    resolution_time::timestamp as resolution_time,
    false_positive::boolean as false_positive,
    CASE 
        WHEN resolution_time IS NOT NULL AND detection_time IS NOT NULL
        THEN extract(epoch from (resolution_time - detection_time)) / 3600.0
        ELSE NULL
    END as resolution_hours
FROM read_csv_auto('data/raw/security_incidents.csv')
""")

# Customer health scores mart
print("Creating customer health scores...")
conn.execute("""
CREATE OR REPLACE TABLE customer_health_scores AS
WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        c.mrr,
        count(si.incident_id) as total_incidents,
        sum(CASE WHEN si.severity = 'Critical' THEN 1 ELSE 0 END) as critical_incidents,
        avg(si.resolution_hours) as avg_resolution_hours,
        sum(CASE WHEN si.false_positive THEN 1 ELSE 0 END) as false_positives
    FROM stg_customers c
    LEFT JOIN stg_security_incidents si ON c.customer_id = si.customer_id
    GROUP BY c.customer_id, c.customer_name, c.mrr
)
SELECT 
    customer_id,
    customer_name,
    mrr,
    total_incidents,
    critical_incidents,
    avg_resolution_hours,
    false_positives,
    CASE 
        WHEN critical_incidents > 2 OR avg_resolution_hours > 24 THEN 'High Risk'
        WHEN critical_incidents > 0 OR avg_resolution_hours > 12 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END as risk_level
FROM customer_metrics
""")

print("Data transformation complete!")
print("Tables created: stg_customers, stg_security_incidents, customer_health_scores")

# Show sample results
print("\nSample customer health scores:")
result = conn.execute("SELECT * FROM customer_health_scores LIMIT 5").fetchdf()
print(result)

conn.close()