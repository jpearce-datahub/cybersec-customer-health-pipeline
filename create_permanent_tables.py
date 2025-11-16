import duckdb

conn = duckdb.connect('data/processed/cybersec_health.duckdb')

# Create permanent staging tables
conn.execute("""
CREATE TABLE IF NOT EXISTS stg_customers AS
SELECT
    customer_id,
    company_name as customer_name,
    industry,
    contract_start_date::date as contract_start_date,
    monthly_recurring_revenue::decimal(10,2) as mrr
FROM read_csv_auto('data/raw/customers.csv')
""")

conn.execute("""
CREATE TABLE IF NOT EXISTS stg_security_incidents AS
SELECT
    customer_id,
    "Attack Type" as incident_type,
    "Severity Level" as severity,
    "Timestamp"::timestamp as detection_time,
    "Malware Indicators" as malware_indicators,
    "Anomaly Scores"::decimal as anomaly_score
FROM read_csv_auto('data/raw/security_incidents.csv')
""")

print("Permanent staging tables created")
conn.close()