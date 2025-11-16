import duckdb
import os

# Connect to DuckDB
conn = duckdb.connect('data/processed/cybersec_health.duckdb')

# Create staging views
print("Creating staging tables...")

# stg_customers
conn.execute("""
CREATE OR REPLACE VIEW stg_customers AS
select
    customer_id,
    company_name as customer_name,
    industry,
    contract_start_date::date as contract_start_date,
    monthly_recurring_revenue::decimal(10,2) as mrr
from read_csv_auto('data/raw/customers.csv')
""")

# stg_security_incidents  
conn.execute("""
CREATE OR REPLACE VIEW stg_security_incidents AS
select
    customer_id,
    "Attack Type" as incident_type,
    "Severity Level" as severity,
    "Timestamp"::timestamp as detection_time,
    "Malware Indicators" as malware_indicators,
    "Anomaly Scores"::decimal as anomaly_score
from read_csv_auto('data/raw/security_incidents.csv')
""")

# Create marts table
print("Creating customer health scores...")
conn.execute("""
CREATE OR REPLACE TABLE customer_health_scores AS
with customer_metrics as (
    select 
        c.customer_id,
        c.customer_name,
        c.mrr,
        count(si.customer_id) as total_incidents,
        sum(case when si.severity = 'High' then 1 else 0 end) as high_incidents,
        avg(si.anomaly_score) as avg_anomaly_score,
        sum(case when si.malware_indicators = 'IoC Detected' then 1 else 0 end) as malware_detections
    from stg_customers c
    left join stg_security_incidents si on c.customer_id = si.customer_id
    group by c.customer_id, c.customer_name, c.mrr
)

select 
    customer_id,
    customer_name,
    mrr,
    total_incidents,
    high_incidents,
    avg_anomaly_score,
    malware_detections,
    case 
        when high_incidents > 2 or avg_anomaly_score > 70 then 'High Risk'
        when high_incidents > 0 or avg_anomaly_score > 40 then 'Medium Risk'
        else 'Low Risk'
    end as risk_level
from customer_metrics
""")

print("Pipeline completed successfully!")
print("Tables created: stg_customers, stg_security_incidents, customer_health_scores")

# Show results
result = conn.execute("SELECT * FROM customer_health_scores").fetchall()
print(f"\nCustomer Health Scores ({len(result)} records):")
for row in result:
    print(row)

conn.close()