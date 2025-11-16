import pandas as pd
import os

# Create processed directory if it doesn't exist
os.makedirs('data/processed', exist_ok=True)

print("Loading raw data...")

# Load raw data
customers = pd.read_csv('data/raw/customers.csv')
incidents = pd.read_csv('data/raw/security_incidents.csv')

print("Transforming data...")

# Clean customers data
customers['contract_start_date'] = pd.to_datetime(customers['contract_start_date'])
customers['contract_end_date'] = pd.to_datetime(customers['contract_end_date'])
customers['monthly_recurring_revenue'] = pd.to_numeric(customers['monthly_recurring_revenue'])

# Clean incidents data
incidents['detection_time'] = pd.to_datetime(incidents['detection_time'])
incidents['resolution_time'] = pd.to_datetime(incidents['resolution_time'])
incidents['false_positive'] = incidents['false_positive'].astype(bool)

# Calculate resolution hours
incidents['resolution_hours'] = (incidents['resolution_time'] - incidents['detection_time']).dt.total_seconds() / 3600

# Create customer health scores
customer_metrics = incidents.groupby('customer_id').agg({
    'incident_id': 'count',
    'severity': lambda x: (x == 'Critical').sum(),
    'resolution_hours': 'mean',
    'false_positive': 'sum'
}).rename(columns={
    'incident_id': 'total_incidents',
    'severity': 'critical_incidents',
    'false_positive': 'false_positives'
})

# Merge with customer data
health_scores = customers.merge(customer_metrics, on='customer_id', how='left')
health_scores = health_scores.fillna(0)

# Calculate risk level
def calculate_risk(row):
    if row['critical_incidents'] > 2 or row['resolution_hours'] > 24:
        return 'High Risk'
    elif row['critical_incidents'] > 0 or row['resolution_hours'] > 12:
        return 'Medium Risk'
    else:
        return 'Low Risk'

health_scores['risk_level'] = health_scores.apply(calculate_risk, axis=1)

# Save processed data
customers.to_csv('data/processed/stg_customers.csv', index=False)
incidents.to_csv('data/processed/stg_security_incidents.csv', index=False)
health_scores.to_csv('data/processed/customer_health_scores.csv', index=False)

print("Data transformation complete!")
print(f"Processed {len(customers)} customers and {len(incidents)} incidents")
print(f"Created customer health scores for {len(health_scores)} customers")

# Show sample results
print("\nSample customer health scores:")
print(health_scores[['customer_name', 'mrr', 'total_incidents', 'critical_incidents', 'risk_level']].head())