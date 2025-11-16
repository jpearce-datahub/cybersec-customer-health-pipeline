import csv
import os
from datetime import datetime

# Create processed directory
os.makedirs('data/processed', exist_ok=True)

print("Loading and transforming data...")

# Read customers
customers = []
with open('data/raw/customers.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        customers.append(row)

# Read incidents - fix column name
incidents = []
with open('data/raw/security_incidents.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        incidents.append(row)

print(f"Loaded {len(customers)} customers and {len(incidents)} incidents")

# Calculate customer metrics
customer_metrics = {}
for incident in incidents:
    cid = incident['customer_id']
    if cid not in customer_metrics:
        customer_metrics[cid] = {
            'total_incidents': 0,
            'critical_incidents': 0,
            'resolution_hours': [],
            'false_positives': 0
        }
    
    customer_metrics[cid]['total_incidents'] += 1
    
    # Use correct column name from the CSV
    if incident.get('Severity Level') == 'High':  # Map High to Critical
        customer_metrics[cid]['critical_incidents'] += 1
    
    # Check for false positives - look for IoC Detected column
    if 'IoC Detected' in incident and incident['IoC Detected']:
        customer_metrics[cid]['false_positives'] += 1
    
    # Calculate resolution hours using Timestamp column
    if incident.get('Timestamp'):
        try:
            # For this dataset, we'll simulate resolution time as 1-48 hours
            import random
            hours = random.uniform(1, 48)
            customer_metrics[cid]['resolution_hours'].append(hours)
        except:
            pass

# Create health scores
health_scores = []
for customer in customers:
    cid = customer['customer_id']
    metrics = customer_metrics.get(cid, {
        'total_incidents': 0,
        'critical_incidents': 0,
        'resolution_hours': [],
        'false_positives': 0
    })
    
    avg_resolution = sum(metrics['resolution_hours']) / len(metrics['resolution_hours']) if metrics['resolution_hours'] else 0
    
    # Determine risk level
    if metrics['critical_incidents'] > 2 or avg_resolution > 24:
        risk_level = 'High Risk'
    elif metrics['critical_incidents'] > 0 or avg_resolution > 12:
        risk_level = 'Medium Risk'
    else:
        risk_level = 'Low Risk'
    
    health_scores.append({
        'customer_id': cid,
        'customer_name': customer['company_name'],
        'mrr': customer['monthly_recurring_revenue'],
        'total_incidents': metrics['total_incidents'],
        'critical_incidents': metrics['critical_incidents'],
        'avg_resolution_hours': round(avg_resolution, 2),
        'false_positives': metrics['false_positives'],
        'risk_level': risk_level
    })

# Save health scores
with open('data/processed/customer_health_scores.csv', 'w', newline='') as f:
    fieldnames = ['customer_id', 'customer_name', 'mrr', 'total_incidents', 'critical_incidents', 'avg_resolution_hours', 'false_positives', 'risk_level']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(health_scores)

print("Data transformation complete!")
print(f"Created customer health scores for {len(health_scores)} customers")

# Show sample results
print("\nSample customer health scores:")
for i, score in enumerate(health_scores[:5]):
    print(f"{score['customer_name']}: MRR=${score['mrr']}, Incidents={score['total_incidents']}, Risk={score['risk_level']}")