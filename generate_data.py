import csv
from datetime import datetime, timedelta
import random

random.seed(42)

industries = ['Retail', 'IT Services', 'Healthcare', 'Manufacturing', 'Finance', 'Legal', 'Real Estate', 'Education']

# Generate 75 customers
customers_data = []
for i in range(1, 76):
    customers_data.append({
        'customer_id': f"CUST_{i:03d}",
        'company_name': f"Company_{i:03d}",
        'industry': random.choice(industries),
        'monthly_recurring_revenue': random.randint(1000, 15000),
        'risk_score': random.choices(['low', 'medium', 'high'], weights=[60, 30, 10])[0],
        'contract_start_date': (datetime.now() - timedelta(days=random.randint(30, 730))).strftime('%Y-%m-%d'),
        'license_utilization': round(random.uniform(0.3, 1.0), 2)
    })

# Generate security incidents
incidents_data = []
severities = ['low', 'medium', 'high', 'critical']
sla_map = {'low': '', 'medium': 60, 'high': 20, 'critical': 5}

for i in range(1, 151):
    severity = random.choices(severities, weights=[40, 35, 20, 5])[0]
    sla_minutes = sla_map[severity]
    
    worked_within_sla = ''
    if sla_minutes:
        worked_within_sla = random.choice([True, False])
    
    incidents_data.append({
        'incident_id': f"INC_{i:04d}",
        'customer_id': random.choice([c['customer_id'] for c in customers_data]),
        'severity': severity,
        'sla_minutes': sla_minutes,
        'worked_within_sla': worked_within_sla,
        'incident_type': random.choice(['Malware', 'Phishing', 'Data Breach', 'DDoS', 'Unauthorized Access']),
        'detection_time': (datetime.now() - timedelta(days=random.randint(1, 90))).strftime('%Y-%m-%d %H:%M:%S'),
        'status': random.choice(['Open', 'In Progress', 'Resolved', 'Closed'])
    })

# Generate support tickets
tickets_data = []

# Customer generated tickets
for i in range(1, 201):
    tickets_data.append({
        'ticket_id': f"TKT_{i:04d}",
        'customer_id': random.choice([c['customer_id'] for c in customers_data]),
        'ticket_type': 'Customer Generated',
        'priority': random.choice(['Low', 'Medium', 'High']),
        'status': random.choice(['Open', 'In Progress', 'Resolved', 'Closed']),
        'created_date': (datetime.now() - timedelta(days=random.randint(1, 60))).strftime('%Y-%m-%d'),
        'resolution_time_hours': random.randint(1, 72) if random.random() > 0.2 else '',
        'escalated': random.choice(['TRUE', 'FALSE']),
        'related_incident_id': ''
    })

# Generate incident-related tickets for high/critical incidents
for incident in incidents_data:
    if incident['severity'] in ['high', 'critical']:
        ticket_id = f"TKT_{len(tickets_data) + 1:04d}"
        tickets_data.append({
            'ticket_id': ticket_id,
            'customer_id': incident['customer_id'],
            'ticket_type': 'Incident Generated',
            'priority': 'High' if incident['severity'] == 'high' else 'Critical',
            'status': incident['status'],
            'created_date': incident['detection_time'][:10],
            'resolution_time_hours': random.randint(1, 24) if incident['status'] in ['Resolved', 'Closed'] else '',
            'escalated': 'TRUE' if incident['severity'] == 'critical' else random.choice(['TRUE', 'FALSE']),
            'related_incident_id': incident['incident_id']
        })

# Generate feedback data
feedback_data = []
for customer in customers_data:
    if random.random() > 0.3:
        feedback_data.append({
            'customer_id': customer['customer_id'],
            'nps_score': random.randint(1, 10),
            'satisfaction_score': random.randint(1, 10),
            'likelihood_to_renew': random.choice(['Low', 'Medium', 'High']),
            'feedback_date': (datetime.now() - timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d')
        })

# Generate usage data
usage_data = []
for customer in customers_data:
    usage_data.append({
        'customer_id': customer['customer_id'],
        'login_frequency': random.randint(1, 30),
        'feature_adoption_score': round(random.uniform(0.2, 1.0), 2),
        'data_volume_gb': random.randint(10, 1000),
        'last_login_date': (datetime.now() - timedelta(days=random.randint(0, 7))).strftime('%Y-%m-%d')
    })

# Write CSV files
with open('data/raw/customers.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['customer_id', 'company_name', 'industry', 'monthly_recurring_revenue', 'risk_score', 'contract_start_date', 'license_utilization'])
    writer.writeheader()
    writer.writerows(customers_data)

with open('data/raw/security_incidents.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['incident_id', 'customer_id', 'severity', 'sla_minutes', 'worked_within_sla', 'incident_type', 'detection_time', 'status'])
    writer.writeheader()
    writer.writerows(incidents_data)

with open('data/raw/support_tickets.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['ticket_id', 'customer_id', 'ticket_type', 'priority', 'status', 'created_date', 'resolution_time_hours', 'escalated', 'related_incident_id'])
    writer.writeheader()
    writer.writerows(tickets_data)

with open('data/raw/customer_feedback.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['customer_id', 'nps_score', 'satisfaction_score', 'likelihood_to_renew', 'feedback_date'])
    writer.writeheader()
    writer.writerows(feedback_data)

with open('data/raw/product_usage.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['customer_id', 'login_frequency', 'feature_adoption_score', 'data_volume_gb', 'last_login_date'])
    writer.writeheader()
    writer.writerows(usage_data)

print(f"Generated datasets:")
print(f"- Customers: {len(customers_data)} records")
print(f"- Support Tickets: {len(tickets_data)} records")
print(f"- Security Incidents: {len(incidents_data)} records")
print(f"- Customer Feedback: {len(feedback_data)} records")
print(f"- Product Usage: {len(usage_data)} records")