import csv
from datetime import datetime

def load_csv(filename):
    """Simple CSV loader"""
    data = []
    with open(filename, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)
    return data

def calculate_simple_metrics():
    """Calculate basic customer health metrics"""
    
    # Load data
    customers = load_csv('../data/raw/customers.csv')
    tickets = load_csv('../data/raw/support_tickets.csv')
    incidents = load_csv('../data/raw/security_incidents.csv')
    feedback = load_csv('../data/raw/customer_feedback.csv')
    
    print("=" * 60)
    print("CYBERSECURITY CUSTOMER HEALTH DASHBOARD")
    print("=" * 60)
    
    # Customer Overview
    total_customers = len(customers)
    high_risk = len([c for c in customers if c['risk_score'] == 'high'])
    total_mrr = sum(float(c['monthly_recurring_revenue']) for c in customers)
    
    print(f"\nCUSTOMER OVERVIEW")
    print(f"Total Customers: {total_customers}")
    print(f"High Risk Customers: {high_risk} ({high_risk/total_customers*100:.1f}%)")
    print(f"Total MRR: ${total_mrr:,.0f}")
    
    # Support Metrics
    resolved_tickets = [t for t in tickets if t['status'] == 'Resolved']
    avg_resolution = sum(float(t['resolution_time_hours']) for t in resolved_tickets if t['resolution_time_hours']) / len(resolved_tickets)
    escalated = len([t for t in tickets if t['escalated'] == 'TRUE'])
    
    print(f"\nSUPPORT PERFORMANCE")
    print(f"Total Tickets: {len(tickets)}")
    print(f"Resolved: {len(resolved_tickets)} ({len(resolved_tickets)/len(tickets)*100:.1f}%)")
    print(f"Average Resolution Time: {avg_resolution:.1f} hours")
    print(f"Escalated: {escalated} ({escalated/len(tickets)*100:.1f}%)")
    
    # Security Metrics
    critical_incidents = len([i for i in incidents if i['severity'] == 'Critical'])
    false_positives = len([i for i in incidents if i['false_positive'] == 'TRUE'])
    
    print(f"\nSECURITY OPERATIONS")
    print(f"Total Incidents: {len(incidents)}")
    print(f"Critical Incidents: {critical_incidents} ({critical_incidents/len(incidents)*100:.1f}%)")
    print(f"False Positives: {false_positives} ({false_positives/len(incidents)*100:.1f}%)")
    
    # Customer Satisfaction
    nps_scores = [int(f['nps_score']) for f in feedback]
    avg_nps = sum(nps_scores) / len(nps_scores)
    high_renewal = len([f for f in feedback if f['likelihood_to_renew'] == 'High'])
    
    print(f"\nCUSTOMER SATISFACTION")
    print(f"Average NPS Score: {avg_nps:.1f}")
    print(f"High Renewal Likelihood: {high_renewal} ({high_renewal/len(feedback)*100:.1f}%)")
    
    # Key Insights
    print(f"\nKEY INSIGHTS")
    print(f"- {high_risk} customers at high risk representing potential churn")
    print(f"- {escalated} support tickets required escalation - review processes")
    print(f"- {critical_incidents} critical security incidents need attention")
    print(f"- {len(feedback) - high_renewal} customers have medium/low renewal likelihood")
    
    return {
        'total_customers': total_customers,
        'high_risk': high_risk,
        'total_mrr': total_mrr,
        'avg_nps': avg_nps
    }

if __name__ == "__main__":
    metrics = calculate_simple_metrics()