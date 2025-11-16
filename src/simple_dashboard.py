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
    """Calculate basic customer health metrics with GitHub dark theme styling"""
    
    # Load data
    customers = load_csv('../data/raw/customers.csv')
    tickets = load_csv('../data/raw/support_tickets.csv')
    incidents = load_csv('../data/raw/security_incidents.csv')
    feedback = load_csv('../data/raw/customer_feedback.csv')
    
    # GitHub dark theme colors
    GREEN = '\033[38;2;35;134;54m'  # GitHub green
    WHITE = '\033[38;2;240;246;252m'  # GitHub light text
    GRAY = '\033[38;2;139;148;158m'  # GitHub muted text
    RED = '\033[38;2;248;81;73m'  # GitHub red
    RESET = '\033[0m'
    
    print(f"{GREEN}{'=' * 60}{RESET}")
    print(f"{WHITE}CYBERSECURITY CUSTOMER HEALTH DASHBOARD{RESET}")
    print(f"{GREEN}{'=' * 60}{RESET}")
    
    # Customer Overview
    total_customers = len(customers)
    high_risk = len([c for c in customers if c['risk_score'] == 'high'])
    total_mrr = sum(float(c['monthly_recurring_revenue']) for c in customers)
    
    print(f"\n{GREEN}CUSTOMER OVERVIEW{RESET}")
    print(f"{WHITE}Total Customers: {GREEN}{total_customers}{RESET}")
    print(f"{WHITE}High Risk Customers: {RED}{high_risk}{RESET} {GRAY}({high_risk/total_customers*100:.1f}%){RESET}")
    print(f"{WHITE}Total MRR: {GREEN}${total_mrr:,.0f}{RESET}")
    
    # Support Metrics
    resolved_tickets = [t for t in tickets if t['status'] == 'Resolved']
    avg_resolution = sum(float(t['resolution_time_hours']) for t in resolved_tickets if t['resolution_time_hours']) / len(resolved_tickets)
    escalated = len([t for t in tickets if t['escalated'] == 'TRUE'])
    
    print(f"\n{GREEN}SUPPORT PERFORMANCE{RESET}")
    print(f"{WHITE}Total Tickets: {GREEN}{len(tickets)}{RESET}")
    print(f"{WHITE}Resolved: {GREEN}{len(resolved_tickets)}{RESET} {GRAY}({len(resolved_tickets)/len(tickets)*100:.1f}%){RESET}")
    print(f"{WHITE}Average Resolution Time: {GREEN}{avg_resolution:.1f} hours{RESET}")
    print(f"{WHITE}Escalated: {RED}{escalated}{RESET} {GRAY}({escalated/len(tickets)*100:.1f}%){RESET}")
    
    # Security Metrics
    critical_incidents = len([i for i in incidents if i['severity'] == 'Critical'])
    false_positives = len([i for i in incidents if i['false_positive'] == 'TRUE'])
    
    print(f"\n{GREEN}SECURITY OPERATIONS{RESET}")
    print(f"{WHITE}Total Incidents: {GREEN}{len(incidents)}{RESET}")
    print(f"{WHITE}Critical Incidents: {RED}{critical_incidents}{RESET} {GRAY}({critical_incidents/len(incidents)*100:.1f}%){RESET}")
    print(f"{WHITE}False Positives: {RED}{false_positives}{RESET} {GRAY}({false_positives/len(incidents)*100:.1f}%){RESET}")
    
    # Customer Satisfaction
    nps_scores = [int(f['nps_score']) for f in feedback]
    avg_nps = sum(nps_scores) / len(nps_scores)
    high_renewal = len([f for f in feedback if f['likelihood_to_renew'] == 'High'])
    
    print(f"\n{GREEN}CUSTOMER SATISFACTION{RESET}")
    print(f"{WHITE}Average NPS Score: {GREEN}{avg_nps:.1f}{RESET}")
    print(f"{WHITE}High Renewal Likelihood: {GREEN}{high_renewal}{RESET} {GRAY}({high_renewal/len(feedback)*100:.1f}%){RESET}")
    
    # Key Insights
    print(f"\n{GREEN}KEY INSIGHTS{RESET}")
    print(f"{WHITE}- {RED}{high_risk}{RESET} {WHITE}customers at high risk representing potential churn{RESET}")
    print(f"{WHITE}- {RED}{escalated}{RESET} {WHITE}support tickets required escalation - review processes{RESET}")
    print(f"{WHITE}- {RED}{critical_incidents}{RESET} {WHITE}critical security incidents need attention{RESET}")
    print(f"{WHITE}- {RED}{len(feedback) - high_renewal}{RESET} {WHITE}customers have medium/low renewal likelihood{RESET}")
    
    return {
        'total_customers': total_customers,
        'high_risk': high_risk,
        'total_mrr': total_mrr,
        'avg_nps': avg_nps
    }

if __name__ == "__main__":
    metrics = calculate_simple_metrics()