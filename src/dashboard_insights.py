import pandas as pd
import numpy as np

def generate_executive_dashboard():
    """Generate key insights for executive dashboard"""
    
    # Load processed data
    try:
        health_data = pd.read_csv('../data/processed/customer_health_scores_latest.csv')
    except:
        from load import load_to_processed
        health_data = load_to_processed()
    
    insights = {}
    
    # Customer Health Overview
    insights['total_customers'] = len(health_data)
    insights['avg_health_score'] = health_data['customer_health_score'].mean()
    insights['at_risk_customers'] = len(health_data[health_data['health_category'] == 'At Risk'])
    insights['champion_customers'] = len(health_data[health_data['health_category'] == 'Champion'])
    
    # Revenue at Risk
    at_risk_revenue = health_data[health_data['health_category'] == 'At Risk']['monthly_recurring_revenue'].sum()
    total_revenue = health_data['monthly_recurring_revenue'].sum()
    insights['revenue_at_risk'] = at_risk_revenue
    insights['revenue_at_risk_pct'] = (at_risk_revenue / total_revenue) * 100
    
    # Support Metrics
    tickets_df = pd.read_csv('../data/raw/support_tickets.csv')
    insights['avg_resolution_time'] = tickets_df['resolution_time_hours'].mean()
    insights['escalation_rate'] = (tickets_df['escalated'].sum() / len(tickets_df)) * 100
    insights['avg_satisfaction'] = tickets_df['satisfaction_score'].mean()
    
    # Security Metrics
    incidents_df = pd.read_csv('../data/raw/security_incidents.csv')
    insights['avg_detection_time'] = incidents_df['mean_time_to_detect_minutes'].mean()
    insights['avg_response_time'] = incidents_df['mean_time_to_respond_minutes'].mean()
    insights['false_positive_rate'] = (incidents_df['false_positive'].sum() / len(incidents_df)) * 100
    
    # Product Usage
    usage_df = pd.read_csv('../data/raw/product_usage.csv')
    insights['avg_feature_adoption'] = usage_df['feature_adoption_score'].mean()
    insights['avg_license_utilization'] = usage_df['license_utilization_pct'].mean()
    
    return insights

def print_dashboard(insights):
    """Print formatted dashboard with GitHub dark theme colors"""
    # GitHub dark theme colors
    GREEN = '\033[38;2;35;134;54m'  # GitHub green
    WHITE = '\033[38;2;240;246;252m'  # GitHub light text
    GRAY = '\033[38;2;139;148;158m'  # GitHub muted text
    RED = '\033[38;2;248;81;73m'  # GitHub red
    RESET = '\033[0m'
    
    print(f"{GREEN}{'=' * 60}{RESET}")
    print(f"{WHITE}CYBERSECURITY CUSTOMER HEALTH DASHBOARD{RESET}")
    print(f"{GREEN}{'=' * 60}{RESET}")
    
    print(f"\n{GREEN}📊 CUSTOMER HEALTH OVERVIEW{RESET}")
    print(f"{WHITE}Total Customers: {GREEN}{insights['total_customers']}{RESET}")
    print(f"{WHITE}Average Health Score: {GREEN}{insights['avg_health_score']:.1f}/100{RESET}")
    print(f"{WHITE}At Risk Customers: {RED}{insights['at_risk_customers']}{RESET} {GRAY}({insights['at_risk_customers']/insights['total_customers']*100:.1f}%){RESET}")
    print(f"{WHITE}Champion Customers: {GREEN}{insights['champion_customers']}{RESET} {GRAY}({insights['champion_customers']/insights['total_customers']*100:.1f}%){RESET}")
    
    print(f"\n{GREEN}💰 REVENUE METRICS{RESET}")
    print(f"{WHITE}Revenue at Risk: {RED}${insights['revenue_at_risk']:,}{RESET} {GRAY}({insights['revenue_at_risk_pct']:.1f}%){RESET}")
    
    print(f"\n{GREEN}🎧 SUPPORT PERFORMANCE{RESET}")
    print(f"{WHITE}Average Resolution Time: {GREEN}{insights['avg_resolution_time']:.1f} hours{RESET}")
    print(f"{WHITE}Escalation Rate: {RED}{insights['escalation_rate']:.1f}%{RESET}")
    print(f"{WHITE}Average Satisfaction: {GREEN}{insights['avg_satisfaction']:.1f}/5{RESET}")
    
    print(f"\n{GREEN}🔒 SECURITY OPERATIONS{RESET}")
    print(f"{WHITE}Average Detection Time: {GREEN}{insights['avg_detection_time']:.1f} minutes{RESET}")
    print(f"{WHITE}Average Response Time: {GREEN}{insights['avg_response_time']:.1f} minutes{RESET}")
    print(f"{WHITE}False Positive Rate: {RED}{insights['false_positive_rate']:.1f}%{RESET}")
    
    print(f"\n{GREEN}📈 PRODUCT ADOPTION{RESET}")
    print(f"{WHITE}Average Feature Adoption: {GREEN}{insights['avg_feature_adoption']:.1f}%{RESET}")
    print(f"{WHITE}Average License Utilization: {GREEN}{insights['avg_license_utilization']:.1f}%{RESET}")

if __name__ == "__main__":
    insights = generate_executive_dashboard()
    print_dashboard(insights)