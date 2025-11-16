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
    """Print formatted dashboard"""
    print("=" * 60)
    print("CYBERSECURITY CUSTOMER HEALTH DASHBOARD")
    print("=" * 60)
    
    print(f"\n📊 CUSTOMER HEALTH OVERVIEW")
    print(f"Total Customers: {insights['total_customers']}")
    print(f"Average Health Score: {insights['avg_health_score']:.1f}/100")
    print(f"At Risk Customers: {insights['at_risk_customers']} ({insights['at_risk_customers']/insights['total_customers']*100:.1f}%)")
    print(f"Champion Customers: {insights['champion_customers']} ({insights['champion_customers']/insights['total_customers']*100:.1f}%)")
    
    print(f"\n💰 REVENUE METRICS")
    print(f"Revenue at Risk: ${insights['revenue_at_risk']:,} ({insights['revenue_at_risk_pct']:.1f}%)")
    
    print(f"\n🎧 SUPPORT PERFORMANCE")
    print(f"Average Resolution Time: {insights['avg_resolution_time']:.1f} hours")
    print(f"Escalation Rate: {insights['escalation_rate']:.1f}%")
    print(f"Average Satisfaction: {insights['avg_satisfaction']:.1f}/5")
    
    print(f"\n🔒 SECURITY OPERATIONS")
    print(f"Average Detection Time: {insights['avg_detection_time']:.1f} minutes")
    print(f"Average Response Time: {insights['avg_response_time']:.1f} minutes")
    print(f"False Positive Rate: {insights['false_positive_rate']:.1f}%")
    
    print(f"\n📈 PRODUCT ADOPTION")
    print(f"Average Feature Adoption: {insights['avg_feature_adoption']:.1f}%")
    print(f"Average License Utilization: {insights['avg_license_utilization']:.1f}%")

if __name__ == "__main__":
    insights = generate_executive_dashboard()
    print_dashboard(insights)