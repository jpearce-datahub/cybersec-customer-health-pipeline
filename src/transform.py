import pandas as pd
import numpy as np

def calculate_customer_health_score():
    """Calculate comprehensive customer health score"""
    
    # Load all data
    customers = pd.read_csv('../data/raw/customers.csv')
    tickets = pd.read_csv('../data/raw/support_tickets.csv')
    incidents = pd.read_csv('../data/raw/security_incidents.csv')
    usage = pd.read_csv('../data/raw/product_usage.csv')
    feedback = pd.read_csv('../data/raw/customer_feedback.csv')
    
    # Aggregate metrics by customer
    ticket_metrics = tickets.groupby('customer_id').agg({
        'ticket_id': 'count',
        'resolution_time_hours': 'mean',
        'satisfaction_score': 'mean',
        'escalated': 'sum'
    }).rename(columns={'ticket_id': 'total_tickets'})
    
    incident_metrics = incidents.groupby('customer_id').agg({
        'incident_id': 'count',
        'mean_time_to_detect_minutes': 'mean',
        'mean_time_to_respond_minutes': 'mean',
        'false_positive': 'sum'
    }).rename(columns={'incident_id': 'total_incidents'})
    
    # Merge all metrics
    health_df = customers.merge(ticket_metrics, on='customer_id', how='left')
    health_df = health_df.merge(incident_metrics, on='customer_id', how='left')
    health_df = health_df.merge(usage[['customer_id', 'feature_adoption_score', 'license_utilization_pct']], on='customer_id', how='left')
    health_df = health_df.merge(feedback[['customer_id', 'nps_score', 'likelihood_to_renew']], on='customer_id', how='left')
    
    # Fill NAs
    health_df = health_df.fillna(0)
    
    # Calculate health score (0-100)
    health_df['usage_score'] = (health_df['feature_adoption_score'] * 50 + health_df['license_utilization_pct'] * 0.5)
    health_df['support_score'] = np.where(health_df['total_tickets'] > 0, 
                                         (health_df['satisfaction_score'] * 20) - (health_df['escalated'] * 5), 100)
    health_df['security_score'] = np.where(health_df['total_incidents'] > 0,
                                          100 - (health_df['total_incidents'] * 5) - (health_df['false_positive'] * 2), 100)
    health_df['satisfaction_score_norm'] = health_df['nps_score'] * 10
    
    # Composite health score
    health_df['customer_health_score'] = (
        health_df['usage_score'] * 0.3 +
        health_df['support_score'] * 0.25 +
        health_df['security_score'] * 0.25 +
        health_df['satisfaction_score_norm'] * 0.2
    ).clip(0, 100)
    
    # Health categories
    health_df['health_category'] = pd.cut(health_df['customer_health_score'], 
                                         bins=[0, 40, 70, 100], 
                                         labels=['At Risk', 'Healthy', 'Champion'])
    
    return health_df

if __name__ == "__main__":
    health_data = calculate_customer_health_score()
    print(f"Calculated health scores for {len(health_data)} customers")
    print(health_data[['customer_id', 'company_name', 'customer_health_score', 'health_category']].head())