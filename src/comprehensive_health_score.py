import pandas as pd
import numpy as np

def calculate_comprehensive_health_score():
    """Calculate comprehensive customer health score using 7 key metrics"""
    
    # Load data
    tickets = pd.read_csv('../data/raw/support_tickets.csv')
    incidents = pd.read_csv('../data/raw/security_incidents.csv')
    feedback = pd.read_csv('../data/raw/customer_feedback.csv')
    customers = pd.read_csv('../data/raw/customers.csv')
    
    # Aggregate by customer
    metrics = customers[['customer_id', 'company_name']].copy()
    
    # Support metrics
    support = tickets.groupby('customer_id').agg({
        'resolution_time_hours': 'mean',
        'ticket_id': 'count',
        'escalated': 'sum'
    })
    support['backlog'] = support['ticket_id'] - support['escalated']
    support['sla_adherence'] = 100 - (support['escalated'] / support['ticket_id'] * 100)
    
    # Security metrics  
    security = incidents.groupby('customer_id').size().reset_index(name='incident_volume')
    
    # Feedback metrics
    sentiment = feedback.groupby('customer_id')['nps_score'].mean()
    
    # Merge all metrics
    metrics = metrics.merge(support[['resolution_time_hours', 'backlog', 'sla_adherence']], on='customer_id', how='left')
    metrics = metrics.merge(security, on='customer_id', how='left')
    metrics = metrics.merge(sentiment.rename('sentiment'), on='customer_id', how='left')
    metrics = metrics.fillna(0)
    
    # Normalize metrics (0-100 scale)
    metrics['sentiment_norm'] = (metrics['sentiment'] + 10) * 5  # NPS -10 to 10 -> 0 to 100
    metrics['incident_norm'] = np.maximum(0, 100 - metrics['incident_volume'] * 10)
    metrics['resolution_norm'] = np.maximum(0, 100 - metrics['resolution_time_hours'] * 2)
    metrics['backlog_norm'] = np.maximum(0, 100 - metrics['backlog'] * 5)
    
    # Calculate comprehensive health score
    metrics['comprehensive_health_score'] = (
        metrics['sentiment_norm'] * 0.20 +           # Sentiment
        metrics['incident_norm'] * 0.15 +            # Incident volume  
        metrics['resolution_norm'] * 0.20 +          # Resolution time
        metrics['backlog_norm'] * 0.15 +             # Backlog
        metrics['sla_adherence'] * 0.30              # SLA adherence (includes ticket resolution time)
    ).clip(0, 100)
    
    return metrics[['customer_id', 'company_name', 'comprehensive_health_score']]

if __name__ == "__main__":
    result = calculate_comprehensive_health_score()
    print(result.head())