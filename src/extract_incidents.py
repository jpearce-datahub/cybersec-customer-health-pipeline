import pandas as pd

def extract_security_incidents():
    """Extract security incident data and calculate response metrics"""
    df = pd.read_csv('../data/raw/security_incidents.csv')
    df['detected_date'] = pd.to_datetime(df['detected_date'])
    df['resolved_date'] = pd.to_datetime(df['resolved_date'])
    
    # Calculate key security metrics
    df['is_resolved'] = df['resolved_date'].notna()
    df['total_response_time'] = df['mean_time_to_detect_minutes'] + df['mean_time_to_respond_minutes']
    df['severity_score'] = df['severity'].map({
        'Low': 1, 'Medium': 2, 'High': 3, 'Critical': 4
    })
    
    return df

if __name__ == "__main__":
    incidents = extract_security_incidents()
    print(f"Extracted {len(incidents)} security incidents")
    print(f"Average detection time: {incidents['mean_time_to_detect_minutes'].mean():.2f} minutes")