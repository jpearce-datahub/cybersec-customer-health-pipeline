import pandas as pd
from datetime import datetime

def extract_support_tickets():
    """Extract support ticket data and calculate key metrics"""
    df = pd.read_csv('../data/raw/support_tickets.csv')
    df['created_date'] = pd.to_datetime(df['created_date'])
    df['resolved_date'] = pd.to_datetime(df['resolved_date'])
    
    # Calculate metrics
    df['is_resolved'] = df['resolved_date'].notna()
    df['days_to_resolve'] = (df['resolved_date'] - df['created_date']).dt.total_seconds() / 3600 / 24
    
    return df

if __name__ == "__main__":
    tickets = extract_support_tickets()
    print(f"Extracted {len(tickets)} support tickets")
    print(f"Average resolution time: {tickets['resolution_time_hours'].mean():.2f} hours")