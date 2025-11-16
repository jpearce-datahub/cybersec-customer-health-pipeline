import pandas as pd
from datetime import datetime

def load_to_processed():
    """Load transformed data to processed directory"""
    from transform import calculate_customer_health_score
    
    # Calculate health scores
    health_data = calculate_customer_health_score()
    
    # Save to processed directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"../data/processed/customer_health_scores_{timestamp}.csv"
    
    health_data.to_csv(output_file, index=False)
    
    # Create latest version
    health_data.to_csv("../data/processed/customer_health_scores_latest.csv", index=False)
    
    print(f"Loaded {len(health_data)} records to {output_file}")
    
    # Summary statistics
    print("\nHealth Score Distribution:")
    print(health_data['health_category'].value_counts())
    print(f"\nAverage Health Score: {health_data['customer_health_score'].mean():.2f}")
    
    return health_data

if __name__ == "__main__":
    load_to_processed()