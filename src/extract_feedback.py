import pandas as pd

def extract_customer_feedback():
    """Extract customer feedback and satisfaction metrics"""
    df = pd.read_csv('../data/raw/customer_feedback.csv')
    df['survey_date'] = pd.to_datetime(df['survey_date'])
    
    # Calculate composite satisfaction score
    df['composite_satisfaction'] = (df['product_satisfaction'] + df['support_satisfaction']) / 2
    df['renewal_risk'] = df['likelihood_to_renew'].map({
        'High': 'Low Risk', 'Medium': 'Medium Risk', 'Low': 'High Risk'
    })
    
    return df

if __name__ == "__main__":
    feedback = extract_customer_feedback()
    print(f"Extracted {len(feedback)} feedback records")
    print(f"Average NPS: {feedback['nps_score'].mean():.2f}")