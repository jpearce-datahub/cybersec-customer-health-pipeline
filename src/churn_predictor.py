#!/usr/bin/env python3
"""Predictive churn modeling for customer health analytics."""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import duckdb
import joblib
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

class ChurnPredictor:
    def __init__(self, db_path='../data/cybersec_health_dbt.duckdb'):
        self.db_path = db_path
        self.model = None
        self.scaler = StandardScaler()
        
    def extract_features(self):
        """Extract features for churn prediction."""
        conn = duckdb.connect(self.db_path)
        
        query = """
        SELECT 
            c.customer_id,
            c.contract_value,
            c.contract_start_date,
            c.contract_end_date,
            
            -- Support metrics
            COUNT(st.ticket_id) as ticket_count,
            AVG(st.resolution_time_hours) as avg_resolution_time,
            AVG(st.satisfaction_score) as avg_satisfaction,
            
            -- Security metrics  
            COUNT(si.incident_id) as incident_count,
            AVG(si.severity_score) as avg_severity,
            
            -- Usage metrics
            AVG(pu.daily_active_users) as avg_daily_users,
            AVG(pu.feature_adoption_rate) as avg_adoption_rate,
            AVG(pu.license_utilization) as avg_license_util,
            
            -- Feedback metrics
            AVG(cf.nps_score) as avg_nps,
            AVG(cf.satisfaction_rating) as avg_rating,
            
            -- Contract metrics
            CASE WHEN c.contract_end_date <= CURRENT_DATE + INTERVAL '90 days' THEN 1 ELSE 0 END as renewal_soon,
            CASE WHEN c.contract_end_date <= CURRENT_DATE THEN 1 ELSE 0 END as churned
            
        FROM customers c
        LEFT JOIN support_tickets st ON c.customer_id = st.customer_id
        LEFT JOIN security_incidents si ON c.customer_id = si.customer_id  
        LEFT JOIN product_usage pu ON c.customer_id = pu.customer_id
        LEFT JOIN customer_feedback cf ON c.customer_id = cf.customer_id
        GROUP BY c.customer_id, c.contract_value, c.contract_start_date, c.contract_end_date
        """
        
        df = conn.execute(query).df()
        conn.close()
        
        # Fill missing values
        df = df.fillna(0)
        
        return df
    
    def train_model(self):
        """Train churn prediction model."""
        df = self.extract_features()
        
        # Features for training
        feature_cols = [
            'contract_value', 'ticket_count', 'avg_resolution_time', 'avg_satisfaction',
            'incident_count', 'avg_severity', 'avg_daily_users', 'avg_adoption_rate',
            'avg_license_util', 'avg_nps', 'avg_rating', 'renewal_soon'
        ]
        
        X = df[feature_cols]
        y = df['churned']
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Train model
        self.model = RandomForestClassifier(n_estimators=100, random_state=42)
        self.model.fit(X_scaled, y)
        
        # Save model
        joblib.dump(self.model, '../models/churn_model.pkl')
        joblib.dump(self.scaler, '../models/churn_scaler.pkl')
        
        print(f"Model trained on {len(df)} customers")
        return self.model
    
    def predict_churn(self):
        """Predict churn for all active customers."""
        # Load model if not trained
        if self.model is None:
            try:
                self.model = joblib.load('../models/churn_model.pkl')
                self.scaler = joblib.load('../models/churn_scaler.pkl')
            except:
                self.train_model()
        
        df = self.extract_features()
        active_customers = df[df['churned'] == 0]
        
        feature_cols = [
            'contract_value', 'ticket_count', 'avg_resolution_time', 'avg_satisfaction',
            'incident_count', 'avg_severity', 'avg_daily_users', 'avg_adoption_rate',
            'avg_license_util', 'avg_nps', 'avg_rating', 'renewal_soon'
        ]
        
        X = active_customers[feature_cols]
        X_scaled = self.scaler.transform(X)
        
        # Predict churn probability
        churn_probs = self.model.predict_proba(X_scaled)[:, 1]
        
        # Create results dataframe
        results = active_customers[['customer_id', 'contract_value']].copy()
        results['churn_probability'] = churn_probs
        results['risk_level'] = pd.cut(churn_probs, 
                                     bins=[0, 0.3, 0.7, 1.0], 
                                     labels=['Low', 'Medium', 'High'])
        
        return results.sort_values('churn_probability', ascending=False)

def main():
    predictor = ChurnPredictor()
    
    # Train model
    print("Training churn prediction model...")
    predictor.train_model()
    
    # Generate predictions
    print("Generating churn predictions...")
    predictions = predictor.predict_churn()
    
    # Save predictions
    predictions.to_csv('../data/processed/churn_predictions.csv', index=False)
    print(f"Predictions saved for {len(predictions)} customers")
    
    # Show high-risk customers
    high_risk = predictions[predictions['risk_level'] == 'High']
    print(f"\nHigh-risk customers: {len(high_risk)}")
    print(high_risk.head())

if __name__ == "__main__":
    main()