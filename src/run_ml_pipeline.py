#!/usr/bin/env python3
"""Run complete ML pipeline with churn prediction and alerting."""

from churn_predictor import ChurnPredictor
from alert_system import AlertSystem
import schedule
import time
from datetime import datetime

def run_daily_ml_pipeline():
    """Run the complete ML pipeline daily."""
    print(f"\n=== Running ML Pipeline - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
    
    # Step 1: Train/update churn model
    print("1. Training churn prediction model...")
    predictor = ChurnPredictor()
    predictor.train_model()
    
    # Step 2: Generate predictions
    print("2. Generating churn predictions...")
    predictions = predictor.predict_churn()
    predictions.to_csv('../data/processed/churn_predictions.csv', index=False)
    
    # Step 3: Run alert system
    print("3. Running alert system...")
    alert_system = AlertSystem()
    alert_system.run_alert_check()
    
    print("=== ML Pipeline Complete ===\n")

def run_scheduler():
    """Run scheduled ML pipeline."""
    # Schedule daily run at 8 AM
    schedule.every().day.at("08:00").do(run_daily_ml_pipeline)
    
    print("ML Pipeline Scheduler started...")
    print("Daily run scheduled for 8:00 AM")
    print("Press Ctrl+C to stop")
    
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--schedule":
        run_scheduler()
    else:
        run_daily_ml_pipeline()