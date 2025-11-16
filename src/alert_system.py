#!/usr/bin/env python3
"""Automated alerting system for at-risk customers."""

import pandas as pd
import smtplib
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
import duckdb
from churn_predictor import ChurnPredictor

class AlertSystem:
    def __init__(self, config_path='../config/alert_config.json'):
        self.config = self.load_config(config_path)
        self.predictor = ChurnPredictor()
        
    def load_config(self, config_path):
        """Load alert configuration."""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            # Default configuration
            return {
                "email": {
                    "smtp_server": "smtp.gmail.com",
                    "smtp_port": 587,
                    "sender_email": "alerts@company.com",
                    "sender_password": "your_password",
                    "recipients": ["customer-success@company.com", "sales@company.com"]
                },
                "thresholds": {
                    "high_churn_probability": 0.7,
                    "medium_churn_probability": 0.3,
                    "high_value_customer": 50000,
                    "critical_incidents": 5,
                    "low_satisfaction": 3.0
                },
                "alert_frequency": "daily"
            }
    
    def get_at_risk_customers(self):
        """Identify at-risk customers based on multiple criteria."""
        # Get churn predictions
        predictions = self.predictor.predict_churn()
        
        # Get additional risk factors
        conn = duckdb.connect(self.predictor.db_path)
        
        risk_query = """
        SELECT 
            c.customer_id,
            c.customer_name,
            c.contract_value,
            c.contract_end_date,
            
            -- Recent support issues
            COUNT(CASE WHEN st.created_date >= CURRENT_DATE - INTERVAL '30 days' THEN 1 END) as recent_tickets,
            AVG(CASE WHEN st.created_date >= CURRENT_DATE - INTERVAL '30 days' THEN st.satisfaction_score END) as recent_satisfaction,
            
            -- Recent security incidents
            COUNT(CASE WHEN si.incident_date >= CURRENT_DATE - INTERVAL '30 days' THEN 1 END) as recent_incidents,
            
            -- Usage trends
            AVG(CASE WHEN pu.date >= CURRENT_DATE - INTERVAL '30 days' THEN pu.daily_active_users END) as recent_usage,
            AVG(CASE WHEN pu.date >= CURRENT_DATE - INTERVAL '90 days' AND pu.date < CURRENT_DATE - INTERVAL '30 days' 
                     THEN pu.daily_active_users END) as previous_usage
            
        FROM customers c
        LEFT JOIN support_tickets st ON c.customer_id = st.customer_id
        LEFT JOIN security_incidents si ON c.customer_id = si.customer_id
        LEFT JOIN product_usage pu ON c.customer_id = pu.customer_id
        WHERE c.contract_end_date > CURRENT_DATE
        GROUP BY c.customer_id, c.customer_name, c.contract_value, c.contract_end_date
        """
        
        risk_df = conn.execute(risk_query).df()
        conn.close()
        
        # Merge with predictions
        at_risk = predictions.merge(risk_df, on='customer_id', how='left')
        
        # Calculate usage trend
        at_risk['usage_trend'] = (at_risk['recent_usage'] - at_risk['previous_usage']) / at_risk['previous_usage'].fillna(1)
        
        return at_risk
    
    def generate_alerts(self):
        """Generate alerts for different risk categories."""
        at_risk = self.get_at_risk_customers()
        alerts = []
        
        # High churn probability alerts
        high_churn = at_risk[at_risk['churn_probability'] >= self.config['thresholds']['high_churn_probability']]
        for _, customer in high_churn.iterrows():
            alerts.append({
                'type': 'HIGH_CHURN_RISK',
                'priority': 'HIGH',
                'customer_id': customer['customer_id'],
                'customer_name': customer['customer_name'],
                'contract_value': customer['contract_value'],
                'churn_probability': customer['churn_probability'],
                'message': f"Customer {customer['customer_name']} has {customer['churn_probability']:.1%} churn probability"
            })
        
        # High-value customer at risk
        high_value_risk = at_risk[
            (at_risk['contract_value'] >= self.config['thresholds']['high_value_customer']) &
            (at_risk['churn_probability'] >= self.config['thresholds']['medium_churn_probability'])
        ]
        for _, customer in high_value_risk.iterrows():
            alerts.append({
                'type': 'HIGH_VALUE_AT_RISK',
                'priority': 'CRITICAL',
                'customer_id': customer['customer_id'],
                'customer_name': customer['customer_name'],
                'contract_value': customer['contract_value'],
                'churn_probability': customer['churn_probability'],
                'message': f"High-value customer {customer['customer_name']} (${customer['contract_value']:,.0f}) at risk"
            })
        
        # Critical incidents alert
        critical_incidents = at_risk[at_risk['recent_incidents'] >= self.config['thresholds']['critical_incidents']]
        for _, customer in critical_incidents.iterrows():
            alerts.append({
                'type': 'CRITICAL_INCIDENTS',
                'priority': 'HIGH',
                'customer_id': customer['customer_id'],
                'customer_name': customer['customer_name'],
                'recent_incidents': customer['recent_incidents'],
                'message': f"Customer {customer['customer_name']} has {customer['recent_incidents']} incidents in 30 days"
            })
        
        # Low satisfaction alert
        low_satisfaction = at_risk[
            (at_risk['recent_satisfaction'] <= self.config['thresholds']['low_satisfaction']) &
            (at_risk['recent_satisfaction'].notna())
        ]
        for _, customer in low_satisfaction.iterrows():
            alerts.append({
                'type': 'LOW_SATISFACTION',
                'priority': 'MEDIUM',
                'customer_id': customer['customer_id'],
                'customer_name': customer['customer_name'],
                'recent_satisfaction': customer['recent_satisfaction'],
                'message': f"Customer {customer['customer_name']} satisfaction dropped to {customer['recent_satisfaction']:.1f}"
            })
        
        # Usage decline alert
        usage_decline = at_risk[at_risk['usage_trend'] <= -0.3]  # 30% decline
        for _, customer in usage_decline.iterrows():
            alerts.append({
                'type': 'USAGE_DECLINE',
                'priority': 'MEDIUM',
                'customer_id': customer['customer_id'],
                'customer_name': customer['customer_name'],
                'usage_trend': customer['usage_trend'],
                'message': f"Customer {customer['customer_name']} usage declined {customer['usage_trend']:.1%}"
            })
        
        return alerts
    
    def send_email_alert(self, alerts):
        """Send email alerts to configured recipients."""
        if not alerts:
            return
        
        # Group alerts by priority
        critical_alerts = [a for a in alerts if a['priority'] == 'CRITICAL']
        high_alerts = [a for a in alerts if a['priority'] == 'HIGH']
        medium_alerts = [a for a in alerts if a['priority'] == 'MEDIUM']
        
        # Create email content
        subject = f"Customer Risk Alert - {len(alerts)} alerts ({len(critical_alerts)} critical)"
        
        body = f"""
        <html>
        <body>
        <h2>Customer Risk Alert Summary</h2>
        <p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        
        <h3>🚨 Critical Alerts ({len(critical_alerts)})</h3>
        <ul>
        """
        
        for alert in critical_alerts:
            body += f"<li><strong>{alert['customer_name']}</strong>: {alert['message']}</li>"
        
        body += f"""
        </ul>
        
        <h3>⚠️ High Priority Alerts ({len(high_alerts)})</h3>
        <ul>
        """
        
        for alert in high_alerts:
            body += f"<li><strong>{alert['customer_name']}</strong>: {alert['message']}</li>"
        
        body += f"""
        </ul>
        
        <h3>📊 Medium Priority Alerts ({len(medium_alerts)})</h3>
        <ul>
        """
        
        for alert in medium_alerts:
            body += f"<li><strong>{alert['customer_name']}</strong>: {alert['message']}</li>"
        
        body += """
        </ul>
        
        <p><em>Please review these customers and take appropriate action.</em></p>
        </body>
        </html>
        """
        
        # Send email
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.config['email']['sender_email']
            msg['To'] = ', '.join(self.config['email']['recipients'])
            
            msg.attach(MIMEText(body, 'html'))
            
            server = smtplib.SMTP(self.config['email']['smtp_server'], self.config['email']['smtp_port'])
            server.starttls()
            server.login(self.config['email']['sender_email'], self.config['email']['sender_password'])
            server.send_message(msg)
            server.quit()
            
            print(f"Email alert sent to {len(self.config['email']['recipients'])} recipients")
            
        except Exception as e:
            print(f"Failed to send email: {e}")
    
    def save_alerts(self, alerts):
        """Save alerts to file for tracking."""
        if alerts:
            df = pd.DataFrame(alerts)
            df['timestamp'] = datetime.now()
            df.to_csv('../data/processed/alerts.csv', mode='a', header=False, index=False)
            print(f"Saved {len(alerts)} alerts to file")
    
    def run_alert_check(self):
        """Run complete alert check process."""
        print("Running customer risk alert check...")
        
        # Generate alerts
        alerts = self.generate_alerts()
        
        if alerts:
            print(f"Generated {len(alerts)} alerts")
            
            # Save alerts
            self.save_alerts(alerts)
            
            # Send email (comment out if no email config)
            # self.send_email_alert(alerts)
            
            # Print summary
            for alert in alerts:
                print(f"[{alert['priority']}] {alert['type']}: {alert['message']}")
        else:
            print("No alerts generated - all customers healthy!")

def main():
    alert_system = AlertSystem()
    alert_system.run_alert_check()

if __name__ == "__main__":
    main()