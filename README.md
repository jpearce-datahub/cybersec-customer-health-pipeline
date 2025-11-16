# Cybersecurity Customer Health Analytics Pipeline

A comprehensive data pipeline for tracking customer health analytics at a managed cybersecurity solutions provider. This system transforms complex operational data into actionable insights for customer journey mapping, support optimization, and churn prevention.

## Overview

As a principal data analyst/engineer, this pipeline provides:
- **Customer Health Scoring**: Composite metrics combining usage, support, security, and satisfaction data
- **Support Operations Analytics**: Ticket resolution times, escalation rates, and satisfaction tracking
- **Security Operations Metrics**: Incident response times, detection efficiency, and false positive rates
- **Customer Journey Insights**: Renewal likelihood, churn risk, and expansion opportunities

## Key Metrics Tracked

### Customer Health Indicators
- Monthly Recurring Revenue (MRR) at risk
- Feature adoption and license utilization
- Support ticket volume and satisfaction
- Security incident frequency and response
- Net Promoter Score (NPS) and renewal likelihood

### Operational KPIs
- Average resolution time: 16.7 hours
- Escalation rate: 40%
- Critical incident rate: 20%
- Customer satisfaction: 7.2/10 NPS
- Revenue at risk: 20% of customers

## Project Structure

```
/src
  extract_tickets.py      # Support ticket data extraction
  extract_incidents.py    # Security incident data extraction  
  extract_feedback.py     # Customer feedback data extraction
  transform.py           # Customer health score calculation
  load.py               # Data loading and processing
  simple_dashboard.py   # Executive dashboard insights
/data
  raw/                  # Source data files
    customers.csv
    support_tickets.csv
    security_incidents.csv
    product_usage.csv
    customer_feedback.csv
    contract_events.csv
  processed/           # Transformed data outputs
/aws_infra
  main.tf             # Terraform infrastructure
/dbt
  models/             # dbt transformation models
    staging/
    marts/
  dbt_project.yml
/tests
  test_etl.py        # Pipeline testing
/.github/workflows/ci.yml
```

## Quick Start

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the dashboard:
```bash
cd src
python simple_dashboard.py
```

## Sample Output

```
CYBERSECURITY CUSTOMER HEALTH DASHBOARD

CUSTOMER OVERVIEW
Total Customers: 10
High Risk Customers: 2 (20.0%)
Total MRR: $246,000

SUPPORT PERFORMANCE  
Total Tickets: 10
Resolved: 8 (80.0%)
Average Resolution Time: 16.7 hours
Escalated: 4 (40.0%)

SECURITY OPERATIONS
Total Incidents: 10
Critical Incidents: 2 (20.0%)
False Positives: 2 (20.0%)

CUSTOMER SATISFACTION
Average NPS Score: 7.2
High Renewal Likelihood: 6 (60.0%)
```

## Data Sources

The pipeline integrates data from:
- **Support Systems**: Ticket volume, resolution times, satisfaction scores
- **Security Operations**: Incident detection, response metrics, threat analysis
- **Product Analytics**: Feature usage, login frequency, license utilization
- **Customer Feedback**: NPS surveys, satisfaction scores, renewal intent
- **Business Systems**: Contract events, revenue data, account management

## Next Steps

- Implement real-time data ingestion
- Add predictive churn modeling
- Create automated alerting for at-risk customers
- Build interactive dashboards with visualization tools
- Integrate with CRM and customer success platforms
