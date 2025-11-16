# Cybersecurity Customer Health Analytics Pipeline

A comprehensive data pipeline for tracking customer health analytics with interactive web dashboard and dbt transformations. This system transforms complex operational data into actionable insights for customer journey mapping, support optimization, and churn prevention.

## Features
- **Interactive Web Dashboard**: GitHub dark-themed webapp with customer-specific metrics
- **dbt Data Transformations**: Staging and mart models for advanced analytics
- **DBeaver Integration**: Dual database setup for raw and transformed data
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
- Average resolution time
- Escalation rate
- Critical incident rate
- Customer satisfaction
- Revenue at risk

## Project Structure

```
/src
  extract_tickets.py      # Support ticket data extraction
  extract_incidents.py    # Security incident data extraction  
  extract_feedback.py     # Customer feedback data extraction
  transform.py           # Customer health score calculation
  load.py               # Data loading and processing
  simple_dashboard.py   # Console dashboard with GitHub dark theme
  dashboard_insights.py # Executive dashboard insights
/webapp
  public/
    index.html          # Interactive web dashboard
  server.js             # Node.js API server
  package.json          # Dependencies
/data
  raw/                  # Source data files
    customers.csv
    support_tickets.csv
    security_incidents.csv
    product_usage.csv
    customer_feedback.csv
    contract_events.csv
  processed/            # Transformed data outputs
  cybersec_health_raw.duckdb    # Raw data database
  cybersec_health_dbt.duckdb    # dbt models database
/dbt
  models/             # dbt transformation models
    staging/          # Staging models
    marts/            # Business logic models
  profiles.yml        # dbt connection config
  dbt_project.yml
/aws_infra
  main.tf             # Terraform infrastructure
/tests
  test_etl.py        # Pipeline testing
setup_dbt_db.py      # Database setup script
/.github/workflows/ci.yml
```

## Quick Start

### Console Dashboard
1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Run console dashboard:
```bash
cd src
python simple_dashboard.py
```

### Web Dashboard
1. Install Node.js dependencies:
```bash
cd webapp
npm install
```

2. Start the web server:
```bash
node server.js
```

3. Open browser to `http://localhost:3000`

### dbt Setup
1. Install dbt:
```bash
pip install dbt-core dbt-duckdb
```

2. Setup databases:
```bash
python setup_dbt_db.py
```

3. Run dbt models:
```bash
cd dbt
dbt run --profiles-dir .
```

## Web Dashboard Features

- **GitHub Dark Theme**: Professional dark interface with green accents
- **Customer Selection**: Filter metrics by individual customer or view averages
- **Focus Areas**:
  - Customer Health Analysis: Sentiment, incident volume, health score
  - Support Efficiency: Resolution time, backlog
  - Incident Effectiveness: Response time, SLA adherence
- **Interactive Charts**: Customer journey, security analytics, threat intelligence
- **Clickable Metrics**: Detailed information modals for each metric

## DBeaver Integration

Connect to dual databases:
- **Raw Data**: `data/cybersec_health_raw.duckdb` - Direct CSV imports
- **dbt Models**: `data/cybersec_health_dbt.duckdb` - Transformed analytics tables

## Data Sources

The pipeline integrates data from:
- **Support Systems**: Ticket volume, resolution times, satisfaction scores
- **Security Operations**: Incident detection, response metrics, threat analysis
- **Product Analytics**: Feature usage, login frequency, license utilization
- **Customer Feedback**: NPS surveys, satisfaction scores, renewal intent
- **Business Systems**: Contract events, revenue data, account management

## Available dbt Models

### Staging Models
- `stg_customers` - Cleaned customer data
- `stg_security_incidents` - Enhanced security incident data

### Mart Models
- `security_attack_patterns` - Attack pattern analysis
- `security_incident_analytics` - Incident metrics and KPIs
- `security_ip_analysis` - IP threat analysis
- `security_predictive_analytics` - Predictive modeling

## Next Steps

- Implement real-time data ingestion
- Add predictive churn modeling
- Create automated alerting for at-risk customers
- Integrate with CRM and customer success platforms
- Expand dbt model coverage
