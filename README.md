# cybersec-customer-health-pipeline

/src
  extract_tickets.py
  extract_incidents.py
  extract_feedback.py
  transform.py
  load.py
/aws_infra
  main.tf
/dbt
  models/
    staging/
    marts/
  dbt_project.yml
/data
  raw/
  processed/
/tests
  test_etl.py
/.github/workflows/ci.yml
README.md
