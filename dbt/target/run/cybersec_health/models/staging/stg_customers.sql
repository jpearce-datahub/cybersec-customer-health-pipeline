
  
  create view "cybersec_health_dbt"."main"."stg_customers__dbt_tmp" as (
    select
    customer_id,
    customer_name,
    industry,
    company_size,
    contract_start_date::date as contract_start_date,
    contract_end_date::date as contract_end_date,
    monthly_recurring_revenue::decimal(10,2) as mrr,
    account_manager
from read_csv_auto('../data/raw/customers.csv')
  );
