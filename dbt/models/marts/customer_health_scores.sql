with customer_metrics as (
    select 
        c.customer_id,
        c.customer_name,
        c.mrr,
        count(si.incident_id) as total_incidents,
        sum(case when si.severity = 'Critical' then 1 else 0 end) as critical_incidents,
        avg(si.resolution_hours) as avg_resolution_hours,
        sum(case when si.false_positive then 1 else 0 end) as false_positives
    from {{ ref('stg_customers') }} c
    left join {{ ref('stg_security_incidents') }} si on c.customer_id = si.customer_id
    group by c.customer_id, c.customer_name, c.mrr
)

select 
    customer_id,
    customer_name,
    mrr,
    total_incidents,
    critical_incidents,
    avg_resolution_hours,
    false_positives,
    case 
        when critical_incidents > 2 or avg_resolution_hours > 24 then 'High Risk'
        when critical_incidents > 0 or avg_resolution_hours > 12 then 'Medium Risk'
        else 'Low Risk'
    end as risk_level
from customer_metrics