-- Test for data freshness
select 
    'Data Freshness Check' as test_name,
    case 
        when max(incident_timestamp) >= current_date - interval '7 days' then 'PASS'
        else 'FAIL'
    end as test_result,
    max(incident_timestamp) as latest_incident,
    current_date - interval '7 days' as expected_threshold
from {{ ref('stg_security_incidents') }}

union all

-- Test for missing critical fields
select 
    'Missing Critical Fields' as test_name,
    case 
        when count(*) = 0 then 'PASS'
        else 'FAIL'
    end as test_result,
    count(*) as records_with_missing_fields,
    null as expected_threshold
from {{ ref('stg_security_incidents') }}
where customer_id is null 
   or attack_type is null 
   or severity_level is null

union all

-- Test for anomaly score range
select 
    'Anomaly Score Range' as test_name,
    case 
        when count(*) = 0 then 'PASS'
        else 'FAIL'
    end as test_result,
    count(*) as records_out_of_range,
    null as expected_threshold
from {{ ref('stg_security_incidents') }}
where anomaly_score < 0 or anomaly_score > 100

union all

-- Test for duplicate incidents
select 
    'Duplicate Incidents' as test_name,
    case 
        when count(*) = 0 then 'PASS'
        else 'FAIL'
    end as test_result,
    count(*) as duplicate_count,
    null as expected_threshold
from (
    select 
        customer_id, 
        incident_timestamp, 
        source_ip, 
        attack_type,
        count(*) as cnt
    from {{ ref('stg_security_incidents') }}
    group by customer_id, incident_timestamp, source_ip, attack_type
    having count(*) > 1
) duplicates