with daily_incidents as (
    select
        date_trunc('day', incident_timestamp) as incident_date,
        customer_id,
        attack_type,
        severity_level,
        response_category,
        count(*) as incident_count,
        avg(severity_score) as avg_severity_score,
        avg(anomaly_score) as avg_anomaly_score,
        count(case when has_malware_indicators then 1 end) as malware_count,
        count(case when alert_triggered then 1 end) as alert_count
    from {{ ref('stg_security_incidents') }}
    group by 1, 2, 3, 4, 5
),

daily_summary as (
    select
        incident_date,
        customer_id,
        sum(incident_count) as total_daily_incidents,
        sum(case when severity_level = 'Critical' then incident_count else 0 end) as critical_count,
        sum(case when severity_level = 'High' then incident_count else 0 end) as high_count,
        sum(case when severity_level = 'Medium' then incident_count else 0 end) as medium_count,
        sum(case when severity_level = 'Low' then incident_count else 0 end) as low_count,
        
        sum(case when attack_type = 'Malware' then incident_count else 0 end) as malware_incidents,
        sum(case when attack_type = 'DDoS' then incident_count else 0 end) as ddos_incidents,
        sum(case when attack_type = 'Intrusion' then incident_count else 0 end) as intrusion_incidents,
        
        sum(case when response_category = 'Prevented' then incident_count else 0 end) as prevented_count,
        sum(case when response_category = 'Detected' then incident_count else 0 end) as detected_count,
        sum(case when response_category = 'Ignored' then incident_count else 0 end) as ignored_count,
        
        sum(malware_count) as total_malware_indicators,
        sum(alert_count) as total_alerts,
        
        avg(avg_severity_score) as daily_avg_severity,
        avg(avg_anomaly_score) as daily_avg_anomaly
        
    from daily_incidents
    group by 1, 2
)

select
    incident_date,
    customer_id,
    total_daily_incidents,
    critical_count,
    high_count,
    medium_count,
    low_count,
    malware_incidents,
    ddos_incidents,
    intrusion_incidents,
    prevented_count,
    detected_count,
    ignored_count,
    total_malware_indicators,
    total_alerts,
    round(daily_avg_severity, 2) as daily_avg_severity,
    round(daily_avg_anomaly, 2) as daily_avg_anomaly,
    
    -- Calculate prevention rate for the day
    case 
        when total_daily_incidents > 0 then 
            round((prevented_count::float / total_daily_incidents::float) * 100, 2)
        else 0
    end as daily_prevention_rate_pct,
    
    -- Risk level for the day
    case 
        when critical_count > 0 then 'Critical'
        when high_count >= 2 then 'High'
        when high_count >= 1 or medium_count >= 3 then 'Medium'
        else 'Low'
    end as daily_risk_level,
    
    -- Moving averages (7-day window)
    avg(total_daily_incidents) over (
        partition by customer_id 
        order by incident_date 
        rows between 6 preceding and current row
    ) as incidents_7day_avg,
    
    avg(daily_avg_severity) over (
        partition by customer_id 
        order by incident_date 
        rows between 6 preceding and current row
    ) as severity_7day_avg

from daily_summary
order by customer_id, incident_date desc