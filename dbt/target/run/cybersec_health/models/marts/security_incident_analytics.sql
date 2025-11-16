
  
    
    

    create  table
      "cybersec_health_dbt"."main"."security_incident_analytics__dbt_tmp"
  
    as (
      with incident_metrics as (
    select
        customer_id,
        count(*) as total_incidents,
        count(case when severity_level = 'Critical' then 1 end) as critical_incidents,
        count(case when severity_level = 'High' then 1 end) as high_incidents,
        count(case when severity_level = 'Medium' then 1 end) as medium_incidents,
        count(case when severity_level = 'Low' then 1 end) as low_incidents,
        
        -- Attack type distribution
        count(case when attack_type = 'Malware' then 1 end) as malware_incidents,
        count(case when attack_type = 'DDoS' then 1 end) as ddos_incidents,
        count(case when attack_type = 'Intrusion' then 1 end) as intrusion_incidents,
        
        -- Response effectiveness
        count(case when response_category = 'Prevented' then 1 end) as prevented_incidents,
        count(case when response_category = 'Detected' then 1 end) as detected_incidents,
        count(case when response_category = 'Ignored' then 1 end) as ignored_incidents,
        
        -- Malware and alert metrics
        count(case when has_malware_indicators then 1 end) as incidents_with_malware,
        count(case when alert_triggered then 1 end) as incidents_with_alerts,
        
        -- Severity scoring
        avg(severity_score) as avg_severity_score,
        max(severity_score) as max_severity_score,
        avg(anomaly_score) as avg_anomaly_score,
        max(anomaly_score) as max_anomaly_score,
        
        -- Time-based metrics
        min(incident_timestamp) as first_incident_date,
        max(incident_timestamp) as latest_incident_date,
        count(case when incident_timestamp >= current_date - interval '30 days' then 1 end) as incidents_last_30_days,
        count(case when incident_timestamp >= current_date - interval '7 days' then 1 end) as incidents_last_7_days
        
    from "cybersec_health_dbt"."main"."stg_security_incidents"
    group by customer_id
),

risk_scoring as (
    select
        *,
        -- Calculate risk scores
        case 
            when critical_incidents > 0 then 'Critical'
            when high_incidents >= 3 or (high_incidents >= 1 and incidents_last_7_days >= 5) then 'High'
            when medium_incidents >= 5 or incidents_last_30_days >= 10 then 'Medium'
            else 'Low'
        end as security_risk_level,
        
        -- Prevention effectiveness ratio
        case 
            when total_incidents > 0 then 
                round((prevented_incidents::float / total_incidents::float) * 100, 2)
            else 0
        end as prevention_rate_pct,
        
        -- Alert coverage ratio
        case 
            when total_incidents > 0 then 
                round((incidents_with_alerts::float / total_incidents::float) * 100, 2)
            else 0
        end as alert_coverage_pct
        
    from incident_metrics
)

select
    customer_id,
    total_incidents,
    critical_incidents,
    high_incidents,
    medium_incidents,
    low_incidents,
    malware_incidents,
    ddos_incidents,
    intrusion_incidents,
    prevented_incidents,
    detected_incidents,
    ignored_incidents,
    incidents_with_malware,
    incidents_with_alerts,
    round(avg_severity_score, 2) as avg_severity_score,
    max_severity_score,
    round(avg_anomaly_score, 2) as avg_anomaly_score,
    round(max_anomaly_score, 2) as max_anomaly_score,
    first_incident_date,
    latest_incident_date,
    incidents_last_30_days,
    incidents_last_7_days,
    security_risk_level,
    prevention_rate_pct,
    alert_coverage_pct,
    
    -- Additional derived metrics
    case 
        when prevention_rate_pct >= 80 then 'Excellent'
        when prevention_rate_pct >= 60 then 'Good'
        when prevention_rate_pct >= 40 then 'Fair'
        else 'Poor'
    end as prevention_effectiveness,
    
    case 
        when alert_coverage_pct >= 90 then 'Excellent'
        when alert_coverage_pct >= 70 then 'Good'
        when alert_coverage_pct >= 50 then 'Fair'
        else 'Poor'
    end as alert_coverage_rating

from risk_scoring
order by 
    case security_risk_level
        when 'Critical' then 1
        when 'High' then 2
        when 'Medium' then 3
        when 'Low' then 4
    end,
    total_incidents desc
    );
  
  