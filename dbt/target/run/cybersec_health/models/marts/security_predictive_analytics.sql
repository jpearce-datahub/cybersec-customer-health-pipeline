
  
    
    

    create  table
      "cybersec_health_dbt"."main"."security_predictive_analytics__dbt_tmp"
  
    as (
      with customer_history as (
    select
        customer_id,
        extract(hour from incident_timestamp) as incident_hour,
        extract(dow from incident_timestamp) as incident_dow,
        attack_type,
        severity_score,
        anomaly_score,
        network_segment,
        
        -- Time-based features
        row_number() over (partition by customer_id order by incident_timestamp desc) as recency_rank,
        count(*) over (partition by customer_id) as total_incidents,
        avg(severity_score) over (partition by customer_id) as avg_customer_severity,
        
        -- Sequence features
        lag(attack_type) over (partition by customer_id order by incident_timestamp) as prev_attack,
        lag(severity_score) over (partition by customer_id order by incident_timestamp) as prev_severity,
        
        -- Time gaps
        case 
            when lag(incident_timestamp) over (partition by customer_id order by incident_timestamp) is not null then
                extract(epoch from (incident_timestamp - lag(incident_timestamp) over (partition by customer_id order by incident_timestamp))) / 3600.0
            else null
        end as hours_since_last
        
    from "cybersec_health_dbt"."main"."stg_security_incidents"
),

risk_patterns as (
    select
        customer_id,
        
        -- Behavioral patterns
        mode() within group (order by incident_hour) as peak_attack_hour,
        mode() within group (order by incident_dow) as peak_attack_day,
        mode() within group (order by attack_type) as most_common_attack,
        
        -- Risk indicators
        avg(severity_score) as avg_severity,
        max(severity_score) as max_severity,
        avg(anomaly_score) as avg_anomaly,
        stddev(anomaly_score) as anomaly_variance,
        
        -- Frequency patterns
        count(*) as incident_count,
        avg(hours_since_last) as avg_time_between_incidents,
        min(hours_since_last) as min_time_between_incidents,
        
        -- Escalation patterns
        count(case when prev_severity < severity_score then 1 end) as escalation_count,
        count(case when prev_attack != attack_type then 1 end) as attack_type_changes,
        
        -- Recent activity
        count(case when recency_rank <= 5 then 1 end) as recent_incidents,
        avg(case when recency_rank <= 5 then severity_score end) as recent_avg_severity
        
    from customer_history
    group by customer_id
),

predictive_scores as (
    select
        *,
        
        -- Next attack likelihood (0-100)
        least(100, greatest(0, 
            case 
                when avg_time_between_incidents <= 24 then 85
                when avg_time_between_incidents <= 72 then 65
                when avg_time_between_incidents <= 168 then 45
                else 25
            end +
            case when recent_avg_severity >= 3 then 15 else 0 end +
            case when escalation_count > 0 then 10 else 0 end +
            case when anomaly_variance > 20 then 10 else 0 end
        )) as next_attack_probability,
        
        -- Severity prediction
        case 
            when recent_avg_severity >= 3.5 then 'Critical'
            when recent_avg_severity >= 2.5 then 'High'
            when recent_avg_severity >= 1.5 then 'Medium'
            else 'Low'
        end as predicted_next_severity,
        
        -- Time to next incident (hours)
        case 
            when avg_time_between_incidents is not null then
                round(avg_time_between_incidents * 
                    case 
                        when recent_incidents >= 3 then 0.7  -- More frequent if recent activity
                        when escalation_count > 0 then 0.8
                        else 1.0
                    end, 1)
            else null
        end as predicted_hours_to_next,
        
        -- Risk classification
        case 
            when next_attack_probability >= 80 then 'Imminent'
            when next_attack_probability >= 60 then 'High Risk'
            when next_attack_probability >= 40 then 'Medium Risk'
            else 'Low Risk'
        end as risk_classification
        
    from risk_patterns
)

select
    customer_id,
    peak_attack_hour,
    peak_attack_day,
    most_common_attack,
    round(avg_severity, 2) as avg_severity,
    max_severity,
    round(avg_anomaly, 2) as avg_anomaly,
    round(anomaly_variance, 2) as anomaly_variance,
    incident_count,
    round(avg_time_between_incidents, 1) as avg_time_between_incidents,
    round(min_time_between_incidents, 1) as min_time_between_incidents,
    escalation_count,
    attack_type_changes,
    recent_incidents,
    round(recent_avg_severity, 2) as recent_avg_severity,
    next_attack_probability,
    predicted_next_severity,
    predicted_hours_to_next,
    risk_classification,
    
    -- Recommended actions
    case 
        when risk_classification = 'Imminent' then 'Immediate monitoring and preventive measures'
        when risk_classification = 'High Risk' then 'Enhanced monitoring and security controls'
        when risk_classification = 'Medium Risk' then 'Regular monitoring and review'
        else 'Standard monitoring'
    end as recommended_action,
    
    current_timestamp as prediction_timestamp

from predictive_scores
order by next_attack_probability desc, recent_avg_severity desc
    );
  
  