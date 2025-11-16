
  
    
    

    create  table
      "cybersec_health_dbt"."main"."security_kpi_dashboard__dbt_tmp"
  
    as (
      with current_metrics as (
    select
        count(distinct customer_id) as total_customers,
        count(*) as total_incidents,
        count(case when severity_level = 'Critical' then 1 end) as critical_incidents,
        count(case when severity_level = 'High' then 1 end) as high_incidents,
        count(case when response_category = 'Prevented' then 1 end) as prevented_incidents,
        count(case when alert_triggered then 1 end) as alerted_incidents,
        count(case when incident_timestamp >= current_date - interval '24 hours' then 1 end) as incidents_24h,
        count(case when incident_timestamp >= current_date - interval '7 days' then 1 end) as incidents_7d,
        count(case when incident_timestamp >= current_date - interval '30 days' then 1 end) as incidents_30d,
        avg(anomaly_score) as avg_anomaly_score
    from "cybersec_health_dbt"."main"."stg_security_incidents"
),

previous_period as (
    select
        count(*) as prev_total_incidents,
        count(case when severity_level = 'Critical' then 1 end) as prev_critical_incidents,
        count(case when response_category = 'Prevented' then 1 end) as prev_prevented_incidents
    from "cybersec_health_dbt"."main"."stg_security_incidents"
    where incident_timestamp >= current_date - interval '60 days'
    and incident_timestamp < current_date - interval '30 days'
),

sla_metrics as (
    select
        -- Mean Time to Detection (simulated)
        avg(case when alert_triggered then 15 else 45 end) as mttr_minutes,
        
        -- Prevention rate
        round((prevented_incidents::float / total_incidents::float) * 100, 2) as prevention_rate,
        
        -- Alert coverage
        round((alerted_incidents::float / total_incidents::float) * 100, 2) as alert_coverage,
        
        -- Critical incident rate
        round((critical_incidents::float / total_incidents::float) * 100, 2) as critical_rate
        
    from current_metrics
),

trend_analysis as (
    select
        cm.*,
        pp.*,
        sm.*,
        
        -- Calculate trends
        case 
            when pp.prev_total_incidents > 0 then
                round(((cm.incidents_30d - pp.prev_total_incidents)::float / pp.prev_total_incidents::float) * 100, 2)
            else 0
        end as incident_trend_pct,
        
        case 
            when pp.prev_critical_incidents > 0 then
                round(((cm.critical_incidents - pp.prev_critical_incidents)::float / pp.prev_critical_incidents::float) * 100, 2)
            else 0
        end as critical_trend_pct,
        
        case 
            when pp.prev_prevented_incidents > 0 then
                round(((cm.prevented_incidents - pp.prev_prevented_incidents)::float / pp.prev_prevented_incidents::float) * 100, 2)
            else 0
        end as prevention_trend_pct
        
    from current_metrics cm
    cross join previous_period pp
    cross join sla_metrics sm
)

select
    -- Current state
    total_customers,
    total_incidents,
    critical_incidents,
    high_incidents,
    incidents_24h,
    incidents_7d,
    incidents_30d,
    
    -- Performance metrics
    prevention_rate,
    alert_coverage,
    critical_rate,
    round(avg_anomaly_score, 2) as avg_anomaly_score,
    round(mttr_minutes, 1) as mttr_minutes,
    
    -- Trends
    incident_trend_pct,
    critical_trend_pct,
    prevention_trend_pct,
    
    -- Status indicators
    case 
        when critical_rate > 20 then 'Critical'
        when critical_rate > 10 then 'High'
        when critical_rate > 5 then 'Medium'
        else 'Good'
    end as security_posture,
    
    case 
        when prevention_rate >= 80 then 'Excellent'
        when prevention_rate >= 60 then 'Good'
        when prevention_rate >= 40 then 'Fair'
        else 'Poor'
    end as prevention_status,
    
    case 
        when alert_coverage >= 90 then 'Excellent'
        when alert_coverage >= 70 then 'Good'
        when alert_coverage >= 50 then 'Fair'
        else 'Poor'
    end as detection_status,
    
    case 
        when incident_trend_pct <= -10 then 'Improving'
        when incident_trend_pct <= 10 then 'Stable'
        when incident_trend_pct <= 25 then 'Concerning'
        else 'Critical'
    end as trend_status,
    
    current_timestamp as last_updated

from trend_analysis
    );
  
  