
  
    
    

    create  table
      "cybersec_health_dbt"."main"."security_clustering_analysis__dbt_tmp"
  
    as (
      with customer_features as (
    select
        customer_id,
        count(*) as incident_count,
        avg(severity_score) as avg_severity,
        avg(anomaly_score) as avg_anomaly,
        count(distinct attack_type) as attack_variety,
        count(distinct source_ip) as unique_ips,
        count(distinct network_segment) as segments_affected,
        count(case when response_category = 'Prevented' then 1 end)::float / count(*) as prevention_rate,
        
        -- Time patterns
        stddev(extract(hour from incident_timestamp)) as hour_variance,
        count(case when extract(hour from incident_timestamp) between 0 and 6 then 1 end) as night_incidents,
        count(case when extract(dow from incident_timestamp) in (0,6) then 1 end) as weekend_incidents,
        
        -- Attack sophistication
        count(case when attack_sophistication = 'Advanced' then 1 end) as advanced_attacks,
        count(case when geo_risk_level = 'High Risk Geography' then 1 end) as high_risk_geo_attacks
        
    from "cybersec_health_dbt"."main"."stg_security_incidents"
    group by customer_id
),

normalized_features as (
    select
        customer_id,
        -- Normalize features for clustering (0-1 scale)
        (incident_count - min(incident_count) over()) / nullif(max(incident_count) over() - min(incident_count) over(), 0) as norm_incident_count,
        (avg_severity - min(avg_severity) over()) / nullif(max(avg_severity) over() - min(avg_severity) over(), 0) as norm_avg_severity,
        (avg_anomaly - min(avg_anomaly) over()) / nullif(max(avg_anomaly) over() - min(avg_anomaly) over(), 0) as norm_avg_anomaly,
        (attack_variety - min(attack_variety) over()) / nullif(max(attack_variety) over() - min(attack_variety) over(), 0) as norm_attack_variety,
        prevention_rate as norm_prevention_rate,
        (advanced_attacks - min(advanced_attacks) over()) / nullif(max(advanced_attacks) over() - min(advanced_attacks) over(), 0) as norm_advanced_attacks,
        
        -- Original values for interpretation
        incident_count,
        avg_severity,
        avg_anomaly,
        attack_variety,
        prevention_rate,
        advanced_attacks,
        high_risk_geo_attacks
    from customer_features
),

customer_clusters as (
    select
        customer_id,
        incident_count,
        round(avg_severity, 2) as avg_severity,
        round(avg_anomaly, 2) as avg_anomaly,
        attack_variety,
        round(prevention_rate * 100, 2) as prevention_rate_pct,
        advanced_attacks,
        high_risk_geo_attacks,
        
        -- Simple clustering based on key metrics
        case 
            when norm_incident_count >= 0.8 and norm_avg_severity >= 0.7 then 'High Volume High Severity'
            when norm_incident_count >= 0.8 and norm_avg_severity < 0.7 then 'High Volume Low Severity'
            when norm_incident_count < 0.8 and norm_avg_severity >= 0.7 then 'Low Volume High Severity'
            when norm_advanced_attacks >= 0.5 then 'Advanced Threat Targets'
            when norm_prevention_rate <= 0.3 then 'Poor Defense'
            when norm_prevention_rate >= 0.8 then 'Strong Defense'
            else 'Standard Profile'
        end as customer_cluster,
        
        -- Risk score calculation
        round(
            (norm_incident_count * 0.3 + 
             norm_avg_severity * 0.3 + 
             norm_avg_anomaly * 0.2 + 
             norm_advanced_attacks * 0.2) * 100, 2
        ) as composite_risk_score
        
    from normalized_features
)

select
    customer_cluster,
    count(*) as customers_in_cluster,
    round(avg(incident_count), 1) as avg_incidents,
    round(avg(avg_severity), 2) as cluster_avg_severity,
    round(avg(avg_anomaly), 2) as cluster_avg_anomaly,
    round(avg(prevention_rate_pct), 2) as cluster_avg_prevention_rate,
    round(avg(composite_risk_score), 2) as cluster_avg_risk_score,
    
    -- Cluster characteristics
    case 
        when customer_cluster = 'High Volume High Severity' then 'Critical - Immediate attention required'
        when customer_cluster = 'Advanced Threat Targets' then 'High - Enhanced monitoring needed'
        when customer_cluster = 'Poor Defense' then 'High - Security improvements needed'
        when customer_cluster = 'Low Volume High Severity' then 'Medium - Monitor for escalation'
        when customer_cluster = 'Strong Defense' then 'Low - Maintain current posture'
        else 'Medium - Standard monitoring'
    end as cluster_priority,
    
    -- Recommended actions
    case 
        when customer_cluster = 'High Volume High Severity' then 'Deploy additional security controls, incident response team engagement'
        when customer_cluster = 'Advanced Threat Targets' then 'Threat hunting, advanced detection rules, security assessment'
        when customer_cluster = 'Poor Defense' then 'Security architecture review, prevention capability enhancement'
        when customer_cluster = 'Low Volume High Severity' then 'Root cause analysis, targeted security measures'
        when customer_cluster = 'Strong Defense' then 'Continue current practices, periodic review'
        else 'Regular monitoring and standard security practices'
    end as recommended_actions

from customer_clusters
group by customer_cluster
order by cluster_avg_risk_score desc
    );
  
  