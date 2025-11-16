with ip_incidents as (
    select
        source_ip,
        count(*) as total_incidents,
        count(distinct customer_id) as customers_affected,
        count(distinct geo_location) as locations_count,
        avg(severity_score) as avg_severity,
        max(severity_score) as max_severity,
        count(case when attack_type = 'Malware' then 1 end) as malware_attacks,
        count(case when attack_type = 'DDoS' then 1 end) as ddos_attacks,
        count(case when attack_type = 'Intrusion' then 1 end) as intrusion_attacks,
        count(case when response_category = 'Prevented' then 1 end) as prevented_count,
        min(incident_timestamp) as first_seen,
        max(incident_timestamp) as last_seen,
        count(case when incident_timestamp >= current_date - interval '7 days' then 1 end) as recent_activity
    from "cybersec_health_dbt"."main"."stg_security_incidents"
    group by source_ip
),

ip_reputation as (
    select
        *,
        case 
            when customers_affected >= 5 and avg_severity >= 3 then 'High Risk'
            when customers_affected >= 3 or avg_severity >= 2.5 then 'Medium Risk'
            when total_incidents >= 10 then 'Suspicious'
            else 'Low Risk'
        end as threat_level,
        
        case 
            when malware_attacks >= ddos_attacks and malware_attacks >= intrusion_attacks then 'Malware'
            when ddos_attacks >= intrusion_attacks then 'DDoS'
            else 'Intrusion'
        end as primary_attack_type,
        
        round((prevented_count::float / total_incidents::float) * 100, 2) as prevention_rate_pct
    from ip_incidents
)

select
    source_ip,
    total_incidents,
    customers_affected,
    locations_count,
    round(avg_severity, 2) as avg_severity,
    max_severity,
    malware_attacks,
    ddos_attacks,
    intrusion_attacks,
    prevented_count,
    first_seen,
    last_seen,
    recent_activity,
    threat_level,
    primary_attack_type,
    prevention_rate_pct,
    
    -- Calculate persistence score
    case 
        when recent_activity > 0 and total_incidents >= 5 then 'Persistent'
        when recent_activity > 0 then 'Active'
        when last_seen >= current_date - interval '30 days' then 'Recent'
        else 'Historical'
    end as activity_status
    
from ip_reputation
order by 
    case threat_level
        when 'High Risk' then 1
        when 'Medium Risk' then 2
        when 'Suspicious' then 3
        else 4
    end,
    total_incidents desc