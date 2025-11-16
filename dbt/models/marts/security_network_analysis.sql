with network_incidents as (
    select
        network_segment,
        attack_type,
        severity_level,
        protocol,
        log_source,
        count(*) as incident_count,
        count(distinct customer_id) as affected_customers,
        avg(severity_score) as avg_severity_score,
        avg(anomaly_score) as avg_anomaly_score,
        count(case when has_malware_indicators then 1 end) as malware_incidents,
        count(case when alert_triggered then 1 end) as alert_incidents,
        count(case when response_category = 'Prevented' then 1 end) as prevented_incidents
    from {{ ref('stg_security_incidents') }}
    group by 1, 2, 3, 4, 5
),

segment_summary as (
    select
        network_segment,
        sum(incident_count) as total_incidents,
        sum(affected_customers) as total_affected_customers,
        
        -- Attack type breakdown
        sum(case when attack_type = 'Malware' then incident_count else 0 end) as malware_count,
        sum(case when attack_type = 'DDoS' then incident_count else 0 end) as ddos_count,
        sum(case when attack_type = 'Intrusion' then incident_count else 0 end) as intrusion_count,
        
        -- Severity breakdown
        sum(case when severity_level = 'Critical' then incident_count else 0 end) as critical_count,
        sum(case when severity_level = 'High' then incident_count else 0 end) as high_count,
        sum(case when severity_level = 'Medium' then incident_count else 0 end) as medium_count,
        sum(case when severity_level = 'Low' then incident_count else 0 end) as low_count,
        
        -- Protocol breakdown
        sum(case when protocol = 'TCP' then incident_count else 0 end) as tcp_incidents,
        sum(case when protocol = 'UDP' then incident_count else 0 end) as udp_incidents,
        sum(case when protocol = 'ICMP' then incident_count else 0 end) as icmp_incidents,
        
        -- Source breakdown
        sum(case when log_source = 'Firewall' then incident_count else 0 end) as firewall_incidents,
        sum(case when log_source = 'Server' then incident_count else 0 end) as server_incidents,
        
        sum(malware_incidents) as total_malware_indicators,
        sum(alert_incidents) as total_alerts,
        sum(prevented_incidents) as total_prevented,
        
        avg(avg_severity_score) as segment_avg_severity,
        avg(avg_anomaly_score) as segment_avg_anomaly
        
    from network_incidents
    group by 1
)

select
    network_segment,
    total_incidents,
    total_affected_customers,
    malware_count,
    ddos_count,
    intrusion_count,
    critical_count,
    high_count,
    medium_count,
    low_count,
    tcp_incidents,
    udp_incidents,
    icmp_incidents,
    firewall_incidents,
    server_incidents,
    total_malware_indicators,
    total_alerts,
    total_prevented,
    round(segment_avg_severity, 2) as segment_avg_severity,
    round(segment_avg_anomaly, 2) as segment_avg_anomaly,
    
    -- Calculate percentages
    round((malware_count::float / total_incidents::float) * 100, 2) as malware_pct,
    round((ddos_count::float / total_incidents::float) * 100, 2) as ddos_pct,
    round((intrusion_count::float / total_incidents::float) * 100, 2) as intrusion_pct,
    
    round((critical_count::float / total_incidents::float) * 100, 2) as critical_pct,
    round((high_count::float / total_incidents::float) * 100, 2) as high_pct,
    
    round((total_prevented::float / total_incidents::float) * 100, 2) as prevention_rate_pct,
    round((total_alerts::float / total_incidents::float) * 100, 2) as alert_rate_pct,
    
    -- Risk assessment per segment
    case 
        when critical_count > 0 and critical_pct > 20 then 'Critical'
        when high_count > 0 and (critical_pct + high_pct) > 30 then 'High'
        when (critical_pct + high_pct + medium_pct) > 50 then 'Medium'
        else 'Low'
    end as segment_risk_level,
    
    -- Most common attack type
    case 
        when malware_count >= ddos_count and malware_count >= intrusion_count then 'Malware'
        when ddos_count >= intrusion_count then 'DDoS'
        else 'Intrusion'
    end as primary_attack_type,
    
    -- Most common protocol
    case 
        when tcp_incidents >= udp_incidents and tcp_incidents >= icmp_incidents then 'TCP'
        when udp_incidents >= icmp_incidents then 'UDP'
        else 'ICMP'
    end as primary_protocol

from segment_summary
order by total_incidents desc, segment_avg_severity desc