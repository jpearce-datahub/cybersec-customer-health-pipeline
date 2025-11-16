
  
  create view "cybersec_health_dbt"."main"."stg_security_incidents__dbt_tmp" as (
    select
    "customer_id",
    "Timestamp"::timestamp as incident_timestamp,
    "Source IP Address" as source_ip,
    "Destination IP Address" as dest_ip,
    "Protocol",
    "Attack Type" as attack_type,
    "Severity Level" as severity_level,
    "Action Taken" as action_taken,
    "Malware Indicators" as malware_indicators,
    "Anomaly Scores"::float as anomaly_score,
    "Alerts/Warnings" as alerts_warnings,
    "Log Source" as log_source,
    "Network Segment" as network_segment,
    "Geo-location Data" as geo_location,
    
    -- Derived fields
    case 
        when "Severity Level" = 'Critical' then 4
        when "Severity Level" = 'High' then 3
        when "Severity Level" = 'Medium' then 2
        when "Severity Level" = 'Low' then 1
        else 0
    end as severity_score,
    
    case 
        when "Action Taken" = 'Blocked' then 'Prevented'
        when "Action Taken" = 'Logged' then 'Detected'
        when "Action Taken" = 'Ignored' then 'Ignored'
        else 'Unknown'
    end as response_category,
    
    case 
        when "Malware Indicators" = 'IoC Detected' then true
        else false
    end as has_malware_indicators,
    
    case 
        when "Alerts/Warnings" = 'Alert Triggered' then true
        else false
    end as alert_triggered,
    
    -- Threat intelligence enrichment
    case 
        when "Source IP Address" like '10.%' or "Source IP Address" like '192.168.%' or "Source IP Address" like '172.%' then 'Internal'
        else 'External'
    end as ip_classification,
    
    -- Geographic risk assessment
    case 
        when "Geo-location Data" like '%China%' or "Geo-location Data" like '%Russia%' or "Geo-location Data" like '%North Korea%' then 'High Risk Geography'
        when "Geo-location Data" like '%US%' or "Geo-location Data" like '%UK%' or "Geo-location Data" like '%Canada%' then 'Low Risk Geography'
        else 'Medium Risk Geography'
    end as geo_risk_level,
    
    -- Port analysis
    "Source Port"::int as source_port,
    "Destination Port"::int as dest_port,
    case 
        when "Destination Port"::int in (22, 23, 3389, 5900) then 'Remote Access'
        when "Destination Port"::int in (80, 443, 8080, 8443) then 'Web Services'
        when "Destination Port"::int in (21, 22, 25, 53, 110, 143) then 'Standard Services'
        when "Destination Port"::int > 1024 then 'High Port'
        else 'System Port'
    end as port_category,
    
    -- Attack sophistication
    case 
        when "Attack Signature" = 'Known Pattern A' and "Malware Indicators" = 'IoC Detected' then 'Advanced'
        when "Attack Signature" = 'Known Pattern B' then 'Intermediate'
        else 'Basic'
    end as attack_sophistication
    
from read_csv_auto('../data/raw/security_incidents.csv')
  );
