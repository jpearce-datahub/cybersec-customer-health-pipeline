
  
    
    

    create  table
      "cybersec_health_dbt"."main"."security_attack_patterns__dbt_tmp"
  
    as (
      with incident_sequences as (
    select
        customer_id,
        incident_timestamp,
        attack_type,
        severity_level,
        source_ip,
        network_segment,
        lag(attack_type) over (partition by customer_id order by incident_timestamp) as prev_attack_type,
        lag(incident_timestamp) over (partition by customer_id order by incident_timestamp) as prev_timestamp,
        lead(attack_type) over (partition by customer_id order by incident_timestamp) as next_attack_type,
        lead(incident_timestamp) over (partition by customer_id order by incident_timestamp) as next_timestamp
    from "cybersec_health_dbt"."main"."stg_security_incidents"
),

attack_chains as (
    select
        customer_id,
        incident_timestamp,
        attack_type,
        severity_level,
        source_ip,
        network_segment,
        prev_attack_type,
        next_attack_type,
        case 
            when prev_timestamp is not null then 
                extract(epoch from (incident_timestamp - prev_timestamp)) / 60.0
            else null
        end as minutes_since_prev,
        case 
            when next_timestamp is not null then 
                extract(epoch from (next_timestamp - incident_timestamp)) / 60.0
            else null
        end as minutes_to_next,
        
        -- Identify potential attack chains
        case 
            when prev_attack_type is not null and 
                 extract(epoch from (incident_timestamp - prev_timestamp)) / 60.0 <= 60
            then concat(prev_attack_type, ' -> ', attack_type)
            else null
        end as attack_sequence
    from incident_sequences
),

pattern_analysis as (
    select
        customer_id,
        attack_sequence,
        count(*) as sequence_count,
        avg(minutes_since_prev) as avg_time_between,
        min(incident_timestamp) as first_occurrence,
        max(incident_timestamp) as last_occurrence
    from attack_chains
    where attack_sequence is not null
    group by customer_id, attack_sequence
),

customer_patterns as (
    select
        customer_id,
        count(distinct attack_type) as unique_attack_types,
        count(distinct source_ip) as unique_source_ips,
        count(distinct network_segment) as segments_affected,
        
        -- Time-based patterns
        extract(hour from incident_timestamp) as attack_hour,
        extract(dow from incident_timestamp) as attack_dow,
        
        -- Escalation patterns
        case 
            when severity_level = 'Low' and next_attack_type is not null and minutes_to_next <= 30 then 'Potential Escalation'
            when prev_attack_type = 'Intrusion' and attack_type = 'Malware' and minutes_since_prev <= 60 then 'Intrusion to Malware'
            when prev_attack_type = 'DDoS' and attack_type = 'Intrusion' and minutes_since_prev <= 120 then 'DDoS to Intrusion'
            else 'Isolated'
        end as pattern_type
        
    from attack_chains
    group by customer_id, incident_timestamp, attack_type, severity_level, next_attack_type, prev_attack_type, minutes_to_next, minutes_since_prev
)

select
    customer_id,
    pattern_type,
    count(*) as pattern_occurrences,
    avg(unique_attack_types) as avg_attack_variety,
    avg(unique_source_ips) as avg_ip_variety,
    avg(segments_affected) as avg_segments_affected,
    
    -- Time patterns
    mode() within group (order by attack_hour) as most_common_hour,
    mode() within group (order by attack_dow) as most_common_day,
    
    -- Risk assessment
    case 
        when pattern_type in ('Potential Escalation', 'Intrusion to Malware') then 'High Risk'
        when pattern_type = 'DDoS to Intrusion' then 'Medium Risk'
        else 'Low Risk'
    end as pattern_risk_level

from customer_patterns
group by customer_id, pattern_type
order by customer_id, pattern_occurrences desc
    );
  
  