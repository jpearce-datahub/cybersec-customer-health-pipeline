with time_series_data as (
    select
        date_trunc('week', incident_timestamp) as week_start,
        date_trunc('month', incident_timestamp) as month_start,
        extract(hour from incident_timestamp) as hour_of_day,
        extract(dow from incident_timestamp) as day_of_week,
        extract(month from incident_timestamp) as month_of_year,
        extract(quarter from incident_timestamp) as quarter,
        
        attack_type,
        severity_level,
        customer_id,
        
        count(*) as incident_count,
        avg(severity_score) as avg_severity,
        avg(anomaly_score) as avg_anomaly
        
    from "cybersec_health_dbt"."main"."stg_security_incidents"
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
),

hourly_patterns as (
    select
        hour_of_day,
        sum(incident_count) as total_incidents,
        avg(avg_severity) as avg_hourly_severity,
        count(distinct customer_id) as customers_affected,
        
        -- Attack type distribution by hour
        sum(case when attack_type = 'Malware' then incident_count else 0 end) as malware_incidents,
        sum(case when attack_type = 'DDoS' then incident_count else 0 end) as ddos_incidents,
        sum(case when attack_type = 'Intrusion' then incident_count else 0 end) as intrusion_incidents
        
    from time_series_data
    group by hour_of_day
),

daily_patterns as (
    select
        case day_of_week
            when 0 then 'Sunday'
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
        end as day_name,
        day_of_week,
        sum(incident_count) as total_incidents,
        avg(avg_severity) as avg_daily_severity,
        count(distinct customer_id) as customers_affected
        
    from time_series_data
    group by day_of_week
),

monthly_patterns as (
    select
        case month_of_year
            when 1 then 'January' when 2 then 'February' when 3 then 'March'
            when 4 then 'April' when 5 then 'May' when 6 then 'June'
            when 7 then 'July' when 8 then 'August' when 9 then 'September'
            when 10 then 'October' when 11 then 'November' when 12 then 'December'
        end as month_name,
        month_of_year,
        sum(incident_count) as total_incidents,
        avg(avg_severity) as avg_monthly_severity,
        count(distinct customer_id) as customers_affected
        
    from time_series_data
    group by month_of_year
),

quarterly_trends as (
    select
        quarter,
        sum(incident_count) as total_incidents,
        avg(avg_severity) as avg_quarterly_severity,
        count(distinct customer_id) as customers_affected,
        
        -- Calculate quarter-over-quarter growth
        lag(sum(incident_count)) over (order by quarter) as prev_quarter_incidents,
        case 
            when lag(sum(incident_count)) over (order by quarter) > 0 then
                round(((sum(incident_count) - lag(sum(incident_count)) over (order by quarter))::float / 
                       lag(sum(incident_count)) over (order by quarter)::float) * 100, 2)
            else null
        end as qoq_growth_pct
        
    from time_series_data
    group by quarter
),

peak_analysis as (
    select
        'Hourly' as time_dimension,
        (select hour_of_day from hourly_patterns order by total_incidents desc limit 1) as peak_time,
        (select total_incidents from hourly_patterns order by total_incidents desc limit 1) as peak_incidents,
        'Peak attack hour' as description
    
    union all
    
    select
        'Daily' as time_dimension,
        (select day_name from daily_patterns order by total_incidents desc limit 1) as peak_time,
        (select total_incidents from daily_patterns order by total_incidents desc limit 1) as peak_incidents,
        'Peak attack day' as description
    
    union all
    
    select
        'Monthly' as time_dimension,
        (select month_name from monthly_patterns order by total_incidents desc limit 1) as peak_time,
        (select total_incidents from monthly_patterns order by total_incidents desc limit 1) as peak_incidents,
        'Peak attack month' as description
)

-- Final output combining all patterns
select
    'Summary' as analysis_type,
    json_object(
        'peak_hour', (select peak_time from peak_analysis where time_dimension = 'Hourly'),
        'peak_day', (select peak_time from peak_analysis where time_dimension = 'Daily'),
        'peak_month', (select peak_time from peak_analysis where time_dimension = 'Monthly'),
        'total_incidents', (select sum(total_incidents) from hourly_patterns),
        'avg_severity', (select round(avg(avg_hourly_severity), 2) from hourly_patterns)
    ) as trend_summary,
    current_timestamp as analysis_timestamp

union all

select
    'Hourly Distribution' as analysis_type,
    json_object(
        'hour', hour_of_day,
        'incidents', total_incidents,
        'severity', round(avg_hourly_severity, 2),
        'customers', customers_affected
    ) as trend_summary,
    current_timestamp as analysis_timestamp
from hourly_patterns
order by 
    case when analysis_type = 'Summary' then 0 else 1 end,
    trend_summary