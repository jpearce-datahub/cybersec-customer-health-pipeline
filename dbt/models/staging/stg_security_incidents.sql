select
    incident_id,
    customer_id,
    incident_type,
    severity,
    detection_time::timestamp as detection_time,
    resolution_time::timestamp as resolution_time,
    false_positive::boolean as false_positive,
    case 
        when resolution_time is not null and detection_time is not null
        then extract(epoch from (resolution_time - detection_time)) / 3600.0
        else null
    end as resolution_hours
from read_csv_auto('../data/raw/security_incidents.csv')