select
    touchpoint_id,
    touchpoint_name,
    touchpoint_medium,
    touchpoint_marketing_activity_status,
    touchpoint_sent_at
from 
    {{ ref("stg_sfdc_rollups__marketing_touchpoints") }}
where true
    and is_weekend_send_date
    and touchpoint_sent_at >= {{ get_run_started_date_at() }}
order by touchpoint_sent_at