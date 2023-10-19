/*
    Limit signup data to 2023 for demo purposes.
*/
select
    date_sub('{{ run_started_at }}', 5) as load_date_at, -- date when the data was loaded
    activity_date_at, -- date when the signup occured
    sum(weight) as signups -- total number of signups
from 
    {{ ref("stg_xsrc_rollups__marketing_events_observed_forecasted") }}
where true
    and activity_type = 5
    and year(activity_date_at) = 2023
group by 1, 2
