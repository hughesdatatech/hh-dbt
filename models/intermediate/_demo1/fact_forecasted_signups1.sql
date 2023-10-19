/*
    #1 forecasted signups for next year.
*/
select
    events.activity_date_at + interval 1 years as activity_date_at, -- next year date
    events.signups * hcm.value as forecasted_signups -- source signups * pre-defined multiplier
from 
    {{ ref("demo_signups_source1") }} as events
    {{ build_predefined_multiplier_join('demo1_multiplier_50', 'events') }}
where true

