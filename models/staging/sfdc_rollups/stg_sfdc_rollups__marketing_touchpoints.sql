select 

    -- Metadata
    insertion_date as _source_meta_insertion_date,

    -- Business Keys
    marketing_activity_id as touchpoint_id,

    deployment_id,
    opportunity_id,

    -- Misc Attributes
    sub_product_name_count as touchpoint_name,
    lower(touchpoint_name) as lower_touchpoint_name,
    try_cast(trim(regexp_extract(sub_product_name_count, '\\d+', 0)) as smallint) as touchpoint_number,
    try_cast(trim(regexp_extract(sub_product_name_count, '[^0-9]+', 0)) as varchar(50)) as touchpoint_medium,
    lower(touchpoint_medium) as lower_touchpoint_medium,
    marketing_activity_status as touchpoint_marketing_activity_status,
    lower(marketing_activity_status) as lower_touchpoint_marketing_activity_status,
    sender as touchpoint_sender_staged,
    lower(touchpoint_sender_staged) as lower_touchpoint_sender_staged,
    deployment_forecast_indicator::smallint as first_or_repeat_deployment, -- TO DO: name? deployment_number?
    test_type,
    lower(test_type) as lower_test_type,
    content_type,
    lower(content_type) as lower_content_type,

    -- JSON

    -- Indicators
    case
        when nvl(lower_touchpoint_marketing_activity_status, '') in ('', 'approved', 'awaiting approval')
            then true 
        else false
    end as was_touchpoint_scheduled, -- indicates touchpoint was / is scheduled to go out
    case 
        when lower_touchpoint_medium = 'email'
            then true 
        else false
    end as is_email_communication,
    case 
        when lower_touchpoint_medium = 'mailer'
            then true 
        else false
    end as is_mailer_communication,
    case 
        when lower_touchpoint_medium in ('client comms / announcements', 'client comms') 
            then true
        else false
    end as is_customer_communication,
    case
        when date_trunc('quarter', try_cast(send_date as date)) = '2022-04-01'::date
                and try_cast(send_date as date) < '2022-05-25'::date
            then true
        else false
    end as was_touchpoint_sent_during_q2_2022, -- indicator for touchpoint dates falling within range of q2 2022 gmail deliverability issue,
    case
        when lower_content_type = 'womens pelvic health'
            then true
        else false
    end as is_womens_pelvic_health,
    dayofweek(try_cast(send_date as date)) in (1, 7) as is_weekend_send_date,

    -- Dates
    {{ build_date_key('send_date', 'touchpoint_sent_at') }},
    try_cast(send_date as date) as touchpoint_sent_at,

    -- Metrics
    partial_population_size as touchpoint_partial_population

from 
   {{ source('sfdc_rollups', 'sf_marketing_activities') }}
