select 

    -- Metadata
    -- TO DO: need source metadata, extracted timestamp?

    -- Business Keys
    touchpoint_id as touchpoint_id,
    email_id as email_address,
    marketing_id as marketing_id,
    user_uuid as member_uuid,
    nvl(client_id, -1) as customer_id,
    try_cast(get_json_object(detail, '$.screener_id') as int) as screening_id,

    -- Misc Attributes
    activity_type,
    try_cast(get_json_object(detail, '$.screener_outcome') as string) as screener_outcome,
    try_cast(get_json_object(detail, '$.program') as string) as program,
    try_cast(get_json_object(detail, '$.sub_program') as string) as sub_program,
    try_cast(get_json_object(detail, '$.utm_source') as string) as utm_source,
    try_cast(get_json_object(detail, '$.utm_medium') as string) as utm_medium,
    try_cast(get_json_object(detail, '$.utm_campaign') as string) as utm_campaign,
    try_cast(get_json_object(detail, '$.utm_template') as string) as utm_template,
    try_cast(get_json_object(detail, '$.utm_content') as string) as utm_content,
    lower(utm_source) as lower_utm_source,
    lower(utm_medium) as lower_utm_medium,
    lower(utm_campaign) as lower_utm_campaign,
    lower(utm_template) as lower_utm_template,
    lower(utm_content) as lower_utm_content,
    try_cast(get_json_object(detail, '$.attribution_type') as string) as attribution_type,
    try_cast(get_json_object(detail, '$.attribution_detail') as string) as attribution_detail,
    case 
        when activity_type = 5 
                and attribution_type = 'trailing'
                and lower_utm_medium in ('print', 'flyer', 'online', 'portal', 'website', 'legacy', 'quantumhealth', 'extreferral')
            then 'trailing_unassociated'
        when activity_type = 5
                and attribution_type = 'trailing'
            then 'trailing_associated'
    end as trailing_attribution_type,
    
    -- JSON
    detail as detail_json,

    -- Indicators
    try_cast(get_json_object(detail, '$.in_forecast_holdout') as boolean) as is_in_holdout,

    -- Dates
    {{ build_date_key('activity_timestamp', 'activity_date_at') }},
    activity_timestamp::date as activity_date_at,
    activity_timestamp as activity_timestamp_at,
    try_cast(get_json_object(detail, '$.forecast_run_date') as timestamp) as _fct_forecasted_accrued_customer_touchpoint_conversions_loaded_at, -- use name based on new process
    -- try_cast(get_json_object(detail, '$.growth_forecast_dt') as timestamp) as growth_forecast_dt_at, -- maintain legacy name, new process not yet implemented
    try_cast(get_json_object(detail, '$.touchpoint_data_date') as timestamp) as _fct_marketing_touchpoints_consolidated_loaded_at, -- use name based on new process
   
    -- Metrics
    weight,
    try_cast(get_json_object(detail, '$.movement_multiplier') as double) as movement_multiplier,
    try_cast(get_json_object(detail, '$.cancellation_multiplier') as double) as cancellation_multiplier,

    -- conversion counts
    case
        when activity_type = 5
            then 1
    end as conversion_count,
    case
        when activity_type = 5
                and attribution_type = 'attributed'
            then 1
        when activity_type = 5
            then 0
    end as attributed_conversion_count,
    case
        when activity_type = 5
                and attribution_type = 'trailing'
            then 1
        when activity_type = 5
            then 0
    end as trailing_conversion_count,
     case
        when activity_type = 5
                and attribution_type = 'unattributed'
            then 1
        when activity_type = 5
            then 0
    end as unattributed_conversion_count,

    -- trailing conversion counts
    case
        when activity_type = 5
                and trailing_attribution_type = 'trailing_unassociated'
            then 1
        when activity_type = 5
            then 0
    end as trailing_unassociated_conversion_count,
    case
        when activity_type = 5
                and trailing_attribution_type = 'trailing_associated'
            then 1
        when activity_type = 5
            then 0
    end as trailing_associated_conversion_count,

    -- y2 and enso conversion counts
    case
        when activity_type = 5
                and attribution_type = 'y2_signups'
            then 1
        when activity_type = 5
            then 0
    end as y2_signups_conversion_count,
    case
        when activity_type = 5
                and attribution_type = 'enso'
            then 1
        when activity_type = 5
            then 0
    end as enso_conversion_count
        
from 
   {{ source('xsrc_rollups', 'marketing_events_observed_forecasted') }}
