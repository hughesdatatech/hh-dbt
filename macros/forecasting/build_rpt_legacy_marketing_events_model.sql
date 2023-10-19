{# 
    Used to build rpt_legacy_marketing_events (includes current and previous data from production forecast runs only),
    and rpt_legacy_marketing_events_with_sandbox (includes current and previous data from any forecast run).
#}
{%- macro build_rpt_legacy_marketing_events_model(is_prod_forecast_model=True) -%}

{%- set forecast_run_filter = '%' -%}
{%- if is_prod_forecast_model == True %}
    {%- set forecast_run_filter = '%' ~ var("prod_forecast_run_metadata") ~ '%' -%}
{%- endif -%}

{%- set unique_key = ['_source_key', 'activity_type'] -%}

with 

/*
    Current production forecast date should be the same for all for the forecasts since they are designed and 
    intended to be built at the same time. Therefore, just select the latest production forecast load
    date from one of the snapshot tables using _dbt_cloud_runvars_metadata field.
*/
current_forecast_date as (

    select
        true as is_current,
        max(_fct_forecasted_attributed_customer_touchpoint_conversions_loaded_at) as forecast_loaded_at
    from 
        {{ ref("fct_forecasted_attributed_customer_touchpoint_conversions_snapshot") }}
    where true
        and _fct_forecasted_attributed_customer_touchpoint_conversions_dbt_cloud_run_vars_metadata like '{{ forecast_run_filter }}'

),

/*
    Previous production forecast is simply the max production forecast load
    date that is not the latest production forecast load date.
*/
previous_forecast_date as (

    select
        false as is_current,
        max(_fct_forecasted_attributed_customer_touchpoint_conversions_loaded_at) as forecast_loaded_at
    from 
        {{ ref("fct_forecasted_attributed_customer_touchpoint_conversions_snapshot") }}
    where true
        and _fct_forecasted_attributed_customer_touchpoint_conversions_dbt_cloud_run_vars_metadata like '{{ forecast_run_filter }}'
        and _fct_forecasted_attributed_customer_touchpoint_conversions_loaded_at is distinct from (
            select
                max(_fct_forecasted_attributed_customer_touchpoint_conversions_loaded_at)
            from 
                {{ ref("fct_forecasted_attributed_customer_touchpoint_conversions_snapshot") }}
            where true
                and _fct_forecasted_attributed_customer_touchpoint_conversions_dbt_cloud_run_vars_metadata like '{{ forecast_run_filter }}'
        )

),

current_and_previous_forecast_dates as (

    select *
    from current_forecast_date
    where true

    union all

    select *
    from previous_forecast_date
    where true

),

/*
    Get scheduled sends used for the current production forecast, activity_type = 0.
*/
scheduled_sends as (

    select
        {{ select_standard_metadata_cols(alias='fct_marketing_touchpoints_consolidated', alias_as='rpt_legacy_marketing_events') }},
        touchpoint_key as _source_key,
        0::integer as activity_type,
        touchpoint_sent_at::timestamp as activity_timestamp,
        approximate_scheduled_send_count as weight,
        touchpoint_id,
        null::string as email_id,
        null::string as marketing_id,
        null::string as user_uuid,
        customer_id as client_id,
        scheduled_send_detail_json as detail,
        null::string as _rescued_data
    from
        {{ ref("rpt_marketing_touchpoints_consolidated_snapshot") }}
    where true
        and _fct_marketing_touchpoints_consolidated_loaded_at = (select forecast_loaded_at from current_and_previous_forecast_dates where is_current)

),

/*
    Get observed conversions used for the current production forecast, activity_type = 5.
*/
observed_conversions as (

    select
        {{ select_standard_metadata_cols(alias='fct_observed_conversions', alias_as='rpt_legacy_marketing_events') }},
        observed_conversion_key as _source_key,
        5::integer as activity_type,
        activity_timestamp_at as activity_timestamp,
        conversion_count::double as weight,
        touchpoint_id,
        null::string as email_id,
        marketing_id as marketing_id,
        member_uuid as user_uuid,
        customer_id as client_id,
        detail_json as detail,
        null::string as _rescued_data
    from
        {{ ref("rpt_observed_conversions_snapshot") }}
    where true
        and _fct_observed_conversions_loaded_at = (select forecast_loaded_at from current_and_previous_forecast_dates where is_current)
),

/*
    If there is no previous forecast date from the snapshot table then this is the first run, 
    so select the previous forecast from the existing (old) marketing_events table.
*/
old_previous_forecast as (

    select
        {{ 
            build_standard_metadata_cols(
                rec_source="'stg_xsrc_rollups__marketing_events_dbt'",
                alias='_rpt_legacy_marketing_events',
                use_null_hd=true
            )
        }}
        null as _source_key,
        6::integer as activity_type,
        activity_timestamp,
        weight,
        touchpoint_id,
        email_id,
        marketing_id,
        user_uuid,
        client_id,
        detail,
        null::string as _rescued_data
    from
        {{ ref("stg_xsrc_rollups__marketing_events_dbt") }}
    where true
        and activity_type = 6
        and (select forecast_loaded_at from current_and_previous_forecast_dates where not is_current) is null

),

other_old_events as (

    select
        {{ 
            build_standard_metadata_cols(
                rec_source="'stg_xsrc_rollups__marketing_events_dbt'",
                alias='_rpt_legacy_marketing_events',
                use_null_hd=true
            )
        }}
        null as _source_key,
        activity_type,
        activity_timestamp,
        weight,
        touchpoint_id,
        email_id,
        marketing_id,
        user_uuid,
        client_id,
        detail,
        null::string as _rescued_data
    from
        {{ ref("stg_xsrc_rollups__marketing_events_dbt") }}
    where true
        and activity_type != 6
        and activity_type != 408

),

/*
    Forecasted Attributed Customer Touchpoint Conversions, activity_type = 6 (previous), 7 (current).
*/
attributed_conversions as (

    select
        {{ select_standard_metadata_cols(alias='fct_forecasted_attributed_customer_touchpoint_conversions', alias_as='rpt_legacy_marketing_events', use_null_hd=true) }},
        forecasted_attributed_customer_touchpoint_conversion_key as _source_key,
        case
            when _fct_forecasted_attributed_customer_touchpoint_conversions_loaded_at = (select forecast_loaded_at from current_forecast_date where is_current)
                then 7
            else 6
        end::integer as activity_type,
        activity_date_at as activity_timestamp,
        weight::double as weight,
        touchpoint_id,
        null::string as email_id,
        null::string as marketing_id,
        null::string as user_uuid,
        customer_id as client_id,
        detail_json as detail,
        null::string as _rescued_data
    from
        {{ ref("fct_forecasted_attributed_customer_touchpoint_conversions_snapshot") }}
    where true
        and _fct_forecasted_attributed_customer_touchpoint_conversions_loaded_at in (select forecast_loaded_at from current_and_previous_forecast_dates)

),

/*
    Forecasted Customer Conversions, activity_type = 6 (previous), 7 (current).
*/
customer_conversions as (

    select
        {{ select_standard_metadata_cols(alias='fct_forecasted_customer_conversions', alias_as='rpt_legacy_marketing_events', use_null_hd=true) }},
        forecasted_customer_conversion_key as _source_key,
        case
            when _fct_forecasted_customer_conversions_loaded_at = (select forecast_loaded_at from current_and_previous_forecast_dates where is_current)
                then 7
            else 6
        end::integer as activity_type,
        activity_date_at as activity_timestamp,
        weight::double as weight,
        null::string as touchpoint_id,
        null::string as email_id,
        null::string as marketing_id,
        null::string as user_uuid,
        customer_id as client_id,
        detail_json as detail,
        null::string as _rescued_data
    from
        {{ ref("fct_forecasted_customer_conversions_snapshot") }}
    where true
        and _fct_forecasted_customer_conversions_loaded_at in (select forecast_loaded_at from current_and_previous_forecast_dates)

),

/*
    Forecasted Y2 Member Conversions, activity_type = 6 (previous), 7 (current).
*/
y2_member_conversions as (

    select
        {{ select_standard_metadata_cols(alias='fct_forecasted_y2_member_conversions', alias_as='rpt_legacy_marketing_events', use_null_hd=true) }},
        forecasted_y2_member_conversion_key as _source_key,
        case
            when _fct_forecasted_y2_member_conversions_loaded_at = (select forecast_loaded_at from current_and_previous_forecast_dates where is_current)
                then 7
            else 6
        end::integer as activity_type,
        activity_date_at as activity_timestamp,
        weight::double as weight,
        null::string as touchpoint_id,
        null::string as email_id,
        marketing_id,
        member_uuid as user_uuid,
        customer_id as client_id,
        detail_json as detail,
        null::string as _rescued_data
    from
        {{ ref("fct_forecasted_y2_member_conversions_snapshot") }}
    where true
        and _fct_forecasted_y2_member_conversions_loaded_at in (select forecast_loaded_at from current_and_previous_forecast_dates)

),

/*
    Forecasted Other Conversions, activity_type = 6 (previous), 7 (current).
*/
other_conversions as (

    select
        {{ select_standard_metadata_cols(alias='fct_forecasted_other_conversions', alias_as='rpt_legacy_marketing_events', use_null_hd=true) }},
        forecasted_other_conversion_key as _source_key,
        case
            when _fct_forecasted_other_conversions_loaded_at = (select forecast_loaded_at from current_and_previous_forecast_dates where is_current)
                then 7
            else 6
        end::integer as activity_type,
        activity_date_at as activity_timestamp,
        weight::double as weight,
        null::string as touchpoint_id,
        null::string as email_id,
        null::string as marketing_id,
        null::string as user_uuid,
        null::integer as client_id,
        detail_json as detail,
        null::string as _rescued_data
    from
        {{ ref("fct_forecasted_other_conversions_snapshot") }}
    where true
        and _fct_forecasted_other_conversions_loaded_at in (select forecast_loaded_at from current_and_previous_forecast_dates)

),

current_and_previous_forecast as (

    select *
    from attributed_conversions
    where true

    union all 

    select *
    from customer_conversions
    where true

    union all 
    
    select *
    from y2_member_conversions
    where true

    union all 
    
    select *
    from other_conversions
    where true

),

q4_baseline_forecast as (

    select
        {{ select_standard_metadata_cols(alias='rpt_legacy_marketing_events', alias_as='null', use_null_hd=true) }},
        _source_key,
        408::integer as activity_type,
        activity_timestamp,
        weight,
        touchpoint_id,
        email_id,
        marketing_id,
        user_uuid,
        client_id,
        detail,
        _rescued_data
    from 
        current_and_previous_forecast
    where true
        and activity_type = 6

),

all_combined as (

    select *
    from scheduled_sends 
    where true

    union all

    select *
    from observed_conversions 
    where true

    union all

    select *
    from current_and_previous_forecast
    where true

    union all

    select *
    from q4_baseline_forecast
    where true

    union all

    select *
    from old_previous_forecast
    where true

    union all

    select *
    from other_old_events
    where true

),

final as (

    select 
        {{ 
            build_hash_value(
                value=build_hash_diff(
                            cols=unique_key
                        ),
                alias='legacy_marketing_events_key'
            )
        }},
        *
    from 
        all_combined
    where true

)

select *
from final
where true

{%- endmacro -%}
