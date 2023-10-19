{{
    config(
        materialized='incremental',
        unique_key='touchpoint_id',
        merge_update_columns = [
            'latest_loaded_at', 
            'latest_touchpoint_marketing_activity_status', 
            'latest_touchpoint_sent_at', 
            '__fct_marketing_touchpoints_status_tracking_update_loaded_at'
        ]
    )
}}

{%- set unique_key = ['touchpoint_id'] -%}

with 

{% if is_incremental() %}

base_tp as (

        select
            touchpoint_id,
            original_loaded_at,
            original_touchpoint_marketing_activity_status,
            original_touchpoint_sent_at,

            latest_loaded_at,
            latest_touchpoint_marketing_activity_status,
            latest_touchpoint_sent_at,

            true::boolean as is_existing_record,
            false::boolean as is_from_history
        from 
            {{ this }}
        where
            true

),

{% else %}

{# First time data load so select from the historical table from the existing process. #}

orig_hist_tp as (

    select
        hist.touchpoint_id,
        hist.updated_at as original_loaded_at,
        hist.touchpoint_marketing_activity_status as original_touchpoint_marketing_activity_status,
        hist.touchpoint_send_date as original_touchpoint_sent_at,
        row_number() over(partition by hist.touchpoint_id order by hist.updated_at asc, hist.id asc) as row_sequence
    from 
        {{ ref("stg_xsrc_rollups__marketing_touchpoints_historical_dbt") }} as hist
    where true
    qualify row_sequence = 1

),

latest_hist_tp as (

    select
        hist.touchpoint_id,
        hist.updated_at as latest_loaded_at,
        hist.touchpoint_marketing_activity_status as latest_touchpoint_marketing_activity_status,
        hist.touchpoint_send_date as latest_touchpoint_sent_at,
        row_number() over(partition by hist.touchpoint_id order by hist.updated_at desc, hist.id desc) as row_sequence
    from 
        {{ ref("stg_xsrc_rollups__marketing_touchpoints_historical_dbt") }} as hist
    where true
    qualify row_sequence = 1

),

base_tp as (

    select
        orig.touchpoint_id,
        orig.original_loaded_at,
        orig.original_touchpoint_marketing_activity_status,
        orig.original_touchpoint_sent_at,
        latest.latest_loaded_at,
        latest_touchpoint_marketing_activity_status,
        latest_touchpoint_sent_at,
        true::boolean as is_existing_record,
        true::boolean as is_from_history
    from 
        orig_hist_tp as orig
        inner join latest_hist_tp as latest 
            on orig.touchpoint_id = latest.touchpoint_id
    where true

),

{% endif %}

current_tp as (

        select
            touchpoint_id,
            {{ get_run_started_timestamp_at() }} as loaded_at,
            touchpoint_marketing_activity_status,
            touchpoint_sent_at
        from 
            {{ ref("stg_sfdc_rollups__marketing_touchpoints") }} 
        where
            true

),

joined_tp as (

    select
        nvl(curr.touchpoint_id, base.touchpoint_id) as touchpoint_id,
        nvl(base.original_loaded_at, curr.loaded_at) as original_loaded_at,
        nvl(curr.loaded_at, base.latest_loaded_at) as latest_loaded_at,
        case 
            when nvl(base.is_existing_record, false)
                then base.original_touchpoint_marketing_activity_status -- matched current or historical record so take the status from the base
            else curr.touchpoint_marketing_activity_status -- new record so take the status from the current data being loaded
        end as original_touchpoint_marketing_activity_status,
        case
            when base.is_existing_record is null -- new record so take the status from the current data being loaded
                then curr.touchpoint_marketing_activity_status
            else base.latest_touchpoint_marketing_activity_status -- matched current or historical record so take the status from the base
        end as latest_touchpoint_marketing_activity_status,
        case 
            when nvl(base.is_existing_record, false)
                then base.original_touchpoint_sent_at -- matched current or historical record so take the send date from the base
            else curr.touchpoint_sent_at -- new record so take the send date from the current data being loaded
        end as original_touchpoint_sent_at,
        case
            when base.is_existing_record is null -- new record so take the send date from the current data being loaded
                then curr.touchpoint_sent_at
            else base.latest_touchpoint_sent_at -- matched current or historical record so take the send date from the base
        end as latest_touchpoint_sent_at,
        case
            when not nvl(base.is_existing_record, false) -- completely new record
                then 'stg_sfdc_rollups__marketing_touchpoints'
            when curr.touchpoint_id is not null and nvl(base.is_from_history, false) -- matched current and historical data
                then 'stg_sfdc_rollups__marketing_touchpoints | stg_xsrc_rollups__marketing_touchpoints_historical_dbt'
            else 'stg_xsrc_rollups__marketing_touchpoints_historical_dbt' -- came from historical data only
        end as record_source
    from 
        current_tp as curr
        full join base_tp as base 
            on curr.touchpoint_id = base.touchpoint_id
    where
        true

),

final as (

    select

        -- Metadata
        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='touchpoint_key',
            rec_source='record_source',
            alias='_fct_marketing_touchpoints_status_tracking'
        ) }},
        {{ build_loaded_at(alias='__fct_marketing_touchpoints_status_tracking_update') }},
        touchpoint_id,
        original_loaded_at,
        latest_loaded_at,
        original_touchpoint_marketing_activity_status,
        latest_touchpoint_marketing_activity_status,
        original_touchpoint_sent_at,
        latest_touchpoint_sent_at

    from 
        joined_tp
    where true

)

select 
    *
from final
where true
