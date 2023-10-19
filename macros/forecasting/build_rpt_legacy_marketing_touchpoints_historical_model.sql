{# 
    Used to build rpt_legacy_marketing_touchpoints (includes current and previous data from production forecast runs only),
    and rpt_legacy_marketing_touchpoints_with_sandbox (includes current and previous data from any forecast run).
#}
{%- macro build_rpt_legacy_marketing_touchpoints_historical_model(is_prod_forecast_model=True) -%}

{%- set forecast_run_filter = '%' -%}
{%- if is_prod_forecast_model == True %}
    {%- set forecast_run_filter = '%' ~ var("prod_forecast_run_metadata") ~ '%' -%}
{%- endif -%}

with 

current_forecast_date as (

    select
        max(_fct_marketing_touchpoints_consolidated_loaded_at) as forecast_loaded_at
    from 
        {{ ref("rpt_legacy_marketing_touchpoints_historical_snapshot") }}
    where true
        and _fct_marketing_touchpoints_consolidated_dbt_cloud_run_vars_metadata like '{{ forecast_run_filter }}'

),

snapshot_history as (

    select
        {{ select_standard_metadata_cols(alias='fct_marketing_touchpoints_consolidated', alias_as='rpt_legacy_marketing_touchpoints_historical') }},
        dbt_scd_id as legacy_marketing_touchpoints_historical_key,
        id,
        touchpoint_id,
        touchpoint_key,
        client_id,
        account_id,
        touchpoint_name,
        touchpoint_number,
        touchpoint_medium,
        emerging_covered_lives,
        deployment_id,
        deployment_name,
        account_annual_counter,
        touchpoint_send_date,
        touchpoint_marketing_activity_status,
        touchpoint_sender,
        first_or_repeat_deployment,
        total_covered_lives,
        eligible_users,
        account_type,
        touchpoint_partial_population,
        updated_at,
        case 
            when _fct_marketing_touchpoints_consolidated_loaded_at = (select forecast_loaded_at from current_forecast_date) 
                then true
            else false 
        end::boolean as current,
        opportunity_covered_lives,
        scheduled_to_be_throttled,
        scheduled_sends,
        forecasted_signups,
        hard_cap,
        scheduled_send_calculation_type,
        forecasted_signups_associated_unattributed,
        forecasted_signups_associated_trailing,
        content_type,
        _rescued_data,
        dbt_scd_id,
        dbt_valid_from,
        dbt_valid_to
    from 
        {{ ref("rpt_legacy_marketing_touchpoints_historical_snapshot") }}
    where true
        and _fct_marketing_touchpoints_consolidated_dbt_cloud_run_vars_metadata like '{{ forecast_run_filter }}'

),

legacy_touchpoint_history as (

    select
        {{ build_standard_metadata_cols(
            unique_key='id',
            unique_key_name='legacy_marketing_touchpoints_historical_key',
            rec_source="'stg_xsrc_rollups__marketing_touchpoints_historical_dbt'",
            alias='_rpt_legacy_marketing_touchpoints_historical',
            use_null_hd=true
        ) }},
        id,
        touchpoint_id,
        {{ 
            build_hash_value(
                value=build_hash_diff(
                            cols=['touchpoint_id']
                        ),
                alias='touchpoint_key'
            )
        }},
        client_id,
        account_id,
        touchpoint_name,
        touchpoint_number,
        touchpoint_medium,
        emerging_covered_lives,
        deployment_id,
        deployment_name,
        account_annual_counter,
        touchpoint_send_date,
        touchpoint_marketing_activity_status,
        touchpoint_sender,
        first_or_repeat_deployment,
        total_covered_lives,
        eligible_users,
        account_type,
        touchpoint_partial_population,
        updated_at,
        false::boolean as current,
        opportunity_covered_lives,
        scheduled_to_be_throttled::boolean,
        scheduled_sends,
        forecasted_signups,
        hard_cap,
        scheduled_send_calculation_type,
        forecasted_signups_associated_unattributed,
        forecasted_signups_associated_trailing,
        content_type,
        null::string as _rescued_data,
        {{ build_hash_value('id::string', 'dbt_scd_id') }},
        updated_at as dbt_valid_from,
        updated_at as dbt_valid_to
    from 
        {{ ref("stg_xsrc_rollups__marketing_touchpoints_historical_dbt") }}
    where true

),

final as (

    select *
    from snapshot_history
    where true

    union all

    select *
    from legacy_touchpoint_history
    where true

)

select *
from final
where true

{%- endmacro -%}
