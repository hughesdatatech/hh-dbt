{% snapshot rpt_legacy_marketing_touchpoints_historical_snapshot %}

    {{
        config(
            target_schema=target.get('schema'),
            unique_key='touchpoint_id',
            strategy='timestamp',
            updated_at='updated_at',
            invalidate_hard_deletes=True
        )
    }}

    with 

    final as (

        select

            {{ select_standard_metadata_cols(alias='fct_marketing_touchpoints_consolidated') }},
            mt.touchpoint_key,
            null::integer as id,
            mt.touchpoint_id,
            mt.customer_id as client_id,
            mt.account_id,
            mt.touchpoint_name,
            mt.touchpoint_number,
            mt.touchpoint_medium,
            mt.emerging_covered_lives,
            mt.deployment_id,
            mt.deployment_name,
            mt.annual_counter as account_annual_counter,
            mt.touchpoint_sent_at as touchpoint_send_date,
            mt.touchpoint_marketing_activity_status,
            mt.touchpoint_sender,
            mt.first_or_repeat_deployment::integer as first_or_repeat_deployment,
            mt.total_covered_lives,
            mt.eligible_members as eligible_users,
            mt.account_type,
            mt.touchpoint_partial_population,
            mt._fct_marketing_touchpoints_consolidated_loaded_at::timestamp as updated_at,
            mt.account_max_opportunity_covered_lives as opportunity_covered_lives,
            mt.is_throttled scheduled_to_be_throttled,
            mt.approximate_scheduled_send_count::double as scheduled_sends,
            mt.forecasted_conversions::double as forecasted_signups,
            mt.hard_cap,
            mt.scheduled_send_type as scheduled_send_calculation_type,
            mt.forecasted_associated_unattributed_conversions::double as forecasted_signups_associated_unattributed,
            mt.forecasted_associated_trailing_conversions::double as forecasted_signups_associated_trailing,
            mt.content_type,
            null::string as _rescued_data

        from 
            {{ ref("rpt_marketing_touchpoints_consolidated") }} as mt
        where true

    )

    select *
    from final
    where true

{% endsnapshot %}
