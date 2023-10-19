with 

final as (

    select

        -- Metadata
        {{ select_standard_metadata_cols('fct_marketing_touchpoints_consolidated') }},

        -- Unique Key
        mt.touchpoint_key,

        -- Dimension Keys
        mt.touchpoint_sent_at_key,
        mt.account_key,
        mt.customer_key,

        -- Related Fact Keys
        mt.deployment_key,
        mt.opportunity_key,
        
        -- Business Keys
        mt.touchpoint_id,
        mt.account_id,
        mt.customer_id,		
        mt.deployment_id,
        mt.opportunity_id,

        -- Misc Attributes		
        mt.touchpoint_name, 
        mt.touchpoint_number, 
        mt.touchpoint_medium, 
        mt.touchpoint_marketing_activity_status, 
        mt.touchpoint_sender,
        mt.first_or_repeat_deployment,
        mt.content_type,
        mt.scheduled_send_type,
        mt.scheduled_send_source_system,
        mt.scheduled_send_detail_json,

        act.account_type,
        act.customer_industry,
        act.customer_tier,
        act.population_type,
        act.customer_membership_size,

        cust.customer_name,
        cust.is_fully_insured,

        dep.deployment_name,

        -- Indicators
        mt.is_email_communication,
        mt.is_mailer_communication,
        mt.is_customer_communication,
        mt.is_womens_pelvic_health,
        mt.was_touchpoint_scheduled,
        mt.was_touchpoint_sent_during_q2_2022,
        mt.is_touchpoint_sender_hh,

        tp.is_throttled,

        -- Dates
        mt.touchpoint_sent_at,

        act.first_closed_at,
        act.initial_deployment_launched_at,

        -- Metrics
        mt.account_max_opportunity_covered_lives,
        mt.touchpoint_partial_population,
        mt.scheduled_send_email_address_count,
        mt.scheduled_send_non_gmail_address_count,
        mt.scheduled_send_non_gmail_address_pct,
        mt.approximate_scheduled_send_count,

        act.total_covered_lives,
        act.eligible_members,

        dep.emerging_covered_lives,		
        dep.hard_cap,
        dep.annual_counter,

        ft.forecasted_conversions,
        ft.forecasted_associated_unattributed_conversions,
        ft.forecasted_associated_trailing_conversions

    from
        {{ ref("fct_marketing_touchpoints_consolidated") }} as mt
        inner join {{ ref("dim_dates") }} as dt 
            on mt.touchpoint_sent_at_key = dt.date_key
        inner join {{ ref("dim_accounts") }} as act 
            on mt.account_key = act.account_key
        inner join {{ ref("dim_customers") }} as cust 
            on mt.customer_key = cust.customer_key
        left join {{ ref("fct_deployments") }} as dep 
            on mt.deployment_key = dep.deployment_key
        left join {{ ref("fct_throttled_touchpoints") }} as tp 
            on mt.touchpoint_key = tp.touchpoint_key
        left join {{ ref("fct_forecasted_touchpoint_conversions") }} as ft 
                on mt.touchpoint_key = ft.touchpoint_key
    where true

)

select *
from final
where true
