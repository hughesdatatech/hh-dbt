{%- set unique_key = ['touchpoint_id'] -%}

with 

deployments as (

    select
        deployment_id as _deployment_id,
        account_id
    from 
        {{ ref("fct_deployments") }} as dep 
    where true

),

accounts as (

    select
        account_id as _account_id,
        customer_id
    from 
        {{ ref("dim_accounts") }} as act 
    where true

),

final as (

    select

        -- Metadata
        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='touchpoint_key',
            hd_source_model='int_marketing_touchpoints',
            hd_except_cols=unique_key,
            rec_source="'int_marketing_touchpoints'",
            alias='_fct_marketing_touchpoints'
        ) }},

        -- Dimension Keys
        touchpoint_sent_at_key,
        {{ build_hash_value(value=build_hash_diff(cols=['account_id']), alias='account_key') }},
        {{ build_hash_value(value=build_hash_diff(cols=['customer_id']), alias='customer_key') }},

        -- Related Fact Keys
        {{ build_hash_value(value=build_hash_diff(cols=['deployment_id']), alias='deployment_key') }},
        {{ build_hash_value(value=build_hash_diff(cols=['opportunity_id']), alias='opportunity_key') }},

        -- Business Keys
        touchpoint_id,
        dep.account_id,
        act.customer_id,
        mt.deployment_id,
        opportunity_id,

        -- Misc Attributes
        touchpoint_name,
        --lower_touchpoint_name,
        touchpoint_number,
        touchpoint_medium,
        --lower_touchpoint_medium,
        touchpoint_marketing_activity_status,
        -- lower_touchpoint_marketing_activity_status,
        touchpoint_sender_staged,
        -- lower_touchpoint_sender,
        first_or_repeat_deployment,
        test_type,
        -- lower_test_type,
        content_type,
        -- lower_content_type,

        -- JSON

        -- Indicators
        was_touchpoint_scheduled,
        is_email_communication,
        is_mailer_communication,
        is_customer_communication,
        was_touchpoint_sent_during_q2_2022,
        is_womens_pelvic_health,

        -- Dates
        touchpoint_sent_at,

        -- Metrics
        touchpoint_partial_population

    from 
        {{ ref("int_marketing_touchpoints") }} as mt -- TO DO: join to im_deployments and im_opportunities to get keys applicable at time of loading
        left join deployments as dep -- TO DO: be aware of type2-scd changes
            on mt.deployment_id = dep._deployment_id
        left join accounts as act 
            on dep.account_id = act._account_id
    where true

)

select *
from final
where true
