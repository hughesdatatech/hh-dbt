{%- set unique_key = ['touchpoint_id'] -%}

with 

final as (

    select

         -- Metadata
        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='touchpoint_key',
            hd_source_model='int_marketing_touchpoints_consolidated',
            hd_except_cols=unique_key,
            rec_source="'int_marketing_touchpoints_consolidated'",
            alias='_fct_marketing_touchpoints_consolidated'
        ) }},

        -- Dimension Keys
        touchpoint_sent_at_key,
        account_key,
        customer_key,

        -- Related Fact Keys
        deployment_key,
        opportunity_key,

        -- Business Keys
        touchpoint_id,	
        account_id,
        customer_id,
        deployment_id,
        opportunity_id,
        
        -- Misc Attributes
        touchpoint_name, 
        touchpoint_number, 
        touchpoint_medium, 
        touchpoint_marketing_activity_status, 
        touchpoint_sender,
        first_or_repeat_deployment,
        content_type,
        scheduled_send_type,
        scheduled_send_source_system,
        scheduled_send_detail_json,

        -- Indicators
        is_email_communication,
        is_mailer_communication,
        is_customer_communication,
        is_womens_pelvic_health,
        was_touchpoint_scheduled,
        was_touchpoint_sent_during_q2_2022,
        is_touchpoint_sender_hh,

        -- Dates
        touchpoint_sent_at,
        
        -- Metrics	
        account_max_opportunity_covered_lives,
        touchpoint_partial_population,
        
        scheduled_send_email_address_count,
        scheduled_send_non_gmail_address_count,
        scheduled_send_non_gmail_address_pct,
        approximate_scheduled_send_count 

    from 
        {{ ref("int_marketing_touchpoints_consolidated") }} as mt
    where true

)

select *
from final
where true
