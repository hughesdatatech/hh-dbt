{%- set unique_key = ['customer_id', 'marketing_id', 'member_uuid', 'attribution_type', 'program', 'sub_program', 'activity_date_at'] -%}

with 

final as (

    select

        -- Metadata
        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='forecasted_y2_member_conversion_key',
            rec_source="record_source",
            alias='_fct_forecasted_y2_member_conversions'
        ) }},

        -- Dimension Keys
        {{ build_date_key('activity_date_at', 'activity_date_at') }},

        -- Related Fact Keys

        -- Business Keys
        customer_id,
        marketing_id,
        member_uuid,

        -- Misc Attributes
        attribution_type,
        program,
        sub_program,

        -- JSON
        detail_json,

        -- Dates
        activity_date_at,

        -- Metrics
        cannibalization_adjustment_multiplier,
        weight, 
        y2_renewal_rate,
        y1_onboarding_rate_for_y2
        
    from 
        {{ ref("int_forecasted_y2_member_conversions") }}
    where true

)

select *
from final
where true
