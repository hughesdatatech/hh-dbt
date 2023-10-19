{%- set unique_key = ['customer_id', 'detail_json', 'activity_date_at'] -%}

with 

final as (

    select

        -- Metadata
        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='forecasted_customer_conversion_key',
            rec_source="record_source",
            alias='_fct_forecasted_customer_conversions'
        ) }},

        -- Dimension Keys
        {{ build_date_key('activity_date_at', 'activity_date_at') }},

        -- Related Fact Keys

        -- Business Keys
        customer_id,

        -- Misc Attributes
        attribution_type,
        program,
        sub_program,

        -- JSON
        detail_json,

        -- Dates
        activity_date_at,

        -- Metrics
        weight, 
        y2_renewal_rate,
        y1_onboarding_rate_for_y2
        
    from 
        {{ ref("int_forecasted_customer_conversions") }}
    where true

)

select *
from final
where true
