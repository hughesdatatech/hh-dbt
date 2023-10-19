{%- set unique_key = ['touchpoint_id'] -%}

with 

final as (

    select

        -- Metadata
        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='touchpoint_key',
            hd_source_model='int_throttled_touchpoints',
            hd_except_cols=unique_key,
            rec_source="'int_throttled_touchpoints'",
            alias='_fct_throttled_touchpoints'
        ) }},

        -- Dimension Keys

        -- Related Fact Keys

        -- Business Keys
        touchpoint_id,
       
       
        -- Misc Attributes
        touchpoint_marketing_activity_status,

        -- Indicators
        is_throttled_base,
        is_throttled

        -- Dates

        -- Metrics

    from 
        {{ ref("int_throttled_touchpoints") }}
    where true

)

select *
from final
where true
