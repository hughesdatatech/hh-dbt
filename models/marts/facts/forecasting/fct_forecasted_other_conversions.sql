{%- set unique_key = ['attribution_type', 'program', 'sub_program', 'adjustment_type', 'activity_date_at'] -%}

with 

final as (

    select

        -- Metadata
        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='forecasted_other_conversion_key',
            rec_source="record_source",
            alias='_fct_forecasted_other_conversions'
        ) }},

        -- Dimension Keys
        {{ build_date_key('activity_date_at', 'activity_date_at') }},

        -- Related Fact Keys

        -- Business Keys

        -- Misc Attributes
        attribution_type,
        program,
        sub_program,
        adjustment_type,

        -- JSON
        detail_json,

        -- Dates
        activity_date_at,

        -- Metrics
        weight
        
    from 
        {{ ref("int_forecasted_other_conversions") }}
    where true

)

select *
from final
where true
