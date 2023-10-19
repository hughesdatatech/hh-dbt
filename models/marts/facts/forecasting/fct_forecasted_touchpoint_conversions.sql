{%- set unique_key = ['touchpoint_id'] -%}

with 

final as (

    select

        -- Metadata
        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='touchpoint_key',
            rec_source="'int_forecasted_touchpoint_conversions'",
            alias='_fct_forecasted_touchpoint_conversions'
        ) }},

        -- Dimension Keys

        -- Related Fact Keys

        -- Business Keys
        touchpoint_id,

        -- Metrics
        forecasted_conversions,
        forecasted_associated_unattributed_conversions,
        forecasted_associated_trailing_conversions

    from 
        {{ ref("int_forecasted_touchpoint_conversions") }}
    where true

)

select *
from final
where true
