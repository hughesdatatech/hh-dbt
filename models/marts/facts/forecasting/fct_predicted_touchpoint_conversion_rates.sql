{%- set unique_key = ['touchpoint_id'] -%}

with 

final as (

    select

        -- Metadata
        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='touchpoint_key',
            hd_source_model='int_py_predicted_touchpoint_conversion_rates',
            hd_except_cols=unique_key,
            rec_source="'int_py_predicted_touchpoint_conversion_rates'",
            alias='_fct_predicted_touchpoint_conversion_rates'
        ) }},
        *
    from 
        {{ ref("int_py_predicted_touchpoint_conversion_rates") }}
    where true

)

select *
from final
where true
