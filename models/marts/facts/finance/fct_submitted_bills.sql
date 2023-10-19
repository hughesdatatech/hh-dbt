{%- set unique_key = ['submission_id'] -%}

with 

final as (

    select
    
        -- Metadata
        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='submitted_bill_key',
            hd_source_model='int_submitted_bills',
            hd_except_cols=unique_key,
            rec_source="'int_submitted_bills'",
            alias='_fct_submitted_bills'
        ) }},
        *

    from 
        {{ ref("int_submitted_bills") }}
    where true

)

select *
from final
where true
