{%- set unique_key = ['submission_id'] -%}

with 

final as (

    select

        -- Metadata
        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='legacy_itemized_submission_key',
            rec_source="'fct_submitted_bills'",
            alias='_fct_legacy_itemized_submissions'
        ) }},

        {{ build_common_legacy_buca_columns(second_col_group=true) }}
        
    from 
        {{ ref("fct_submitted_bills") }}
    where true

)

select *
from final
where true
