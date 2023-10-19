{%- set unique_key = ['remit_id'] -%}

with 

final as (

    select
    
        -- Metadata
        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='bill_payment_key',
            hd_source_model='int_bill_payments',
            hd_except_cols=unique_key,
            rec_source="'int_bill_payments'",
            alias='_fct_bill_payments'
        ) }},
        *

    from 
        {{ ref("int_bill_payments") }}
    where true

)

select *
from final
where true
