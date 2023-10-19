{%- set unique_key = ['remit_id'] -%}

with 

final as (

    select

        -- metadata
        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='legacy_itemized_remit_key',
            rec_source="'fct_bill_payments'",
            alias='_fct_legacy_itemized_remits'
        ) }},

        {{ build_common_legacy_buca_columns(second_col_group=true) }},

        remit_id as payment_id,
        payment_date_at as payment_date,
        patient_payment_amount as payment_amount,
        payer_name as payer,
        payment_number,
        icn,
        remit_payment_type as payment_type
        
    from 
        {{ ref("fct_bill_payments") }}
    where true

)

select *
from final
where true
