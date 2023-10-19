{%- set unique_key = ['subscription_id'] -%}

with 

final as (

    select

        -- Metadata
        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='billable_subscription_key',
            hd_source_model='int_billable_subscriptions',
            hd_except_cols=unique_key,
            rec_source="'int_billable_subscriptions'",
            alias='_fct_billable_subscriptions'
        ) }},
        *

    from 
        {{ ref("int_billable_subscriptions") }}
    where true

)

select *
from final
where true
