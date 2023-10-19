with

parent_customer_ids as (

    select
        customer_id
    from 
        {{ ref("stg_hh_db_public__customers") }}
    where true

),

final as (

    select 
        acct.*
    from 
        {{ ref("stg_sfdc_rollups__accounts") }} as acct 
        left join {{ ref("stg_hh_db_public__customers") }} as cust 
            on acct.customer_id = cust.customer_id
    where true
        and (acct.customer_id is null or cust.customer_id is not null)   {# Ensure the customer_id is either null, or matches a customer_id from public.customers #}
   
)

select *
from final
where true
