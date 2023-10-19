{# replaces SQL/scheduled_sends/ss2-scheduled_send_prep_tables.sql >>> multiple ctes #}
with

final as (

    select
        customer_id,
        count(1)::integer as iterable_email_address_count,
        sum(
            case
                when not is_iterable_gmail_address 
                    then 1
                else 0
            end
        )::integer as iterable_non_gmail_address_count
    from
        {{ ref("int_iterable_members_joined_to_members") }}
    where true
        and customer_id is not null
    group by 1

)

select *
from final
where true
