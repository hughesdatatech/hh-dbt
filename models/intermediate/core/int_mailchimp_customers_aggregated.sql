{# replaces SQL/scheduled_sends/ss2-scheduled_send_prep_tables.sql >>> multiple ctes #}
with

customer_lists as (

    select
        xwalk.customer_id,
        memb.mailchimp_list_id,
        count(1)::integer as mailchimp_email_address_count,
        sum(
            case
                when not memb.is_gmail_address 
                    then 1
                else 0
            end
        )::integer as mailchimp_non_gmail_address_count
    from
        {{ ref("stg_mailchimp_mailchimp__members") }} as memb 
        inner join {{ ref("stg_mailchimp_rollups__customer_lists_xwalk") }} as xwalk 
            on memb.mailchimp_list_id = xwalk.mailchimp_list_id
    where true
    group by 1, 2

),

/*
    Some Customers have multiple audiences. Rank the lists and select the one with the most addresses for send counts.
*/
final as (

    select
        *,
        row_number() over(partition by customer_id order by mailchimp_email_address_count desc, mailchimp_non_gmail_address_count desc, mailchimp_list_id desc) as list_rank
    from 
        customer_lists
    where true
        qualify list_rank = 1
    

)

select *
from final
where true
