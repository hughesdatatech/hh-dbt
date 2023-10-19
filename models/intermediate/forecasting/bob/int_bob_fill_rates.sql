{# scheduled sends #}
{# replaces demand_forecast/SQL/scheduled_sends/ss2-scheduled_send_prep_tables.sql >>> bob_fill_rate #}

with 

efile_custs_by_relationship as (

    select
        el.customer_id,
        el.relationship,
        count(distinct el.marketing_id) as member_count,
        count(distinct iterable.iterable_email_address) as email_address_count,
        count(distinct
                case 
                    when not iterable.is_iterable_gmail_address 
                        then iterable.iterable_email_address
                end
            ) as non_gmail_address_count
    from 
        {{ ref("stg_efile_rollups__eligible_lives") }} as el
        left join {{ ref("int_iterable_members_joined_to_members")}} as iterable
            on el.marketing_id = iterable.final_marketing_id
        where true
            and el.is_18_plus
            and el.is_active
    group by 1, 2

),

efile_custs_by_relationship_with_cust_totals as (

    select
        customer_id,
        relationship,
        member_count,
        email_address_count,
        non_gmail_address_count,
        1.0 * sum(email_address_count) over (partition by customer_id) / sum(member_count) over (partition by customer_id) as customer_email_to_member_pct,
        sum(member_count) over () as total_member_count
    from 
        efile_custs_by_relationship
    where true
 
),

/*
    When Member list cannot be directly drawn from efile, use bob_fill_rates (book of business) to understand what percentage of estimated eligible lives to include.
    These are used when we do not have Customer specific info.
*/
final as (

    select
        relationship,
        sum(member_count)::integer as member_count,
        (1.0 * sum(member_count) / avg(total_member_count))::double as member_relationship_pct,
        sum(
            case
                when customer_email_to_member_pct > 0.2 
                    then email_address_count
                else 0
            end  
        )::integer as emails_w_list_count,
        sum(
            case
                when customer_email_to_member_pct > 0.2 -- TO DO: should this be .15?
                    then member_count
                else 0
            end
        )::integer as members_w_email_list_count,
        (
            1.0 * 
            emails_w_list_count / 
            sum(
                case
                    when customer_email_to_member_pct > 0.15 
                        then member_count
                    else 0
                end
            )
        )::double as members_w_email_pct,
        (
            1.0 * 
            sum(
                case
                    when customer_email_to_member_pct > 0.2 
                        then non_gmail_address_count
                    else 0
                end 
            ) /
            sum(
                case
                    when customer_email_to_member_pct > 0.15 
                        then member_count
                    else 0
                end
            )
         )::double as non_gmail_members_pct
    from 
        efile_custs_by_relationship_with_cust_totals
    where true
    group by 1

)

select *
from final
where true
