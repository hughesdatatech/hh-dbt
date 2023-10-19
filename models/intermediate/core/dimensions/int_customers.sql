with

/*
    Count the number of efile records per Customer.
*/
efile_customers as (
    
    select
        customer_id as efile_customer_id,
        count(1)::integer as efile_eligible_lives
    from {{ ref("stg_efile_rollups__eligible_lives") }}
    where true
        and is_18_plus
        and is_active
    group by
        1

),

customers_lob as (

    select
        customer_id,
        line_of_business,
        lower_line_of_business,
        is_fully_insured
    from
        {{ ref("stg_hh_db_rollups__customer_line_of_business")}}

),

customer_first_conversion as (

     select 
        customer_id,
        min(activity_date_at) as first_conversion_at
    from 
        {{ ref("stg_xsrc_rollups__marketing_events_observed_forecasted") }}
    where true
        and activity_type in (0, 5)
    group by 1

),

customers_base as (

    select 

        -- All base attributes from staged Customer data
        cust.*,
        
        -- Related IDs from Accounts
        act.account_id,

        -- Additional Misc Attributes
        clob.line_of_business,
        clob.lower_line_of_business,
        
        -- Additional Indicators
        clob.is_fully_insured,
        case
            when ec.efile_customer_id is null
                then true
            else false
        end as is_missing_efile,

        -- Dates
        cfc.first_conversion_at,

        -- Additional Metrics
        nvl(efile_eligible_lives, 0) as efile_eligible_lives,
        nvl(iterable_email_address_count, 0) as iterable_email_address_count,
        nvl(iterable_non_gmail_address_count, 0) as iterable_non_gmail_address_count,
        nvl(mailchimp_email_address_count, 0) as mailchimp_email_address_count,
        nvl(mailchimp_non_gmail_address_count, 0) as mailchimp_non_gmail_address_count,
        nvl(act.eligible_members, 0) as eligible_members

    from 
        {{ ref("stg_hh_db_public__customers") }} as cust
        left join efile_customers as ec 
            on cust.customer_id = ec.efile_customer_id
        left join customers_lob as clob
            on cust.customer_id = clob.customer_id
        left join customer_first_conversion as cfc 
            on cust.customer_id = cfc.customer_id
        left join {{ ref("int_iterable_customers_aggregated") }} as icust 
            on cust.customer_id = icust.customer_id
        left join {{ ref("int_mailchimp_customers_aggregated") }} as mcust 
            on cust.customer_id = mcust.customer_id
        left join {{ ref("stg_sfdc_rollups__accounts") }} as act 
            on cust.customer_id = act.customer_id
    where true

),

{# replaces demand_forecast/SQL/scheduled_sends/ss2-scheduled_send_prep_tables.sql >>> max_list #}
{% set list_size_threshold = 50 %}
{% set population_size_factor = '.2' %}
/*
    Combine Customers having email list sizes more than n Members (list_size_threshold defined below), otherwise ingore and assume they test lists.
    Also add a factor to only include Customers with a sufficiently large population (population_size_factor defined below), otherwise ignore and assume pilots.
*/
mailchimp_iterable_combined as (

    select
        cust.customer_id,
        cust.mailchimp_email_address_count as scheduled_send_email_address_count,
        cust.mailchimp_non_gmail_address_count as scheduled_send_non_gmail_address_count,
        cust.efile_eligible_lives,
        'mailchimp' as scheduled_send_source_system,
        1 as priority_code -- if Mailchimp list exists, prioritize and assume it is accurate since efiles are automatically uploaded to Iterable and may not have supplemental lists
    from 
        customers_base as cust
    where true
        and cust.mailchimp_email_address_count > {{ list_size_threshold }}
        and cust.mailchimp_email_address_count >= greatest(cust.efile_eligible_lives, cust.eligible_members) * {{ population_size_factor }}

    union all    

    select
        cust.customer_id,
        cust.iterable_email_address_count as scheduled_send_email_address_count,
        cust.iterable_non_gmail_address_count as scheduled_send_non_gmail_address_count,
        cust.efile_eligible_lives,
        'iterable' as scheduled_send_source_system,
        2 as priority_code
    from 
        customers_base as cust
    where true
        and cust.iterable_email_address_count > {{ list_size_threshold }} 
        and cust.iterable_email_address_count >= greatest(cust.efile_eligible_lives, cust.eligible_members) * {{ population_size_factor }}

),

/*
    Rank the customers by sender (with Mailchimp being the priority) and select the top one.
*/
combined_ranked as (

    select
        scheduled_send_source_system,
        customer_id,
        scheduled_send_email_address_count,
        scheduled_send_non_gmail_address_count,
        (scheduled_send_non_gmail_address_count * 1.0 / scheduled_send_email_address_count)::double as scheduled_send_non_gmail_address_pct,
        row_number() over(partition by customer_id order by priority_code asc) as sender_rank
    from 
        mailchimp_iterable_combined
    where true
        qualify sender_rank = 1

),

final as (

    select 

        cb.*,
        cr.scheduled_send_source_system,
        cr.scheduled_send_email_address_count,
        cr.scheduled_send_non_gmail_address_count,
        cr.scheduled_send_non_gmail_address_pct

    from 
        customers_base as cb
        left join combined_ranked as cr 
            on cb.customer_id = cr.customer_id

)

select *
from final
where true
