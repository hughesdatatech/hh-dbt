with 

cust_info as (

    select
        mt.customer_id,
        act.dmc_programs_launched_at,
        min(mt.touchpoint_sent_at) as first_touchpoint_sent_at -- TO DO: this should go in dim_accounts or dim_customer, or use new status table
    from 
        {{ ref("fct_marketing_touchpoints") }} as mt
        inner join {{ ref("dim_accounts") }} as act 
            on mt.customer_id = act.customer_id
    where true 
        and mt.touchpoint_marketing_activity_status in ('Approved', 'Awaiting Approval') -- TO DO: move this to status or other table and add as indicator
    group by 1, 2
        
),

final as (

    select
        customer_id,
        greatest(date_trunc('month', dt.date_day) , date_trunc('week', dt.date_day)) as activity_date_at,
        coalesce(dt.date_day >= ci.dmc_programs_launched_at, false) as is_dmc_included,
        count(dt.date_day) as num_dates
    from 
        cust_info as ci
        inner join {{ ref("dim_dates") }} as dt 
            on dt.date_day >= ci.first_touchpoint_sent_at
            and dt.date_day between '2021-01-01'::date and (date_trunc('year', {{ get_run_started_date_at() }}) + interval 2 years - interval 1 days)
    where true
    group by 1, 2, 3

)

select *
from final
where true
