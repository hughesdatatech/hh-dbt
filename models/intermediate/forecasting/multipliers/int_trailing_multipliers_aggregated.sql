{%- set first_of_last_month -%}
    date_trunc('month', {{ get_run_started_date_at() }}) - interval 1 month
{%- endset -%}

with 

/*
    Get a sub-set of the date dimension based on key dates.
*/
dates as (

    select
        date_day,
        day_of_month
    from
        {{ ref("dim_dates") }}
    where true
        and date_day between '{{ var("trailing_unattributed_multiplier_key_date_2") }}'::date 
        and (date_trunc('year', {{ get_run_started_date_at() }}) + interval 2 year) - interval 1 day

),

customer_first_send as (

    select 
        customer_id,
        min(touchpoint_sent_at) as touchpoint_sent_at
    from 
        {{ ref("fct_marketing_touchpoints_consolidated") }}
    where true
        and scheduled_send_type is not null
    group by 1

),

customer_first_accepted as (

    select 
        cust.customer_id,
        cust.account_id,
        cust.customer_name,
        cust.eligible_members,
        cust.is_fully_insured,
        nvl(cust.first_conversion_at, cfs.touchpoint_sent_at) as accepted_first_member_at
    from 
        {{ ref("dim_customers") }} as cust
        left join customer_first_send cfs 
            on cust.customer_id = cfs.customer_id
    where true
        and cust.customer_id is not null -- exclude dimension placeholder record
        and cust.account_id is not null
        and nvl(cust.first_conversion_at, cfs.touchpoint_sent_at) is not null -- should be more intentional as to why we are excluding, but for now this works

),


/*
    Get a list of Customers by month since their first accepted member.
*/
customers_by_month as(

    select
        dt.date_day as activity_month_at,
        cust.customer_id,
        cust.customer_name,
        cust.eligible_members,
        cust.is_fully_insured
    from 
        dates as dt
        inner join customer_first_accepted as cust
            on dt.date_day >= date_trunc('month', cust.accepted_first_member_at)::date
            and dt.day_of_month = 1
    where true 
  
),

/*
    Get a list of Customers with total signups per month. 
*/
signups_by_month as (

    select
        date_trunc('month', activity_timestamp_at)::date as activity_month_at,
        customer_id,
        sum(attributed_conversion_count) as attributed_conversion_count,
        sum(trailing_unassociated_conversion_count) as trailing_unassociated_conversion_count,
        sum(trailing_associated_conversion_count) as trailing_associated_conversion_count,
        sum(unattributed_conversion_count) as unattributed_conversion_count
    from 
        {{ ref("fct_observed_conversions") }}
    where true
        and date_trunc('month', activity_timestamp_at)::date >= '{{ var("trailing_unattributed_multiplier_key_date_2") }}'::date
    group by 1, 2

),

/*
    Using signups_by_month above, account for months with 0 signups by Customer.
*/
signups_by_month_including_zeros as (

    select
        cm.activity_month_at,
        cm.customer_id,
        cm.customer_name,
        cm.eligible_members,
        sum(nvl(attributed_conversion_count, 0)) as attributed_conversion_count,
        sum(nvl(trailing_unassociated_conversion_count, 0)) / 
            max(date_diff((cm.activity_month_at + interval 1 month)::date, cm.activity_month_at)) as daily_trailing_unassociated_conversion_count,
        sum(nvl(trailing_associated_conversion_count, 0)) as trailing_associated_conversion_count,
        sum(nvl(unattributed_conversion_count, 0)) as unattributed_conversion_count,
        count(distinct csm.activity_month_at) as includes_signup_data_count
    from
        customers_by_month as cm
        left join signups_by_month as csm
            on cm.customer_id = csm.customer_id
            and cm.activity_month_at = csm.activity_month_at
    where true
        and cm.activity_month_at <= {{ get_run_started_date_at() }}
    {{ dbt_utils.group_by(4) }}

),

rolling_signups_by_month as (

    select
        *,
        sum(attributed_conversion_count) over w as rolling_attributed_conversion_count,
        1.0 * (avg(daily_trailing_unassociated_conversion_count) over w) as rolling_trailing_unassociated_conversion_count,
        sum(trailing_associated_conversion_count) over w as rolling_trailing_associated_conversion_count,
        sum(unattributed_conversion_count) over w as rolling_unattributed_conversion_count,
        sum(includes_signup_data_count) over w as rolling_includes_signup_data_count,
        1.0 * (sum(trailing_associated_conversion_count) over w) / greatest(sum(attributed_conversion_count) over w, 1) as associated_trailing_multiplier,
        1.0 * (avg(daily_trailing_unassociated_conversion_count) over w) / eligible_members as unassociated_trailing_multiplier
    from 
        signups_by_month_including_zeros
    where true
        and eligible_members > 0
    window w as (partition by customer_id order by activity_month_at asc rows between 12 preceding and 1 preceding)

),

/*
    There seemed to be no direct equivalent in Databricks to the Postgres query originally used in the next section.
    Unfortunately the behaviors appeared different, and you are not able to pass a table reference to percentile_cont.
    Therefore, all multipliers had to be "exploded" in their own ctes and then joined back together at the end.
*/
{% set ptiles = 'array(0.2::double, 0.4, 0.6, 0.8, 1.0)' %} -- same as generate_series (0.2,1,0.2)
{% set ptiles_2 = 'array(0.0::double, 0.2, 0.4, 0.6, 0.8)' %} -- same as generate_series (0.2,1,0.2)-.2
{% set ptiles_2_2 = 'array(0.1::double, 0.3, 0.5, 0.7, 0.9)' %} -- same as generate_series (0.2,1,0.2)-.2/2

group_ranges as (

    select
        distinct 
        activity_month_at,
        array(0.2::double, 0.4, 0.6, 0.8, 1.0) as ptiles,
        percentile_cont(array(0.0::double, 0.2, 0.4, 0.6, 0.8)) within group (order by least(associated_trailing_multiplier, 5) asc) over(partition by activity_month_at) as lower_limit_associated_multiplier,
        percentile_cont(array(0.1::double, 0.3, 0.5, 0.7, 0.9)) within group (order by least(associated_trailing_multiplier, 5) asc) over(partition by activity_month_at) as median_associated_multiplier,
        percentile_cont(array(0.2::double, 0.4, 0.6, 0.8, 1.0)) within group (order by least(associated_trailing_multiplier, 5) asc) over(partition by activity_month_at) as upper_limit_associated_multiplier,
        percentile_cont(array(0.0::double, 0.2, 0.4, 0.6, 0.8)) within group (order by least(unassociated_trailing_multiplier, .0001) asc) over(partition by activity_month_at) as lower_limit_unassociated_multiplier,
        percentile_cont(array(0.1::double, 0.3, 0.5, 0.7, 0.9)) within group (order by least(unassociated_trailing_multiplier, .0001) asc) over(partition by activity_month_at) as median_unassociated_multiplier,
        percentile_cont(array(0.2::double, 0.4, 0.6, 0.8, 1.0)) within group (order by least(unassociated_trailing_multiplier, .0001) asc) over(partition by activity_month_at) as upper_limit_unassociated_multiplier
    from 
        rolling_signups_by_month
    where true
        and activity_month_at between '{{ var("trailing_unattributed_multiplier_key_date_3") }}'::date 
        and {{ get_run_started_date_at() }} - interval 3 months  -- temporarily excluding June 2023 and after due to data issues, usual code is - interval 1 month
        and nvl(rolling_attributed_conversion_count, 0) > 0

),

exploded_ptiles as (

    select
        activity_month_at, 
        row_number() over(partition by activity_month_at order by null) as row_num,
        explode(ptiles) as ptile
    from group_ranges
    where true

),

exploded_lower_limit_associated_multiplier as (

    select
        activity_month_at, 
        row_number() over(partition by activity_month_at order by null) as row_num,
        explode(lower_limit_associated_multiplier) as lower_limit_associated_multiplier
    from group_ranges
    where true


),

exploded_median_associated_multiplier as (

    select
        activity_month_at, 
        row_number() over(partition by activity_month_at order by null) as row_num,
        explode(median_associated_multiplier) as median_associated_multiplier
    from group_ranges
    where true

),

exploded_upper_limit_associated_multiplier (

    select
        activity_month_at, 
        row_number() over(partition by activity_month_at order by null) as row_num,
        explode(upper_limit_associated_multiplier) as upper_limit_associated_multiplier
    from group_ranges
    where true


),

exploded_lower_limit_unassociated_multiplier (

    select
        activity_month_at, 
        row_number() over(partition by activity_month_at order by null) as row_num,
        explode(lower_limit_unassociated_multiplier) as lower_limit_unassociated_multiplier
    from group_ranges
    where true


),

exploded_median_unassociated_multiplier (

    select
        activity_month_at, 
        row_number() over(partition by activity_month_at order by null) as row_num,
        explode(median_unassociated_multiplier) as median_unassociated_multiplier
    from group_ranges
    where true


),

exploded_upper_limit_unassociated_multiplier (

    select
        activity_month_at, 
        row_number() over(partition by activity_month_at order by null) as row_num,
        explode(upper_limit_unassociated_multiplier) as upper_limit_unassociated_multiplier
    from group_ranges
    where true


),

/*
    Join all multipliers back together for each month.
    The order is preserved using the row_num assigned to each exploded array element in the previous ctes.
    This was tested, and is deterministic.
*/
combined_multipliers as (

    select
        ep.activity_month_at,
        ep.row_num,
        ep.ptile,
        llam.lower_limit_associated_multiplier,
        mam.median_associated_multiplier,
        ulam.upper_limit_associated_multiplier,
        llum.lower_limit_unassociated_multiplier,
        mum.median_unassociated_multiplier,
        ulum.upper_limit_unassociated_multiplier
    from 
        exploded_ptiles as ep 
        inner join exploded_lower_limit_associated_multiplier as llam 
            on ep.activity_month_at = llam.activity_month_at
            and ep.row_num = llam.row_num
        inner join exploded_median_associated_multiplier as mam 
            on ep.activity_month_at = mam.activity_month_at
            and ep.row_num = mam.row_num
        inner join exploded_upper_limit_associated_multiplier as ulam 
            on ep.activity_month_at = ulam.activity_month_at
            and ep.row_num = ulam.row_num
        inner join exploded_lower_limit_unassociated_multiplier as llum
            on ep.activity_month_at = llum.activity_month_at
            and ep.row_num = llum.row_num
        inner join exploded_median_unassociated_multiplier as mum
            on ep.activity_month_at = mum.activity_month_at
            and ep.row_num = mum.row_num
        inner join exploded_upper_limit_unassociated_multiplier as ulum
            on ep.activity_month_at = ulum.activity_month_at
            and ep.row_num = ulum.row_num

),

monthly_customer_multipliers as (

    select
        rcm.customer_id,
        rcm.customer_name,
        rcm.eligible_members,
        rcm.activity_month_at,
        rcm.rolling_attributed_conversion_count,
        rcm.rolling_trailing_unassociated_conversion_count,
        rcm.rolling_trailing_associated_conversion_count,
        associated_gr.ptile as associated_multiplier_bucket,
        unassociated_gr.ptile as unassociated_multiplier_bucket,
        associated_gr.median_associated_multiplier as avg_associated_multiplier,
        unassociated_gr.median_unassociated_multiplier as avg_unassociated_multiplier,
        1.0 * sum(rcm.rolling_trailing_associated_conversion_count) over overall / sum(rcm.rolling_attributed_conversion_count) over overall as overall_associated_multiplier,
        1.0 * sum(rcm.rolling_trailing_unassociated_conversion_count) over overall / sum(rcm.eligible_members) over overall as overall_unassociated_multiplier
    from rolling_signups_by_month as rcm
    inner join combined_multipliers as associated_gr 
        on associated_gr.activity_month_at = rcm.activity_month_at
        and rcm.associated_trailing_multiplier between associated_gr.lower_limit_associated_multiplier and associated_gr.upper_limit_associated_multiplier
        and (rcm.associated_trailing_multiplier < associated_gr.upper_limit_associated_multiplier or associated_gr.ptile = 1.0) -- between allows value to equal lower and upper limit, want to restrict upper limit to prevent fanouts
    left join combined_multipliers as unassociated_gr
        on unassociated_gr.activity_month_at = rcm.activity_month_at
        and rcm.unassociated_trailing_multiplier between unassociated_gr.lower_limit_unassociated_multiplier and unassociated_gr.upper_limit_unassociated_multiplier
        and (rcm.unassociated_trailing_multiplier < unassociated_gr.upper_limit_unassociated_multiplier or unassociated_gr.ptile = 1.0)
    window 
        w_asso as (partition by rcm.activity_month_at, associated_gr.ptile),
        w_unasso as (partition by rcm.activity_month_at, unassociated_gr.ptile),
        overall as (partition by rcm.activity_month_at)

),

overall_multiplier as (

    select 
        activity_month_at,
        avg(overall_associated_multiplier) as overall_associated_multiplier,
        avg(overall_unassociated_multiplier) as overall_unassociated_multiplier
    from monthly_customer_multipliers
    group by 1
 
),

final as (

    select
        cm.*,
        (mcm.associated_multiplier_bucket / .2)::integer as associated_multiplier_decile,
        (mcm.unassociated_multiplier_bucket / .2)::integer as unassociated_multiplier_decile,
        coalesce(mcm.avg_associated_multiplier, gr.median_associated_multiplier, ovr.overall_associated_multiplier) as associated_trailing_multiplier,
        coalesce(mcm.avg_unassociated_multiplier, gr.median_unassociated_multiplier, ovr.overall_unassociated_multiplier) * cm.eligible_members as unassociated_trailing_daily_signups
    from  
        customers_by_month as cm
        left join overall_multiplier as ovr
            on ovr.activity_month_at = least(cm.activity_month_at, date_trunc('month', {{ get_run_started_timestamp_at() }}) - interval 3 months) -- temporarily excluding June 2023 and after due to data issues, usual code is least(cm.activity_month_at, {{ first_of_last_month }})
        left join monthly_customer_multipliers as mcm
            on mcm.customer_id = cm.customer_id
            and mcm.activity_month_at = least(cm.activity_month_at, date_trunc('month', {{ get_run_started_timestamp_at() }}) - interval 3 months) -- temporarily excluding June 2023 and after due to data issues, usual code is least(cm.activity_month_at, {{ first_of_last_month }})
        left join combined_multipliers as gr
            on cm.is_fully_insured
            and gr.ptile = .2
            and gr.activity_month_at = least(cm.activity_month_at, date_trunc('month', {{ get_run_started_timestamp_at() }}) - interval 3 months) -- temporarily excluding June 2023 and after due to data issues, usual code is least(cm.activity_month_at, {{ first_of_last_month }})
    where true
        and cm.activity_month_at between '{{ var("trailing_unattributed_multiplier_key_date_3") }}'::date and (date_trunc('year', {{ get_run_started_date_at() }}) + interval 2 year) - interval 1 day
)

select *
from final
where true
