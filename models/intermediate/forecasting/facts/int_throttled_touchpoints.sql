with 

unassociated_trailing as (

    select
        cw.customer_id,
        'trailing' as attribution_type,
        'chronic' as program,
        'unassociated' as attribution_detail,
        cw.activity_date_at,
        sum(tam.unassociated_trailing_daily_signups * cw.num_dates * (case when is_dmc_included then .71 else 1.0 end)) as weight
    from 
        {{ ref("int_customer_weeks_with_dmc_indicator") }} as cw
        inner join {{ ref("fct_customer_trailing_unattributed_multipliers") }} as tam 
            on cw.customer_id = tam.customer_id
            and date_trunc('month', cw.activity_date_at) = tam.activity_month_at
    where true
    {{ dbt_utils.group_by(5) }}
),

combined_marketing_events as (

    select
        7 as activity_type,
        null::string as touchpoint_id,
        customer_id,
        program,
        activity_date_at,
        null::date as _fct_marketing_touchpoints_consolidated_loaded_at,
        null::double as client_cancellation_multiplier,
        weight
    from 
        unassociated_trailing
    where true

    union all 

    -- forecasted, chronic conversions
    select
        7 as activity_type, -- TO DO: add activity type to fact table
        touchpoint_id,
        customer_id,
        program,
        activity_date_at,
        _fct_marketing_touchpoints_consolidated_loaded_at,
        null::double as client_cancellation_multiplier, -- not used
        weight
    from 
        {{ ref("fct_forecasted_attributed_customer_touchpoint_conversions") }}
    where true
        and program = 'chronic'

    union all 

    -- observed, chronic conversions
    select
        activity_type,
        touchpoint_id,
        customer_id,
        program,
        activity_date_at,
        null::date as _fct_marketing_touchpoints_consolidated_loaded_at, -- TO DO: is _fct_marketing_touchpoints_consolidated_loaded_at required in fct_observed conversions?
        null::double as client_cancellation_multiplier,
        conversion_count as weight
    from 
        {{ ref("fct_observed_conversions") }} as oc
    where true
        and program = 'chronic'
        and screener_outcome in ('auto_accepted', 'distinguished', 'relevant') -- TO DO: should these be grouped into a single indicator?

),

forecast_by_touchpoint as (

    select 
        touchpoint_id,
        date_trunc('year', activity_date_at) as period,
        customer_id,
        max(_fct_marketing_touchpoints_consolidated_loaded_at) as _fct_marketing_touchpoints_consolidated_loaded_at,
        sum(
            case 
                when activity_date_at < {{ get_run_started_date_at() }} 
                        and activity_type = 5 
                    then weight 
                else 0 
            end
        ) as observed, -- use observed aq approvals before today
        sum(
            case 
                when activity_date_at >= {{ get_run_started_date_at() }} -- use forecasted aq approvals from today forward
                        and activity_type = 7 
                    then weight / coalesce(client_cancellation_multiplier, 1.0) 
                else 0 
            end
        ) as forecasted -- divide out the customer cancellation multiplier because other customers cancelling will not impact remaining customers throttling strategy
    from combined_marketing_events
    group by 1, 2, 3

),

/*
    Pull in customer info, hard and soft caps, and trailing and unattributed forecast estimates for each touchpoint to be used later in throttling calcs
*/
touchpoint_info as (

     select 
        ft.touchpoint_id,
        ft.customer_id,
        cust.is_fully_insured,
        mt.is_mailer_communication,
        ft.forecasted,
        ft.observed,
        observed + forecasted * (1 + coalesce(mm.associated_trailing_multiplier, 0) + coalesce(mm.predicted_unattributed_multiplier, 0)) as weighted_forecasted, -- get the total estimated touchpoints from this
        coalesce(mt.touchpoint_sent_at, ft.period) as touchpoint_sent_at,
        nullif(dep.hard_cap, 0) as hard_cap,
        year(touchpoint_sent_at) as touchpoint_send_year,
        month(touchpoint_sent_at) < 6 as is_touchpoint_in_h1, -- h1 is considered may for this purpose
        mt.first_or_repeat_deployment,
        ft._fct_marketing_touchpoints_consolidated_loaded_at,
        year(act.first_closed_at) as close_year, -- year that we closed the customer
        coalesce(close_year < year(mt.touchpoint_sent_at), false) as is_legacy_customer,
        coalesce(lower(mt.touchpoint_marketing_activity_status) = 'throttled', false) as is_sfdc_throttled,
        act.account_yearly_uot_goal
    from 
        forecast_by_touchpoint as ft
        left join {{ ref("fct_marketing_touchpoints_consolidated") }} as mt
            on ft.touchpoint_id = mt.touchpoint_id
            and mt.touchpoint_sent_at >= date_trunc('year', {{ get_run_started_date_at() }}) - interval 1 years
            and mt.touchpoint_sent_at < date_trunc('year', {{ get_run_started_date_at() }}) + interval 2 years
        left join {{ ref("fct_deployments") }} as dep 
            on mt.deployment_key = dep.deployment_key
        left join {{ ref("fct_customer_trailing_unattributed_multipliers") }} as mm
            on mm.activity_month_at = date_trunc('month', mt.touchpoint_sent_at)
            and mm.customer_id = mt.customer_id
        left join {{ ref("dim_accounts") }} as act 
            on ft.customer_id = act.customer_id
            and nvl(act.customer_id, 0) > 0
        left join {{ ref("dim_customers") }} as cust 
            on ft.customer_id = cust.customer_id
    where true
        and year(ft.period) = year({{ get_run_started_date_at() }})
),

h1_throttled as (

    select 
        row_number() over (partition by customer_id, touchpoint_send_year order by touchpoint_sent_at asc) as touchpoint_number_yearly,
        sum(weighted_forecasted) over (partition by customer_id, touchpoint_send_year order by touchpoint_sent_at asc) as running_sum_yearly,
        row_number() over (partition by customer_id, touchpoint_send_year, is_touchpoint_in_h1 order by touchpoint_sent_at asc) as touchpoint_number_half,
        sum(weighted_forecasted) over (partition by customer_id, touchpoint_send_year, is_touchpoint_in_h1 order by touchpoint_sent_at asc) as running_sum_half,
        not is_touchpoint_in_h1 -- allow all h2 touchpoints to pass for now
            or (not is_fully_insured and is_touchpoint_in_h1 
                and not (is_mailer_communication and _fct_marketing_touchpoints_consolidated_loaded_at + interval 3 weeks > touchpoint_sent_at) 
                    and (
                        coalesce(
                            nullif(base.hard_cap, 0), -- first consider the hard cap if there is one
                            case when is_legacy_customer then 1.7 * account_yearly_uot_goal end, -- if no hard cap, but customer is legacy customer consider 140% of yearly goal regardless of half
                            0.7 * account_yearly_uot_goal
                        ) -- if no hard cap but customer is new customer than only look for 60% of yearly goal in h1
                        + base.weighted_forecasted
                    ) -- include the first touchpoint to take us over the threshold
                    < sum(weighted_forecasted) over (partition by customer_id, touchpoint_send_year, is_touchpoint_in_h1 order by touchpoint_sent_at asc)
                    and touchpoint_sent_at > _fct_marketing_touchpoints_consolidated_loaded_at  -- only throttle touchpoints that have not yet gone out
        ) as is_throttled, -- only truly throttled in h1 so far, h2 is considered later
        base.*
    from 
        touchpoint_info as base
    where true

),

h2_throttled as (
    
    select distinct
         (is_throttled and is_touchpoint_in_h1) 
            or (
                not is_touchpoint_in_h1 and not is_fully_insured and not (is_mailer_communication and _fct_marketing_touchpoints_consolidated_loaded_at + interval 3 weeks > touchpoint_sent_at) 
                    and coalesce(
                            max(case when not is_throttled and is_touchpoint_in_h1 then running_sum_half else 0 end) over(partition by customer_id, touchpoint_send_year), 0) -- get the number forecasted in h1
                            + running_sum_half - weighted_forecasted -- plus all forecasted values before this one
                            > coalesce(
                                    hard_cap, 
                                    (
                                        case 
                                            when is_legacy_customer 
                                                then 1.7 
                                            when first_or_repeat_deployment = 1 
                                                then 1.2 -- for customers launching in h2 only target 60% of annual uot goal
                                            else 1.2 -- for rest of new customers target 130% of uot goal
                                        end 
                                    ) 
                                    * account_yearly_uot_goal -- compare against the hard cap if available, 1.4x uot goal for legacy customers or the uot goal for new customers
                            )  
                    and touchpoint_sent_at > _fct_marketing_touchpoints_consolidated_loaded_at  -- only throttle touchpoints that have not yet gone out that clearly went out
         ) as is_throttled_base,
        touchpoint_id,
        _fct_marketing_touchpoints_consolidated_loaded_at,
        is_sfdc_throttled
    from 
        h1_throttled
    where true

),

final as (

    select

        -- Business Keys
        t.touchpoint_id,

        -- Misc Attributes
        mt.touchpoint_marketing_activity_status,

        -- Indicators
        t.is_throttled_base,
        (t.is_throttled_base or lower(mt.touchpoint_marketing_activity_status) = 'throttled') as is_throttled

    from 
        h2_throttled as t 
        inner join {{ ref("fct_marketing_touchpoints") }} as mt 
            on t.touchpoint_id = mt.touchpoint_id
            and (
                mt.touchpoint_sent_at > mt._fct_marketing_touchpoints_loaded_at
                or coalesce(t.is_sfdc_throttled, false)
            )
            and mt._fct_marketing_touchpoints_loaded_at = t._fct_marketing_touchpoints_consolidated_loaded_at -- TO DO: all of this date joining should not be required since we are working with the current data and then snapshotting it after

)

select *
from final
where true
