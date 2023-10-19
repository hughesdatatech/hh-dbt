{%- set tp_history_start_at = "'2021-11-15'::date" -%}
{%- set obs_conversions_start_at = "'2022-01-01'::date" -%}

with 

tp_history as (

    select 
        touchpoint_id,
        original_loaded_at > original_touchpoint_sent_at as sneaky_tp -- looking for tp that were sent before they first appeared on our marketing schedule
    from 
        {{ ref("fct_marketing_touchpoints_status_tracking") }}
    where true
        and original_loaded_at >= {{ tp_history_start_at }}
   
),

pct_sneaky_per_period as (

    select
        month(oc.activity_date_at) as activity_month_at,
        sum(
            case 
                when th.sneaky_tp 
                    then oc.conversion_count 
                else 0 
            end
        ) / sum(oc.conversion_count) 
        * case 
            when month(oc.activity_date_at) = 1 
                then 0.5 
            else 1.0
        end as pct_sneaky_signups, -- only allow half of jan sneaky touchpoint volume
        sum(
            case 
                when th.sneaky_tp 
                    then oc.conversion_count 
                else 0 
            end
        ) as sneaky_signups,
        sum(oc.conversion_count) as total_signups
    from 
        {{ ref("fct_observed_conversions") }} as oc
        inner join tp_history as th
            on th.touchpoint_id = oc.touchpoint_id
    where true
        and oc.activity_date_at between {{ obs_conversions_start_at }} and {{ get_run_started_date_at() }}
    group by 1
  
),

{#
    Define common join and filter to limit the unioned subqueries in the cte below.
    This is faster than unioning first and then filtering.
#}
{%- set att_trail_unatt_filter -%}

        and tp.activity_date_at >= date_trunc('week', {{ get_run_started_date_at() }}) + interval 1 weeks
        and month(tp.activity_date_at) = 1 -- only apply topline assumption for january 

{%- endset -%}

att_trail_unatt as (

    select
        tp.touchpoint_id,
        tp.program,
        tp.activity_date_at,
        month(tp.activity_date_at) as activity_month_at,
        tp.weight
    from 
        {{ ref("fct_forecasted_attributed_customer_touchpoint_conversions") }} as tp
        left join {{ ref("fct_throttled_touchpoints") }} as stat 
            on tp.touchpoint_id = stat.touchpoint_id
    where true
        and not nvl(stat.is_throttled, false)
        {{ att_trail_unatt_filter }}
        
    union all

    select
        null::integer as touchpoint_id,
        tp.program,
        tp.activity_date_at,
        month(tp.activity_date_at) as activity_month_at,
        tp.weight
    from 
        {{ ref("int_trailing_unattributed") }} as tp
    where true
        {{ att_trail_unatt_filter }}

),

/*
    Add in unplanned aka "sneaky" touchpoints not accounted for until after the scheduled send date.
*/
sneaky_tps as (

    select
        'int_unplanned_touchpoints' as record_source,
        'topline_adjustments' as attribution_type,
         tp.program,
        'unplanned_touchpoints' as adjustment_type,
        tp.activity_date_at,
        sum(tp.weight * nvl(spp.pct_sneaky_signups, 0.008)) as weight
    from 
        att_trail_unatt as tp
        left join pct_sneaky_per_period as spp
            on tp.activity_month_at = spp.activity_month_at
    where true
    {{ dbt_utils.group_by(5) }}

),

final as (

    select
        record_source,
        attribution_type,
        program,
        adjustment_type,
        to_json(
            named_struct(
                'attribution_type', attribution_type,
                'program', program,
                'adjustment_type',adjustment_type
            )
        ) as detail_json,
        activity_date_at,
        weight
    from 
        sneaky_tps
    where true


)

select *
from final
where true
