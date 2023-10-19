with 

latest_forecast as (

    select
        lf.activity_date_at,
        lf.forecasted_conversions,
        lf.touchpoint_id,
        lf.customer_id,
        lf.in_holdout,
        lf._fct_forecasted_accrued_customer_touchpoint_conversions_loaded_at,
        case
            when lower(mt.content_type) = 'womens pelvic health'
                then 'womens_public_health' -- apparently a mistake that is safer to live with
            else 'core'
        end as sub_program
    from 
        {{ ref("fct_forecasted_accrued_customer_touchpoint_conversions") }} as lf
        inner join {{ ref("fct_marketing_touchpoints_consolidated") }} as mt 
            on lf.touchpoint_id = mt.touchpoint_id
        where true

),

tp_summary as (

    select
        touchpoint_id,
        original_loaded_at as first_appearance_at,
        latest_loaded_at as latest_appearance_at,
        latest_touchpoint_sent_at as current_sent_at,
        year(latest_touchpoint_sent_at) as current_send_year,
        latest_touchpoint_marketing_activity_status as latest_status,
        datediff(current_sent_at, {{ get_run_started_date_at() }}) as days_to_send,
        least(greatest(days_to_send, -7), 120) as cancellation_days_to_send
    from 
        {{ ref("fct_marketing_touchpoints_status_tracking") }}
    where true

),

multiplier_awaiting as (

    select
        days_to_send,
        pct_of_cancellation_to_apply,
        forecast_year
    from 
        {{ ref("ref_forecast_cancellation_timing_curve") }} 
    where true
        and touchpoint_status = 'Awaiting Approval'    

),

multiplier_in_year as (

    select
        days_to_send,
        pct_of_cancellation_to_apply,
        forecast_year
    from 
        {{ ref("ref_forecast_cancellation_timing_curve") }} 
    where true
        and touchpoint_status = 'approved_in_year' 

),

multiplier_previous_year as (

    select
        days_to_send,
        pct_of_cancellation_to_apply,
        forecast_year
    from 
        {{ ref("ref_forecast_cancellation_timing_curve") }} 
    where true
        and touchpoint_status = 'approved_previous_year' 

),

joined as (

    select 
        lf.activity_date_at,
        lf.forecasted_conversions,
        lf.touchpoint_id,
        lf.customer_id,
        lf.in_holdout as is_in_holdout,
        lf._fct_forecasted_accrued_customer_touchpoint_conversions_loaded_at,
        'attributed' as attribution_type,
        'chronic' as program,
        lf.sub_program,
        tph._fct_marketing_touchpoints_consolidated_loaded_at,
        case
            when extract('month', tps.current_sent_at) <= 6 
                    and tph.touchpoint_sent_at > {{ get_run_started_date_at() }}
                then -.025 
            when extract('month', tps.current_sent_at) > 6 
                    and tph.touchpoint_sent_at > {{ get_run_started_date_at() }} 
                then .02 
            else 0 
        end::double as movement_multiplier, -- account for marketing movement
        case 
            when tps.latest_status = 'Awaiting Approval' 
                then 0.135 * ma.pct_of_cancellation_to_apply 
            when tps.latest_status = 'Approved' 
                    and date_trunc('year', tps.current_sent_at) = date_trunc('year', tps.first_appearance_at) 
                then 0.0136 * my.pct_of_cancellation_to_apply
            when tps.latest_status = 'Approved' 
                    and date_trunc('year', tps.current_sent_at) > date_trunc('year', tps.first_appearance_at) 
                then 0.0808 * mp.pct_of_cancellation_to_apply
            else 0 
        end as cancellation_multiplier,
        to_json(
            named_struct(
                'in_forecast_holdout', is_in_holdout,
                'forecast_run_date', lf._fct_forecasted_accrued_customer_touchpoint_conversions_loaded_at,
                'attribution_type', attribution_type,
                'program', program,
                'sub_program', lf.sub_program,
                'touchpoint_data_date', tph._fct_marketing_touchpoints_consolidated_loaded_at,
                'movement_multiplier', movement_multiplier,             
                'cancellation_multiplier', cancellation_multiplier
            )
        ) as detail_json
    from 
        latest_forecast as lf
        left join {{ ref("fct_marketing_touchpoints_consolidated") }} as tph
            on lf.touchpoint_id = tph.touchpoint_id
        left join tp_summary as tps -- used to determine the discount applied to the touchpoint
            on lf.touchpoint_id = tps.touchpoint_id 
        left join multiplier_awaiting as ma 
            on tps.cancellation_days_to_send = ma.days_to_send
            and tps.current_send_year = ma.forecast_year
        left join multiplier_in_year as my 
            on tps.cancellation_days_to_send = my.days_to_send
            and tps.current_send_year = my.forecast_year
        left join multiplier_previous_year as mp 
            on tps.cancellation_days_to_send = mp.days_to_send
            and tps.current_send_year = mp.forecast_year
),

final as (

    select 
        activity_date_at,
        /*
            Discount touchpoints based on the liklihood they will be canceled down the road.
            https://tableau.analytics.hingehealth.com/#/views/MarketingMovementAnalysis/Summary?:iid=1
            TO DO: Discount this based on the decay curve from the link above (the closer the touchpoint, the more likely it is to go out).
        */
        forecasted_conversions * (1 + coalesce(movement_multiplier, 0) - coalesce(cancellation_multiplier, 0)) as weight,
        touchpoint_id,
        customer_id,
        is_in_holdout,
        _fct_forecasted_accrued_customer_touchpoint_conversions_loaded_at, -- TO DO: check, why is this required?
        attribution_type,
        program,
        sub_program,
        _fct_marketing_touchpoints_consolidated_loaded_at, -- TO DO: check, why is this required?
        movement_multiplier, 
        cancellation_multiplier,
        detail_json
    from 
        joined

)

select *
from final
where true
