with

curve_joined_with_predictions as (

    select

        -- Metadata
        cr._meta_model_type,
        cr._meta_model_version,
        cr._meta_model_features,

        -- IDs
        cr.customer_id,
        cr.touchpoint_id,

        -- Misc Attributes
        cr.in_holdout,

        -- Dates
        dateadd(day, ceiling(tc.days_to_conversion), cr.touchpoint_sent_at) as activity_date_at,

        -- Metrics
        tc.pct_of_medium * cr.predicted_conversion_rate * cr.approximate_scheduled_send_count as forecasted_conversions
        
    from
        {{ ref("fct_accrual_timing_curve") }} as tc 
        inner join {{ ref("fct_predicted_touchpoint_conversion_rates") }} as cr
            on tc.is_email_communication = cr.is_email_communication
            and tc.is_customer_communication = cr.is_customer_communication
    where true

),

final as (

    select

        -- Metadata
        _meta_model_type,
        _meta_model_version,
        _meta_model_features,

        -- IDs
        customer_id,
        touchpoint_id,

        -- Misc Attributes
        in_holdout,

        -- Dates
        case
            when activity_date_at > ({{ get_run_started_date_at() }} - interval 1 weeks) and activity_date_at < ({{ get_run_started_date_at() }} + interval 2 weeks)
                then activity_date_at
            when month(activity_date_at) > month((date_sub(activity_date_at, weekday(activity_date_at))))
                then date_sub(activity_date_at, day(activity_date_at) - 1)
            else date_sub(activity_date_at, weekday(activity_date_at))
        end as activity_date_at,

        sum(forecasted_conversions) as forecasted_conversions

    from 
        curve_joined_with_predictions
    where
        true
    {{ dbt_utils.group_by(n=7) }}

)

select *
from final
where true
