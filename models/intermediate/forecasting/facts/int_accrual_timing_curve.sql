with

conversions as (

    select
        mt.is_email_communication,
        mt.is_customer_communication,
        datediff(oc.activity_date_at, mt.touchpoint_sent_at) as days_to_conversion,
        count(1)::integer as conversion_count
    from 
        {{ ref("fct_marketing_touchpoints_consolidated") }} as mt
        inner join {{ ref("fct_observed_conversions") }} as oc
            on mt.touchpoint_id = oc.touchpoint_id
            and (mt.is_email_communication or mt.is_mailer_communication or mt.is_customer_communication)
            and mt.touchpoint_sent_at between {{ get_run_started_date_at() }} - interval 455 day and {{ get_run_started_date_at() }} - interval 90 day
        left join {{ ref("fct_screenings") }} as sc 
            on oc.screening_key = sc.screening_key
        left join {{ ref("fct_pathways") }} as pt 
            on oc.pathway_key = pt.pathway_key
    where true 
        and oc.attribution_type is distinct from 'y2_signups'
        and datediff(oc.activity_date_at, mt.touchpoint_sent_at) >= 0
        and sc.was_application_accepted
        and pt.is_chronic_program
    {{ dbt_utils.group_by(3) }}

),

final as (

    select

        -- Misc Attributes
        is_email_communication,
        is_customer_communication,

        -- Dates
        {{ build_date_key(get_run_started_date_at(), 'forecast_date_at') }},
        {{ get_run_started_date_at() }} as forecast_date_at,

        -- Metrics
        conversion_count,
        days_to_conversion,
        1.0 * conversion_count / sum(conversion_count) over(partition by is_email_communication, is_customer_communication)::double as pct_of_medium
        
    from
        conversions
    where true

)

select *
from final
where true
