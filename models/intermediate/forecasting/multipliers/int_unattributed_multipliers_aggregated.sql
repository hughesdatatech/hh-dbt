with 

attributions_by_month as (

    select 
        date_trunc('month', oc.activity_timestamp_at)::date as activity_month_at,
        sum(oc.attributed_conversion_count) as attributed_conversion_count,
        sum(oc.unattributed_conversion_count) as unattributed_conversion_count
    from  
        {{ ref("fct_observed_conversions") }} oc
        inner join {{ ref("fct_screenings") }} as sc 
            on oc.screening_id = sc.screening_id
    where true
        and sc.screening_created_at >= '{{ var("trailing_unattributed_multiplier_key_date_2") }}'::timestamp
        and sc.screening_created_at <= date_trunc('month', {{ get_run_started_date_at()}} )::timestamp
    group by 1
           
),

final as (

    select
        activity_month_at,
        months_between(activity_month_at, '{{ var("trailing_unattributed_multiplier_key_date_1") }}'::date) as months_since_2020,
        sum(unattributed_conversion_count)::integer as unattributed_conversion_count,
        (1.0 * sum(unattributed_conversion_count) / sum(attributed_conversion_count))::double as unattributed_multiplier
    from 
        attributions_by_month
    where true
        and activity_month_at < '2023-06-01' -- temporarily excluding June 2023 and after due to data issues
    group by 1
    having 
        sum(attributed_conversion_count) * 0.6 > sum(unattributed_conversion_count) -- more attributed than unattributed

)

select * 
from final
where true
