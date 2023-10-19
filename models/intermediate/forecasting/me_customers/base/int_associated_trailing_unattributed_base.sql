/*
    TO DO: this can be turned into a fct_ table if required.
*/

with 

assoc_trailing as (

    select
        ac.touchpoint_id,    
        ac.customer_id,
        'trailing' as attribution_type,
        ac.program,
        ac.sub_program,
        regexp_replace(replace(ac.detail_json, 'attributed', 'trailing'), '"in_forecast_holdout":[^,]+,', '') as detail_json,
        ac.activity_date_at,
        sum(ac.weight) * max(coalesce(tam.associated_trailing_multiplier, 0)) as weight
    from  
        {{ ref("fct_forecasted_attributed_customer_touchpoint_conversions") }} as ac 
        inner join {{ ref("fct_customer_trailing_unattributed_multipliers") }} as tam 
            on ac.customer_id = tam.customer_id
            and date_trunc('month', ac.activity_date_at) = tam.activity_month_at
        left join {{ ref("fct_throttled_touchpoints") }} as tt
            on ac.touchpoint_id = tt.touchpoint_id
    where true
        and not nvl(tt.is_throttled, false)
    {{ dbt_utils.group_by(7) }}

),

assoc_unattributed as (

    select
        ac.touchpoint_id,    
        ac.customer_id,
        'unattributed' as attribution_type,
        ac.program,
        ac.sub_program,
        regexp_replace(replace(ac.detail_json, 'attributed', 'unattributed'), '"in_forecast_holdout":[^,]+,', '') as detail_json,
        ac.activity_date_at,
        sum(ac.weight) * max(coalesce(tam.predicted_unattributed_multiplier, 0)) as weight
    from  
        {{ ref("fct_forecasted_attributed_customer_touchpoint_conversions") }} as ac 
        inner join {{ ref("fct_customer_trailing_unattributed_multipliers") }} as tam 
            on ac.customer_id = tam.customer_id
            and date_trunc('month', ac.activity_date_at) = tam.activity_month_at
        left join {{ ref("fct_throttled_touchpoints") }} as tt
            on ac.touchpoint_id = tt.touchpoint_id
    where true
        and not nvl(tt.is_throttled, false)
    {{ dbt_utils.group_by(7) }}

),

final as (

    select *
    from assoc_trailing
    where true

    union all 

    select *
    from assoc_unattributed
    where true

)

select 
    'int_associated_trailing_unattributed_base' as record_source,
    *
from 
    final
where 
    true
