with 

final as (

    select 
        ac.touchpoint_id,
        ac.customer_id,
        sum(ac.weight) as forecasted_conversions,
        sum(unatt.weight) as forecasted_associated_unattributed_conversions,
        sum(trail.weight) as forecasted_associated_trailing_conversions
    from 
        {{ ref("fct_forecasted_attributed_customer_touchpoint_conversions") }} as ac 
        left join {{ ref("int_associated_trailing_unattributed_base") }} as unatt 
            on ac.customer_id = unatt.customer_id
            and ac.touchpoint_id = unatt.touchpoint_id
            and ac.program = unatt.program
            and ac.sub_program = unatt.sub_program
            and ac.activity_date_at = unatt.activity_date_at
            and unatt.attribution_type = 'unattributed'
       left join {{ ref("int_associated_trailing_unattributed_base") }} as trail 
            on ac.customer_id = trail.customer_id
            and ac.touchpoint_id = trail.touchpoint_id
            and ac.program = trail.program
            and ac.sub_program = trail.sub_program
            and ac.activity_date_at = trail.activity_date_at
            and trail.attribution_type = 'trailing'
    where true
    group by 1, 2

)

select *
from final
where true
