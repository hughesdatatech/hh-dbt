{# observed #}

with

final as (

    select 
        
        -- Business Keys
        tm.customer_id,
        
        -- Misc Attributes

        -- Dates
        {{ build_date_key('tm.activity_month_at', 'activity_month_at') }},
        tm.activity_month_at,

        -- Metrics 

        -- Trailing Associated
        tm.associated_multiplier_decile,
        tm.associated_trailing_multiplier,

        -- Trailing Unassociated
        tm.unassociated_multiplier_decile,
        tm.unassociated_trailing_daily_signups,

        -- Unattributed
        um.months_since_2020,
        um.unattributed_conversion_count,
        um.unattributed_multiplier,

        -- Predicted
        umr.predicted_unattributed_multiplier

    from
        {{ ref("int_trailing_multipliers_aggregated") }} as tm
        left join {{ ref("int_unattributed_multipliers_aggregated") }} as um
            on tm.activity_month_at = um.activity_month_at
        left join {{ ref("int_py_predicted_unattributed_multipliers") }} as umr 
            on tm.activity_month_at = umr.activity_month_at
    where true

)

select *
from final
where true
