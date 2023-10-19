with 

final as (

    select
        ac.activity_date_at,
        null::double as cannibalization_adjustment_multiplier,
        ac.weight * coalesce(hcm.value, 0.0) as weight, -- assuming some % of acute will ultimately enroll in chronic once additional cv options are available, this strictly adds the chronic projections where acute is available
        ac.touchpoint_id,
        ac.customer_id,
        ac.is_in_holdout,
        ac._fct_forecasted_accrued_customer_touchpoint_conversions_loaded_at,
        ac.attribution_type,
        'chronic' as program,
        'cv_only' as sub_program,
        ac._fct_marketing_touchpoints_consolidated_loaded_at,
        ac.movement_multiplier, 
        ac.cancellation_multiplier,
        replace(replace(ac.detail_json, 'acute', 'chronic'), 'core', 'cv_only') as detail_json
    from 
        {{ ref("int_acute_core_base") }} as ac
        inner join {{ ref("int_chronic_computer_vision_customers") }} as cv 
            on ac.customer_id = cv.customer_id
            and ac.activity_date_at >= cv.cv_program_started_at
        {{ build_hardcoded_multiplier_join('pct_acute_cannibalized_to_chronic_from_ecp', 'ac', true, true) }}
    where true
        
)

select 
    *,
    'int_chronic_cv_only_cannibals' as record_source
from 
    final
where 
    true
