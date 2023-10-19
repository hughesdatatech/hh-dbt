with

/*
    CTE will be used to adjust the weights on those acute core touchpoints estimated to be cannibalized by computer vision.
*/
cannibalization_adjustments as (

    select 
        cv.customer_id,
        -- (1 - coalesce(hcm.value, 0.0)) as adjustment_multiplier, 
        -- TO DO: hcm has no impact here, why is this hardcoded and not selected from the hcm table?
        1 - 0.294::double as adjustment_multiplier, -- assume % of acute signups will be cannibalized into chronic, set by megan and cv team
        hcm.signups_starting,
        hcm.signups_ending,
        cv.cv_program_started_at
    from 
        {{ ref("int_chronic_computer_vision_customers") }} as cv 
        {{ build_hardcoded_multiplier_join('pct_acute_cannibalized_to_chronic_from_ecp', '', false, true) }}
    where true

),

ac_base as (

    select
        ac.activity_date_at,
        nvl(ca.adjustment_multiplier, 1.0) as cannibalization_adjustment_multiplier,
        cannibalization_adjustment_multiplier * ac.weight as weight,
        ac.touchpoint_id,
        ac.customer_id,
        ac.is_in_holdout,
        ac._fct_forecasted_accrued_customer_touchpoint_conversions_loaded_at,
        ac.attribution_type,
        ac.program,
        ac.sub_program,
        ac._fct_marketing_touchpoints_consolidated_loaded_at,
        ac.movement_multiplier, 
        ac.cancellation_multiplier,
        ac.detail_json
    from 
        {{ ref("int_acute_core_base") }} as ac 
        left join cannibalization_adjustments as ca 
            on ac.customer_id = ca.customer_id
            and ac.activity_date_at >= ca.cv_program_started_at
            and ac.activity_date_at between ca.signups_starting and ca.signups_ending
    where true

),

final as (

    select

        -- Metadata
        'int_acute_core' as record_source, 

        -- IDs
        touchpoint_id,
        customer_id,

        -- Misc Attributes
        attribution_type,
        program,
        sub_program,

        -- JSON
        detail_json,

        -- Indicators
        is_in_holdout,

        -- Dates
        activity_date_at,
        _fct_forecasted_accrued_customer_touchpoint_conversions_loaded_at,
        _fct_marketing_touchpoints_consolidated_loaded_at,

        -- Metrics
        movement_multiplier, 
        cancellation_multiplier,
        cannibalization_adjustment_multiplier,
        null::double as aq_shortening_adjustment_multiplier, -- n/a for acute
        null::double as automating_clinical_review_adjustment_multiplier, -- n/a for acute
        weight

    from 
        ac_base
    where true

)

select *
from final
where true
