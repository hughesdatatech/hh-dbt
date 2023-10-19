with 

all_chronic as (

    select *
    from {{ ref("int_chronic_core_cv_only") }}
    where true

    union all

    select *
    from {{ ref("int_chronic_cv_only_cannibals") }}
    where true

    union all

    select *
    from {{ ref("int_chronic_wph") }}
    where true

    union all

    select *
    from {{ ref("int_chronic_wph_cannibals") }}
    where true

),

/*
    CTE will be used to adjust the weights based on estimated pct lift from aq shortening and automating clinical review.
*/
lift_adjustments as (

    select
        *,
        null::double as aq_shortening_adjustment_multiplier, -- lift from aq shortening is no longer applied, but retaining field in final output
        1 + coalesce(hcm_cr.value, 0) as automating_clinical_review_adjustment_multiplier
    from 
        all_chronic as ac 
        left join {{ ref("ref_forecast_hard_coded_multiplier_scd") }} as hcm_cr 
            on hcm_cr.field_name = 'pct_lift_for_automating_clinical_review'
            and ac.activity_date_at between hcm_cr.signups_starting and hcm_cr.signups_ending
            and {{ get_run_started_date_at() }} between hcm_cr.valid_from and hcm_cr.valid_through
    where true

),

final as (

    select

        -- Metadata
        la.record_source, 

        -- IDs
        la.touchpoint_id,
        la.customer_id,

        -- Misc Attributes
        la.attribution_type,
        la.program,
        la.sub_program,

        -- JSON
        la.detail_json,

        -- Indicators
        la.is_in_holdout,

        -- Dates
        la.activity_date_at,
        la._fct_forecasted_accrued_customer_touchpoint_conversions_loaded_at,
        la._fct_marketing_touchpoints_consolidated_loaded_at,

        -- Metrics
        la.movement_multiplier, 
        la.cancellation_multiplier,
        la.cannibalization_adjustment_multiplier,
        la.aq_shortening_adjustment_multiplier,
        la.automating_clinical_review_adjustment_multiplier,
        la.weight * la.automating_clinical_review_adjustment_multiplier as weight

    from 
        lift_adjustments as la
    where true

)

select *
from final
where true
