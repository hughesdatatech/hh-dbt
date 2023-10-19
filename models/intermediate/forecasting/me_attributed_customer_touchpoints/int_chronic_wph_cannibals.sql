with 

final as (

    select
        cc.activity_date_at,
        null::double as cannibalization_adjustment_multiplier,
        cc.weight * coalesce(hcm.value, 0.0) as weight, -- assuming 9.6% of chronic core will ultimately enroll in wph, with some cannibalized from other core programs
        cc.touchpoint_id,
        cc.customer_id,
        cc.is_in_holdout,
        cc._fct_forecasted_accrued_customer_touchpoint_conversions_loaded_at,
        cc.attribution_type,
        cc.program,
        'womens_public_health' as sub_program,
        cc._fct_marketing_touchpoints_consolidated_loaded_at,
        cc.movement_multiplier, 
        cc.cancellation_multiplier,
        replace(cc.detail_json, 'core', 'womens_public_health') as detail_json
    from 
        {{ ref("int_chronic_core_base") }} as cc
        inner join {{ ref("int_chronic_womens_pelvic_health_customers") }} as wph 
            on cc.customer_id = wph.customer_id
            and cc.activity_date_at >= wph.wph_program_started_at
        {{ build_hardcoded_multiplier_join('wph_to_chronic_ratio', 'cc', true, true) }}
    where true

)

select 
    *,
    'int_chronic_wph_cannibals' as record_source
from 
    final
where 
    true
