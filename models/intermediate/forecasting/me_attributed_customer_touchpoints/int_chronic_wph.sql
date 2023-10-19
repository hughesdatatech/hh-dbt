with 

final as (

    select
        wph.activity_date_at,
        null::double as cannibalization_adjustment_multiplier,
        wph.weight,
        wph.touchpoint_id,
        wph.customer_id,
        wph.is_in_holdout,
        wph._fct_forecasted_accrued_customer_touchpoint_conversions_loaded_at,
        wph.attribution_type,
        wph.program,
        wph.sub_program,
        wph._fct_marketing_touchpoints_consolidated_loaded_at,
        wph.movement_multiplier, 
        wph.cancellation_multiplier,
        wph.detail_json
    from 
        {{ ref("int_chronic_wph_base") }} as wph
    where true

)

/*
    Represents the estimated subset of touchpoint conversions that will be organic wph.
*/
select 
    *,
    'int_chronic_wph' as record_source
from 
    final
where 
    true 
