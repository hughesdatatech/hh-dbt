with

/*
    CTE will be used to adjust the weights on those chronic core touchpoints estimated to be cannibalized by wph.
*/
cannibalization_adjustments as (

    select 
        wph.customer_id,
        (1 - coalesce(hcm.value, 0.0)) as adjustment_multiplier, -- assume 7% of chronic signups that head to wph are cannibalized (5.9% back plus 1.1% hip)
        hcm.signups_starting,
        hcm.signups_ending,
        wph.wph_program_started_at
    from 
        {{ ref("int_chronic_womens_pelvic_health_customers") }} as wph 
        {{ build_hardcoded_multiplier_join('pct_wph_cannibalized_from_chronic', '', false, true) }}
    where true

), 

chronic_core as (

    select
        cc.activity_date_at,
        nvl(ca.adjustment_multiplier, 1.0) as cannibalization_adjustment_multiplier,
        cannibalization_adjustment_multiplier * cc.weight as weight,
        cc.touchpoint_id,
        cc.customer_id,
        cc.is_in_holdout,
        cc._fct_forecasted_accrued_customer_touchpoint_conversions_loaded_at,
        cc.attribution_type,
        cc.program,
        cc.sub_program,
        cc._fct_marketing_touchpoints_consolidated_loaded_at,
        cc.movement_multiplier, 
        cc.cancellation_multiplier,
        cc.detail_json
    from 
        {{ ref("int_chronic_core_base") }} as cc 
        left join cannibalization_adjustments as ca 
            on cc.customer_id = ca.customer_id
            and cc.activity_date_at >= ca.wph_program_started_at
            and cc.activity_date_at between ca.signups_starting and ca.signups_ending
    where true

),

first_acute as (

    select
        customer_id,
        min(activity_date_at) as first_acute_activity_date_at
    from 
        {{ ref("int_acute_core_base") }}
    where true
    group by 1

),

cv_only as (

    select
        cc.activity_date_at,
        null::double as cannibalization_adjustment_multiplier,
        cc.weight * coalesce(hcm.value, 0.0) as weight, -- assuming incremental % of chronic core will enroll due to additional cv only pathways once available, typically cannibalized from acute, but this section handles when acute is not available for customer
        cc.touchpoint_id,
        cc.customer_id,
        cc.is_in_holdout,
        cc._fct_forecasted_accrued_customer_touchpoint_conversions_loaded_at,
        cc.attribution_type,
        cc.program,
        'cv_only' as sub_program,
        cc._fct_marketing_touchpoints_consolidated_loaded_at,
        cc.movement_multiplier, 
        cc.cancellation_multiplier,
        replace(cc.detail_json, 'core', 'cv_only') as detail_json
    from 
        chronic_core as cc
        inner join first_acute as fa 
            on cc.customer_id = fa.customer_id  
            and fa.first_acute_activity_date_at > cc.activity_date_at -- only include signups before customer first acute showing
        inner join {{ ref("int_chronic_computer_vision_customers") }} as cv 
            on cc.customer_id = cv.customer_id
            and cc.activity_date_at >= cv.cv_program_started_at
        {{ build_hardcoded_multiplier_join('pct_incremental_chronic_from_ecp', 'cc', true, true) }}
    where true
     
),

final as (

    select 
        *,
        'int_chronic_core' as record_source
    from
        chronic_core
    where true
   
    union all

    select 
        *,
         'int_chronic_cv_only' as record_source
    from
        cv_only
    where true

)
    
select *
from final
where true
