/*
    Add acute program forecast as % of chronic forecast when expected signup date is greater than dmc launch date.
*/
with final as (

    select
        cc.activity_date_at,
        cc.weight * coalesce(hcm.value, 0) as weight, -- assuming acute accounts for incremental percentage of chronic volume on dmc clients, based on core chronic performance and annual rate set by Megan and forecast team
        cc.touchpoint_id,
        cc.customer_id,
        cc.is_in_holdout,
        cc._fct_forecasted_accrued_customer_touchpoint_conversions_loaded_at,
        cc.attribution_type,
        'acute' as program,
        cc.sub_program,
        cc._fct_marketing_touchpoints_consolidated_loaded_at,
        cc.movement_multiplier, 
        cc.cancellation_multiplier,
        replace(cc.detail_json, 'chronic', 'acute') as detail_json
    from 
        {{ ref("int_chronic_core_base") }} as cc
        inner join {{ ref("dim_accounts") }} as act 
            on cc.customer_id = act.customer_id
            and act.dmc_programs_launched_at <= cc.activity_date_at
            and nvl(act.customer_id, 0) > 0
        {{ build_hardcoded_multiplier_join('acute_to_chronic_ratio', 'cc', true, true) }}
        where true
   
)

select *
from final
where true
