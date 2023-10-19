with

/*
    Pull in observed conversions, matching to the nearest touchpoint.
*/
final as (

    select

        -- Business Keys
        obs.touchpoint_id,
        obs.marketing_id,
        memb.member_id,
        obs.screening_id,
        obs.member_uuid,
        obs.customer_id,
        act.account_id,
        mt.deployment_id,
        pt.pathway_id,

        -- Misc Attributes
        obs.screener_outcome,
        obs.program,
        obs.sub_program,
        obs.activity_type,
        obs.attribution_type,
        obs.trailing_attribution_type,
        obs.detail_json,

        -- Dates
        obs.activity_date_at_key,
        obs.activity_date_at,
        obs.activity_timestamp_at,
        obs._fct_forecasted_accrued_customer_touchpoint_conversions_loaded_at,
        obs._fct_marketing_touchpoints_consolidated_loaded_at,

        -- Metrics
        obs.conversion_count,
        obs.attributed_conversion_count,
        obs.trailing_conversion_count,
        obs.unattributed_conversion_count,
        obs.trailing_associated_conversion_count,
        obs.trailing_unassociated_conversion_count,
        obs.y2_signups_conversion_count,
        obs.enso_conversion_count

    from 
        -- ultimately replace the stg model with hinge-airflow/lib/views/sql/marketing_events_observed_forecasted.sql
        {{ ref("stg_xsrc_rollups__marketing_events_observed_forecasted") }} as obs 
        left join {{ ref("stg_hh_db_public__members") }} as memb 
            on obs.member_uuid = memb.member_uuid
        left join {{ ref("stg_sfdc_rollups__accounts") }} as act 
            on obs.customer_id = act.customer_id
        left join {{ ref("stg_sfdc_rollups__marketing_touchpoints") }} as mt 
            on obs.touchpoint_id = mt.touchpoint_id
        left join {{ ref("stg_hh_db_rollups__screenings") }} as sc 
            on obs.screening_id = sc.screening_id
        left join {{ ref("stg_hh_db_rollups__pathways") }} as pt 
            on sc.pathway_id = pt.pathway_id
    where true
        and activity_type = 5
    
)

select *
from final
where true
