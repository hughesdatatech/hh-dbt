with 

final as (

    select

        -- Metadata
        {{ select_standard_metadata_cols('fct_observed_conversions') }},

        -- Unique Key
        oc.observed_conversion_key,

        -- Dimension Keys
        oc.activity_date_at_key,
        sc.screening_created_at_key,
        oc.member_key,
        oc.customer_key,
        oc.account_key,
        
        -- Related Fact Keys
        oc.screening_key,
        oc.deployment_key,
        
         -- Business Keys
        oc.touchpoint_id,
        oc.member_id,
        oc.customer_id,
        oc.account_id,
        oc.screening_id,
        oc.deployment_id, 
        oc.pathway_id,

         -- Misc Attributes,
        oc.screener_outcome,
        oc.program,
        oc.sub_program,
        oc.activity_type,
        oc.attribution_type,
        oc.trailing_attribution_type,
        oc.detail_json,

        -- Add in Members attributes + more
        memb.member_uuid,
        memb.marketing_id,
        -- etc.

        -- Add in Customers attributes + more
        cust.customer_name,
        -- etc.

        -- Add in Accounts attributes + more
        act.account_name,

        -- Add in Screenings attributes + more
        sc.was_application_accepted,

        -- Add in Deployments attributes + more

        -- Add in Pathways attributes + more
        pt.is_chronic_program,

        -- Indicators

        -- Dates
        oc.activity_date_at,
        oc.activity_timestamp_at,
        oc._fct_forecasted_accrued_customer_touchpoint_conversions_loaded_at,
        oc._fct_marketing_touchpoints_consolidated_loaded_at,
        sc.screening_created_at,

        -- Add in Activity Date attributes + more
        activity_dt.day_of_week_name,
        activity_dt.month_name,
        activity_dt.quarter_of_year,
        -- etc.

        -- Metrics
        oc.conversion_count,
        oc.attributed_conversion_count,
        oc.trailing_conversion_count,
        oc.unattributed_conversion_count,
        oc.trailing_associated_conversion_count,
        oc.trailing_unassociated_conversion_count,
        oc.y2_signups_conversion_count,
        oc.enso_conversion_count

    from
        {{ ref("fct_observed_conversions") }} as oc
        inner join {{ ref("dim_dates") }} as activity_dt 
            on oc.activity_date_at_key = activity_dt.date_key
        inner join {{ ref("dim_members") }} as memb 
            on oc.member_key = memb.member_key
        inner join {{ ref("dim_customers") }} as cust 
            on oc.customer_key = cust.customer_key
        inner join {{ ref("dim_accounts") }} as act 
            on oc.account_key = act.account_key
        left join {{ ref("fct_screenings") }} as sc 
            on oc.screening_key = sc.screening_key
        left join {{ ref("fct_deployments") }} as dep 
            on oc.deployment_key = dep.deployment_key
        left join {{ ref("fct_pathways") }} as pt 
            on oc.pathway_key = pt.pathway_key
    where true

)

select *
from final
where true
