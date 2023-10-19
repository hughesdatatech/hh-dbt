select 

    -- Metadata
    insertion_date as _source_meta_insertion_date,

    -- Business Keys
    account_id,
    name as account_name,

    parent_account_id,
    {# 
        We use a macro to temporarily set any duplicate customer_ids to null that we have configured in the account_client_exclusion_list project variable.
        It requires us to confirm that it is OK to temporarily exclude those customer_ids in order to do a forecast build.
        The source field before renaming is sf_client_id.
    #}
    {{ get_account_customer_id() }}, 
    {#
        We retain the original, unmodified sf_client_id field for testing purposes so that we can continue to warn if there are duplicates.
    #}
    sf_client_id::integer, 

    enrollment_marketing_manager_id,
    enrollment_marketing_manager_name,
    record_type_id,
    health_plan_partner_account_id,
    consultant_account_id,
    consultant_parent,
    client_lead_id as customer_lead_id,
    client_lead_name as customer_lead_name,
    client_support_id as customer_support_id,
    client_support_name as customer_support_name,

    -- Misc Attributes
    billing_street,
    billing_city,
    billing_state,
    billing_postal_code,
    type as account_type,
    industry as customer_industry,
    client_tier as customer_tier,
    case
        when lower(industry) = 'government'
            then industry
        else population_type
    end as population_type,
    eligibility,
    approved_programs,
    contracting_partner,
    case 
        when membership_size in ('100,000+', 'Greater than 50,000')
            then '50,000+'
        else membership_size
    end as customer_membership_size,
    lead_source,
    last_dep_number::smallint,
    carriers,
    eligibility_notes,
    billing_type,
    payment_structure,
    cs_lead_division,
    cs_team_lead,
    client_other_vendor_sends_emails as customer_other_vendor_sends_emails,
    cap_amount_details,
    which_programs_have_a_billing_cap,
    website,
    phone_number,
    pt_ops_referral_url,
    non_standard_program_offering_details,
    lost_customer_reason,

    -- JSON

    -- Indicators
    emerging_account::boolean as is_emerging_account,
    pathways_listed::boolean as has_pathways_listed,
    sensor_language_present::boolean as is_sensor_language_present,
    womens_pelvic_health::boolean as is_womens_pelvic_health,
    cap_recurring_yoy as has_recurring_yoy_cap,
    does_this_clinet_have_billing_caps as has_billing_caps,
    ecp as is_ecp,
   
    -- Dates
    hh_program_launch_date as hh_program_launched_at,
    first_close_date as first_closed_at,
    dmc_programs_launch_date as dmc_programs_launched_at,
    last_launch as last_launch_at,
    next_launch as next_launch_at,
    launch_after_next as launch_after_next_at,
    initial_deployment_launch_date as initial_deployment_launched_at,
    billing_cap_effective_date as billing_cap_effective_at,
    billing_cap_end_date as billing_cap_ended_at,
    cancellation_date as cancelled_at,

    -- Metrics
    total_covered_lives,
    yearly_uot_goal as account_yearly_uot_goal,
    yoy_enrollment_target_percentage as yoy_enrollment_target_pct,
    nvl(formula_eligible_users, 0) as eligible_members,
    annual_medical_spend,
    annual_msk_spend,
    price::double,
    verbal_commit_opps,
    billing_cap_amount,
    market_cap

from 
   {{ source('sfdc_rollups', 'sf_accounts') }}
