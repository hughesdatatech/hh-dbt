{%- set accounts_columns = dbt_utils.get_filtered_columns_in_relation(from=ref("int_accounts"), except=[]) -%}
{%- set unique_key = ['account_id'] -%}

with 

accounts as (

    select 
        *,
        'int_accounts' as record_source
    from 
        {{ ref("int_accounts") }}
    where true

    union all

    select 
    {% for column_name in accounts_columns %}
        null,
    {% endfor %}
        'system' as record_source

),

final as (

    select 

        -- Metadata
        _source_meta_insertion_date,

        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='account_key',
            hd_source_model='int_accounts',
            hd_name='account_hd',
            hd_except_cols=unique_key,
            rec_source='record_source',
            alias='_dim_accounts'
        ) }},

        -- Business Keys
        account_id,
        account_name,

        -- Related IDs
        parent_account_id,
        customer_id,
        enrollment_marketing_manager_id,
        enrollment_marketing_manager_name,
        record_type_id,
        health_plan_partner_account_id,
        consultant_account_id,
        consultant_parent,
        customer_lead_id,
        customer_lead_name,
        customer_support_id,
        customer_support_name,

        -- Misc Attributes
        billing_street,
        billing_city,
        billing_state,
        billing_postal_code,
        account_type,
        customer_industry,
        customer_tier,
        population_type,
        eligibility,
        approved_programs,
        contracting_partner,
        customer_membership_size,
        lead_source,
        last_dep_number,
        carriers,
        eligibility_notes,
        billing_type,
        payment_structure,
        cs_lead_division,
        cs_team_lead,
        customer_other_vendor_sends_emails,
        cap_amount_details,
        which_programs_have_a_billing_cap,
        website,
        phone_number,
        pt_ops_referral_url,
        non_standard_program_offering_details,
        lost_customer_reason,

        -- JSON

        -- Indicators
        is_emerging_account,
        has_pathways_listed,
        is_sensor_language_present,
        is_womens_pelvic_health,
        has_recurring_yoy_cap,
        has_billing_caps,
        is_ecp,
    
        -- Dates
        hh_program_launched_at,
        first_closed_at,
        dmc_programs_launched_at,
        last_launch_at,
        next_launch_at,
        launch_after_next_at,
        initial_deployment_launched_at,
        billing_cap_effective_at,
        billing_cap_ended_at,
        cancelled_at,

        -- Metrics
        total_covered_lives,
        account_yearly_uot_goal, 
        yoy_enrollment_target_pct,
        eligible_members,
        annual_medical_spend,
        annual_msk_spend,
        price,
        verbal_commit_opps,
        billing_cap_amount,
        market_cap

    from 
        accounts
    where true

)

select *
from final
where true
