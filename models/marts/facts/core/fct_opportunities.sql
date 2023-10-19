{%- set unique_key = ['opportunity_id'] -%}

with 

final as (

    select

        -- Metadata
        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='opportunity_key',
            hd_source_model='int_opportunities',
            hd_except_cols=unique_key,
            rec_source="'int_opportunities'",
            alias='_fct_opportunities'
        ) }},

        -- Dimension Keys
        {{ build_hash_value(value=build_hash_diff(cols=['account_id']), alias='account_key') }},
        closed_at_key,
        created_at_key,
        launched_at_key,

        -- Business Keys
        opportunity_id,
        account_id,

        -- Misc Attributes
        name,
        record_type_id,
        stage_name,
        lead_source,
        forecast_category_name,
        campaign_id,
        campaign_name,
        owner_name,
        created_by_id,
        loss_reason,
        loss_reason_details,
        opportunity_type,
        deployment_number,
        line_of_business,

        -- JSON

        -- Indicators
    
        -- Dates
        closed_at,
        created_at,
        launched_at,

        -- Metrics
        opportunity_amount,
        opportunity_cap_amount,
        opportunity_covered_lives,
        target_enrollment,
        users_on_team_goal,
        efile_covered_lives,
        source_opportunity_covered_lives

    from 
        {{ ref("int_opportunities") }} as dp -- TO DO: join to dim_accounts to get account_key if implementing type-2 scds
    where true

)

select *
from final
where true
