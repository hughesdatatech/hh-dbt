select 

    -- Metadata
    insertion_date as _source_meta_insertion_date,

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
    type as opportunity_type,
    deployment_number,
    line_of_business,

    -- JSON

    -- Indicators
    
    -- Dates
    {{ build_date_key('close_date', 'closed_at') }},
    close_date as closed_at,
    {{ build_date_key('created_date', 'created_at') }},
    created_date as created_at,
    {{ build_date_key('launch_date', 'launched_at') }},
    launch_date as launched_at,

    -- Metrics
    amount as opportunity_amount,
    cap_amount as opportunity_cap_amount,
    nvl(total_covered_lives, 0) as opportunity_covered_lives, -- TO DO: Why not use the opportunity_covered_lives already in table, what's difference?
    target_enrollment,
    users_on_team_goal,
    efile_covered_lives,
    opportunity_covered_lives as source_opportunity_covered_lives

from 
   {{ source('sfdc_rollups', 'sf_opportunities') }}
