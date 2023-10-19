select 

    -- Metadata
    last_refreshed_date as _last_refreshed_at,
    
    -- Business Keys
    pathway_id,
    
    recruitment_id,
    recruitment_name,
    user_id as member_id,
    indication_id,
    pathway_indication_name,
    program_indication_identifier,
    lower(program_indication_identifier) as lower_program_indication_identifier,
    team_id,
    stream,
    pathway_configuration_id,

    -- Misc Attibutes
    {{ build_program_indication_parts('lower_program_indication_identifier') }},
    current_phase, 

    -- JSON
    
    -- Indicators
    phoenix as is_phoenix, -- TO DO: name?
    {{ build_program_indicators('program', 'indication') }},

    -- Derived Attributes Using Indicators
    case 
        when is_chronic_program or is_perisurgical_program
            then 'chronic-surgery'
        else program
    end as program_cohort_grouping,

    -- Dates
    starts_at, 
    pathway_start_date as pathway_started_at, 
    first_activity_date as first_activity_at, 
    last_activity_date as last_activity_at, 
    core_end_date as core_ended_at, 
    maintain_start_date as maintain_started_at, 
    aq_approval_date as aq_approved_at, -- Synonyms
    aq_approval_date as pathway_accepted_at, -- Synonyms
    pathway_first_login as pathway_first_login_at,
    first_et as first_et_at

    -- Metrics

from 
    {{ source('hh_db_rollups', 'pathways_view') }}
