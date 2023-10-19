select 

    -- Metadata
    {{ select_debezium_metadata_cols() }},
    
    -- Business Keys
    id as pathway_id,
    uuid as pathway_uuid,

    pathway_configuration_id, 
	program_indication_id,
    team_id,
    user_id as member_id,
    indication_id,

    -- Misc Attributes
    phase, 
	stage, 
	stream, 
	state, 

    -- JSON
    
    -- Indicators
    primary as is_primary,
    activity_engine_enabled as is_activity_engine_enabled,

    -- Dates
    starts_at as started_at, 
    first_et_at, 
    created_at, 
    transferred_to_coach_at,
    accepted_at,
    coach_transfer_attempted_at,
    updated_at,
    completed_sensor_setup_at,
    ae_ftu_completed_at

    -- Metrics

from 
    {{ source('hh_db_public', 'public_pathways') }}
