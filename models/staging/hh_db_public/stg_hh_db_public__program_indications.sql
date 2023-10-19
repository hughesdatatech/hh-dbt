select 

    -- Metadata
    {{ select_debezium_metadata_cols() }},

    -- Business Keys
    id as program_indication_id,
    identifier as program_indication_identifier,
    lower(identifier) as lower_program_indication_identifier,

    program_id,
    indication_id,

    -- Misc Attributes
    {{ build_program_indication_parts('lower_program_indication_identifier') }},

    -- JSON

    -- Indicators
    {{ build_program_indicators('program', 'indication') }},
    
    -- Dates
    updated_at, 
	created_at

    -- Metrics

from 
    {{ source('hh_db_public', 'public_program_indications') }}
