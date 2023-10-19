select

    -- Metadata
    {{ select_debezium_metadata_cols() }},

    -- Business Keys
    id as partnership_id,

    -- Misc Attributes
    name as partnership_name,
    partnership_type,

    -- JSON

    -- Indicators
    requires_state_credentials,
    allow_cohort_expansion as allows_cohort_expansion,

    -- Dates
    created_at,
    updated_at
  		
    -- Metrics

from 
    {{ source('hh_db_public', 'public_partnerships') }}
