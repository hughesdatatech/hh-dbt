select

    -- Metadata
    {{ select_debezium_metadata_cols() }},

    -- Business Keys
    id as line_of_business_id,

    -- Misc Attributes
    name as line_of_business_name,

    -- JSON

    -- Indicators

    -- Dates
    created_at,
    updated_at
  		
    -- Metrics

from 
    {{ source('hh_db_public', 'public_line_of_business') }}
