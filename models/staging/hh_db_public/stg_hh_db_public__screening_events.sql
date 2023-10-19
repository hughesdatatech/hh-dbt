select 

    -- Metadata
    {{ select_debezium_metadata_cols() }},

    -- Business Keys
    id as screening_event_id,
   
    screening_id,

    -- Misc Attributes
    event_name, 
	implementor,  

    -- JSON
    value_changed,

    -- Indicators
    
    -- Dates
    created_at

    -- Metrics

from 
    {{ source('hh_db_public', 'public_screening_events') }}
