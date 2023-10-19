select

    -- Metadata
    {{ select_debezium_metadata_cols() }},

    -- Business Keys
    id as customer_configuration_id,

    configuration_id, 
	client_id as customer_id, 

    -- Misc Attributes

    -- JSON

    -- Indicators

    -- Dates
    updated_at, 
	ends_on as ended_at, 
	created_at, 
	starts_on as started_at

    -- Metrics

from 
    {{ source('hh_db_public', 'public_client_configurations') }}
 