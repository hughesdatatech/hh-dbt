select 

    -- Metadata
    {{ select_debezium_metadata_cols() }},

    -- Business Keys
    id as subscription_id,
    
    pathway_id,
    user_id as member_id, 
	contract_id, 
	program_id, 
    clients_insurer_id as customer_insurer_id, 
    
    -- Misc Attributes

    -- JSON
    
    -- Indicators

    -- Dates
    starts_at as started_at, 
    updated_at, 
    created_at,
    ends_at as ended_at,  

    -- Metrics
    year_count

from 
    {{ source('hh_db_public', 'public_subscriptions') }}
