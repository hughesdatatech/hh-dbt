select

    -- Metadata
    {{ select_debezium_metadata_cols() }},

    -- Business Keys
    id as customer_insurer_partnership_id,
    clients_insurer_id as customer_insurer_id,
    partnership_id

    -- Misc Attributes

    -- JSON

    -- Indicators

    -- Dates
  		
    -- Metrics

from 
    {{ source('hh_db_public', 'public_clients_insurers_partnerships') }}
