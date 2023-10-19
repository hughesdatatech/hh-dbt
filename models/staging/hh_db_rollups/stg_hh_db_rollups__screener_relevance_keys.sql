select 

    -- Metadata
    -- TO DO: need source metadata, extracted timestamp?

    -- Business Keys
    id as screener_relevance_id,
    description as screener_relevance_description

    -- Misc Attibutes

    -- JSON
    
    -- Indicators

    -- Dates

    -- Metrics

from 
    {{ source('hh_db_rollups', 'screener_relevance_key') }}
