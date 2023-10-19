select 

    -- Metadata
    {{ select_debezium_metadata_cols() }},
    
    -- Business Keys
    id as tag_id,
    name as tag_name,

    
    -- Misc Attributes
    
    -- JSON
    
    -- Indicators

    -- Dates 

    -- Metrics
    taggings_count

from 
    {{ source('hh_db_public', 'public_tags') }}
