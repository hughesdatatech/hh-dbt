select 

    -- Metadata
    {{ select_debezium_metadata_cols() }},

    -- Business Keys
    id as tagging_id,

    tag_id,
    taggable_id, 
	tagger_id, 
    
    -- Misc Attributes
    context, 
	tagger_type, 
	taggable_type, 

    -- JSON
    extras,
    
    -- Indicators

    -- Dates 
    created_at

    -- Metrics

from 
    {{ source('hh_db_public', 'public_taggings') }}
