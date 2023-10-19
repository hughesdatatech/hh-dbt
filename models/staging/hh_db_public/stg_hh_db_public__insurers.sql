select

    -- Metadata
    {{ select_debezium_metadata_cols() }},

    -- Business Keys
    id as insurer_id,
    identifier as insurer_identifier,

    -- Misc Attributes
    name as insurer_name,
    eligibility_override,	
    eligibility_ref,	
    claims_ref,	
    member_id_prefix,	

    -- JSON

    -- Indicators
    member_id_required as is_member_id_required,
    ignore_primary as should_ignore_primary,

    -- Dates
    created_at,	
    updated_at	

    -- Metrics

from 
    {{ source('hh_db_public', 'public_insurers') }}
