select 

    -- Metadata
    {{ select_debezium_metadata_cols() }},

    -- Business Keys
    id as recruitment_id,
    name as recruitment_name,

    client_id as customer_id,
    program_indication_id,
    indication_id,

    -- Misc Attributes
    product_type,
    profile_reminder_wait,
    application_reminder_wait,

    -- JSON

    -- Indicators
    dmc_enabled as is_dmc_enabled,
    profile_shortcut as has_profile_shortcut,
    enabled as is_enabled,
    legitimate as is_legitimate,
    
    -- Dates
    starts_at,
    created_at,
    updated_at,
    ends_at

    -- Metrics

from 
    {{ source('hh_db_public', 'public_recruitments') }}
