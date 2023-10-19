select

    -- Metadata
    {{ select_debezium_metadata_cols() }},

    -- Business Keys
    id::int as archive_id,
    submission_id::int, 

    -- Misc Attributes
    reason as archive_reason,

    -- JSON

    -- Indicators

    -- Dates
    created_at as archive_created_at,

    -- Metrics
    amount as archive_amount

from 
    {{ source('billing_billing', 'billing_archive') }}
