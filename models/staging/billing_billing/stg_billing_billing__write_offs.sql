select

    -- Metadata
    {{ select_debezium_metadata_cols() }},

    -- Business Keys
    id::int as write_off_id,	
    submission_id::int,

    -- Misc Attributes
    reason as write_off_reason,

    -- JSON

    -- Indicators

    -- Dates
    created_at as write_off_created_at,

    -- Metrics
    amount as write_off_amount

from 
    {{ source('billing_billing', 'billing_write_offs') }}
