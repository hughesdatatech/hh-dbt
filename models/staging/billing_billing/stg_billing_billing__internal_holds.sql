select

    -- Metadata
    {{ select_debezium_metadata_cols() }},

    -- Business Keys
    bill_id::int,

    -- Misc Attributes
    note as hold_note,
    hold_type as hold_type,

    -- JSON

    -- Indicators

    -- Dates
    created_at as hold_date_at,
    cleared_at as hold_cleared_date_at

    -- Metrics

from 
    {{ source('billing_billing', 'billing_internal_holds') }}
