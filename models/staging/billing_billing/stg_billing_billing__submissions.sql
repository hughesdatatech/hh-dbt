select

    -- Metadata
    {{ select_debezium_metadata_cols() }},

    -- Business Keys
    id::int as submission_id,
    bill_id::int,	
    coverage_id,
    transaction_id,	
    netsuite_id,		

    -- Misc Attributes
    status as submission_status,

    -- JSON

    -- Indicators
    case
        when lower(status) not in ('void', 'duplicate', 'unknown')
            then true
        else false 
    end as is_valid_submission_status,
    not is_valid_submission_status as is_submission_voided,

    -- Dates
    created_at as submission_created_at,
    updated_at as submission_updated_at,
    status_update_at as submission_status_updated_at,
    submission_date as submission_date_at,

    -- Metrics
    submission_amount

from 
    {{ source('billing_billing', 'billing_submissions') }}
