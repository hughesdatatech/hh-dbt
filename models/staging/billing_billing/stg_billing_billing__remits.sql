select

    -- Metadata
    {{ select_debezium_metadata_cols() }},

    -- Business Keys
    id::int as remit_id,
    submission_id::int,

    -- Misc Attributes
    payment_type as remit_payment_type,		
    payment_number,		
    icn,
    source_of_record,	
    payer as payer_name,

    -- JSON

    -- Indicators

    -- Dates
    created_at as remit_created_at,
    response_date as remit_response_date_at,
    payment_date as payment_date_at,

    -- Metrics
    payment_amount,
    patient_payment_amount

from 
    {{ source('billing_billing', 'billing_remits') }}
