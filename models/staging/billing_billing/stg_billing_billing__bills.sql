select

    -- Metadata
    {{ select_debezium_metadata_cols() }},

    -- Business Keys
    id::int as bill_id,
    coverage_id,
    pathway_id,	
    subscription_id,
    clients_insurer_id as customer_insurer_id,	
    source_id::int,
    claim_identifier,

    -- Misc Attributes
    payment_type as bill_payment_type,
    source as source_name,
    billing_month as billing_month_string,

    -- JSON

    -- Indicators

    -- Dates
    created_at as bill_created_at,
    updated_at as bill_updated_at,
    from_date as bill_from_date_at,
    to_date as bill_to_date_at,
    try_cast(billing_month as date) as billing_month_at,

    -- Metrics
    base_charge,
    bill_amount
    
from 
    {{ source('billing_billing', 'billing_bills') }}
