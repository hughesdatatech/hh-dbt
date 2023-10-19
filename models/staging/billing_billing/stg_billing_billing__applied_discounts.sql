select

    -- Metadata
    {{ select_debezium_metadata_cols() }},

    -- Business Keys
    id::int as applied_discount_id,
    program_id,
    partnership_id,
    client_id as customer_id,
    revenue_share_detail_id,	
    insurer_id,
    discount_detail_id,

    -- Misc Attributes
    discount_type,	
    tier,	
    state as discount_status,
    source as source_name,
    source_id::int,

    -- JSON
    discount_milestone_amount as discount_milestone_amount_struct,

    -- Indicators
    suppress_empty_charges as should_suppress_empty_charges,
    apply_to_price as should_apply_to_price,

    -- Dates
    created_at,

    -- Metrics
    discount_amount

from 
    {{ source('billing_billing', 'billing_applied_discounts') }}
