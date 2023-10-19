select

    -- Metadata
    {{ select_debezium_metadata_cols() }},

    -- Business Keys
    id::integer as subscription_id,

    user_id::integer as member_id,
    user_uuid as member_uuid,
    client_id::integer as customer_id,
    clients_insurer_id::integer as customer_insurer_id,
    pathway_id::integer as pathway_id,
    pathway_uuid,
    program_id::integer as program_id,
    contract_id::integer as contract_id,

    -- Misc Attributes
    
    -- JSON

    -- Indicators
    nvl(try_cast(void as boolean), false) as is_subscription_voided,

    -- Dates
    starts_at::date as subscription_starts_at,
    ends_at::date as subscription_ends_at,
    created_at::timestamp as subscription_created_at,
	updated_at::timestamp as subscription_updated_at,
    term_date::date as subscription_term_date_at,
    case 
        when is_subscription_voided
            then subscription_updated_at
        else null
    end as subscription_voided_at,

    -- Metrics
    year_count::integer as subscription_year_count

from 
    {{ source('billing_billing', 'billing_subscriptions') }}
