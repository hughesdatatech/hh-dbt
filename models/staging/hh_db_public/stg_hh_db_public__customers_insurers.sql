select

    -- Metadata
    {{ select_debezium_metadata_cols() }},

    -- Business Keys
    id as customer_insurer_id,
    client_id as customer_id,
    insurer_id,
    line_of_business_id::int,	

    -- Misc Attributes
    core_contract_rule,
    api_code,
    billing_type as source_billing_type,
    case
        when source_billing_type ilike '%claim%' 
            then 'Claims'
        when source_billing_type ilike '%invoice%'
            then 'Invoice'
        else
            source_billing_type
    end as billing_type,

    -- JSON

    -- Indicators
    efile_required as is_efile_required,
    auto_eligibility_enabled as has_auto_eligibility_enabled,
    disable_eligibility_alerts as has_eligibility_alerts_disabled,	
    incremental_eligibility	as has_incremental_eligibility,	
    api_required as is_api_required,	
    athena as is_athena,	
    ignore_primary as should_ignore_primary,

    -- Dates
    term_date as term_date_at,	
    effective_date as effective_date_at,
    created_at,	
    updated_at,	
  		
    -- Metrics
    acute_base_price,	
    chronic_base_price

from 
    {{ source('hh_db_public', 'public_clients_insurers') }}
