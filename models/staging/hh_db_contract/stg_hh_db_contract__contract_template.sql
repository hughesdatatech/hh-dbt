select

    -- Metadata
    {{ select_debezium_metadata_cols() }},

    -- Business Keys	
    id as contract_template_id,
    contract_rule_id,
    partnership_id,

    -- Misc Attributes
    name as contract_template_name,
    name as billing_category,
    contract_type,
    last_modified_by,

    -- JSON
    contract as contract_struct,

    -- Indicators
    is_active,

    -- Dates
    created_at,
    updated_at

    -- Metrics

from 
    {{ source('hh_db_contract', 'contract_contract_template') }}
