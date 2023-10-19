select

    -- Metadata
    {{ select_debezium_metadata_cols() }},

    -- Business Keys	
    id as discount_detail_id,
    discount_definition_id,

    -- Misc Attributes
    subscription_tier,
    tier,
    name as discount_detail_name,
    last_modified_by,

    -- JSON
    milestone_discount as milestone_discount_struct,

    -- Indicators

    -- Dates
    created_at,
    updated_at,
  		
    -- Metrics
    discount::double

from 
    {{ source('hh_db_contract', 'contract_discount_details') }}
