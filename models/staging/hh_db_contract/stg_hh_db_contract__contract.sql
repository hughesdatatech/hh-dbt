select

    -- Metadata
    {{ select_debezium_metadata_cols() }},

    -- Business Keys	
    id as contract_id,
    client_id as customer_id,
    contract_rule_id,
    insurer_id,
    clients_insurer_id as customer_insurer_id,
    contract_template_id,	


    -- Misc Attributes
    last_modified_by,

    -- JSON
    contract as contract_struct,

    -- Indicators
    void as is_voided,

    -- Dates
    created_at,
    updated_at,
    start_date as start_date_at,
    end_date as end_date_at,
  		
    -- Metrics
    acute_price::double,
    chronic_price::double

from 
    {{ source('hh_db_contract', 'contract_contract') }}
