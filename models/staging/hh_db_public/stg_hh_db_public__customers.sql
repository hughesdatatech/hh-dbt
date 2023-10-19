select 

    -- Metadata
    {{ select_debezium_metadata_cols() }},
    
    -- Business Keys
    id as customer_id,
    lower(identifier) as customer_identifier,
    nvl(name, '') as customer_name,
    lower(customer_name) as lower_customer_name,
    uuid as customer_uuid,
    
    -- Misc Attributes
    dlp, 
    shipping_carrier,
    case
        when lower(country_code) in ('us', 'usa', 'united states')
            then 'USA'
        else upper(country_code)
    end as country_code,
	hcsc_account_names, 
    cvs_codes, 
    castlight_employer_key, 
    logo_file_name, 
    logo_content_type, 
    logo_file_size, 

    -- JSON
    
    -- Indicators
    hcsc_reporting as has_hcsc_reporting,
    shipping_enabled as is_shipping_enabled, 
    event_messaging as has_event_messaging,
    paying as is_paying,
    vip as is_vip,
    case
        when customer_identifier like '%vip%'
            then True
        else False
    end as is_vip_in_identifier,
    case
        when customer_identifier in ('london', 'facebook', 'legacy_client', 'fake', 'hhdevtest', 'testjan', 'pentesting', 'phx_beta', 'phoenix_alpha')
            then True
        else False
    end as is_test_customer,

    -- Dates
    created_at,
    logo_updated_at,
    updated_at, 

    -- Metrics
    efile_lookback_days, 
	core_charge,
    maintain_charge

from 
    {{ source('hh_db_public', 'public_clients') }}
