{%- set customers_columns = dbt_utils.get_filtered_columns_in_relation(from=ref("int_customers"), except=[]) -%}
{%- set unique_key = ['customer_id'] -%}

with 

customers as (

    select 
        *,
        'int_customers' as record_source
    from 
        {{ ref("int_customers") }}
    where true

    union all

    select 
    {% for column_name in customers_columns %}
        null,
    {% endfor %}
        'system' as record_source

),

final as (

    select 

        -- Metadata
        {{ select_debezium_metadata_cols(select_from_source=False) }},

        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='customer_key',
            hd_source_model='int_customers',
            hd_name='customer_hd',
            hd_except_cols=unique_key,
            rec_source='record_source',
            alias='_dim_customers'
        ) }},
    
        -- Business Keys
        customer_id,
        customer_identifier,
        customer_name,
        customer_uuid,
        
        -- Related IDs
        account_id,
        
        -- Misc Attributes
        line_of_business,
        lower_line_of_business,
        dlp, 
        shipping_carrier,  
        country_code, 
        hcsc_account_names, 
        cvs_codes, 
        castlight_employer_key, 
        logo_file_name, 
        logo_content_type, 
        logo_file_size, 

        -- JSON
        
        -- Indicators
        has_hcsc_reporting,
        is_shipping_enabled, 
        has_event_messaging,
        is_paying,
        is_vip_in_identifier,
        is_test_customer,
        is_fully_insured,
        is_missing_efile,

        -- Dates
        created_at,
        logo_updated_at,
        updated_at, 
        first_conversion_at,

        -- Metrics
        efile_lookback_days, 
        core_charge,
        maintain_charge,
        efile_eligible_lives,
        iterable_email_address_count,
        iterable_non_gmail_address_count,
        mailchimp_email_address_count,
        mailchimp_non_gmail_address_count,
        eligible_members,

        scheduled_send_source_system,
        scheduled_send_email_address_count,
        scheduled_send_non_gmail_address_count,
        scheduled_send_non_gmail_address_pct

    from 
        customers
    where true

)

select *
from final
where true
