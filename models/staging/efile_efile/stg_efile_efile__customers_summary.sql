select

    -- Metadata
    {{ select_debezium_metadata_cols() }},
    s3_source as _s3_source,

    -- Business Keys
    id as efile_customer_summary_id,
    
    -- Create surrogate key used to check that current record is unique
    case 
        when not nvl(try_cast(current as boolean), false)
            then null 
        else nvl(client_id::string, '') || nvl(insurer_id::string, '') || nvl(published_date::string, '')
    end as efile_customer_summary_key,

    client_id as customer_id,
    client_identifier as customer_identifier, 
    client_name as customer_name,
	insurer_id, 
	insurer_identifier, 

    -- Misc Attributes
    
    -- JSON
    state_counts_total as total_state_counts_struct, 
	state_counts_employees as employee_state_counts_struct,
    age_counts as age_counts_struct,
    gender_counts as gender_counts_struct, 
    hcsc_account_counts as hcsc_account_counts_struct,

    -- Indicators
    nvl(try_cast(current as boolean), false) as is_current_record,
    
    -- Dates
    published_date as published_at,
    created_at,
    updated_at,
		
    -- Metrics
    employee_count, 
    spouse_count, 
    dependent_count, 
    other_count,
    employee_count + spouse_count + dependent_count + other_count as total_family_count, 
    total_count,
	email_count_total as total_email_count, 
	email_count_employees as employee_email_count,
    nvl(try_cast(get_json_object(gender_counts, '$.F') as int), 0) as female_count,
    nvl(try_cast(get_json_object(gender_counts, '$.M') as int), 0) as male_count,
    nvl(try_cast(get_json_object(gender_counts, '$.F') as int), 0) + nvl(try_cast(get_json_object(gender_counts, '$.M') as int), 0) as total_male_female_count

from 
    {{ source('efile_efile', 'efile_efile_summaries') }}
