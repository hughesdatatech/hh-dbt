select 

    -- Metadata
    try_cast(_duplicate as boolean) as _is_duplicate,
    _fivetran_synced as _fivetran_synced,

    -- Business Keys 
    try_cast(user_id as int) as member_id, -- int member_id sometimes stored in the user_id field in Iterable after a member signs up for HH instead of the marketing_id
    nvl(uuid, '') as member_uuid,
    nvl(lower(email), '') as email_address,
    case
        when try_cast(user_id as int) is null
            then user_id
        else null
    end as marketing_id, -- user_id = marketing_id when user_id is not int
    first_name, 
	last_name, 
    
	try_cast(clientid as integer) as customer_id, 
    nvl(client, '') as customer_identifier,
    nvl(lower(customer_identifier), '') as lower_customer_identifier,
    replace(lower_customer_identifier, ' ', '') as lower_customer_identifier_short,

    -- Misc Attibutes
	phone_number, 
	signup_source, 
	email_list_ids as iterable_email_list_ids, 
	phone_number_carrier, 
	phone_number_country_code_iso, 
	phone_number_line_type, 
	slug, 
	firstname, -- TO DO: diff with first_name?
	lastname, -- TO DO: diff with last_name?
	postalcode as postal_code, 
	relationship,

    -- JSON

    -- Indicators
    try_cast(duplicate as boolean) as is_duplicate,
    iff(email_address ilike '%@placeholder.email', true, false) as is_placeholder_email_address,
    {{ build_is_gmail_indicator('email') }},

    -- Dates
    updated_at, 
    signup_date as signed_up_at,
    try_cast(phone_number_updated_at as timestamp) as phone_number_updated_at,
    try_cast(dateofbirth as date) as date_of_birth_at

    -- Metrics

from 
    {{ source('iterable_rollups', 'iterable_user') }}
