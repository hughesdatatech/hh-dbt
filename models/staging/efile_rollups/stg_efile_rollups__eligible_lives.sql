select

    -- Metadata
    created_at as _created_at,
	updated_at as _updated_at,

    -- Business Keys
    marketing_id,

    client_id as customer_id,

    -- Misc Attributes
    account_name,
    insurer as insurer_name, 
    case 
        when nvl(relationship, '') = '' 
            then 'EE'
        when lower(relationship) in ('employee', 'ee')
            then 'EE'
        when lower(relationship) = 'spouse' 
            then 'Spouse'
        when lower(relationship) in ('child', 'adult child')
            then 'Child'
        else 'Other'
    end as relationship,
    zip as postal_code, 
	sex,
	
    -- JSON

    -- Indicators
    has_email, 
	has_phone_number, 
	has_address, 
	is_18_plus, 
	is_active, 

    -- Dates

    -- Metrics
    age

from 
    {{ source('efile_rollups', 'eligible_lives') }}
