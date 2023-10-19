select 

    -- Metadata
    last_refreshed_date as _last_refreshed_at,

    -- Business Keys
    user_id as member_id,
    uuid as member_uuid,
    lower(email) as email_address,
    marketing_id, -- not unique, 25 non-null marketing_ids with 2 distinct customer_ids
    first_name, 
    last_name,

	client_id as customer_id,
    client as customer_name,
    team_id, 
	first_team_id, 
	employee_number,
    application_pathway_id, 
    member_id as external_member_id,  
    subscriber_id,

    -- Misc Attibutes 
    gender, 
    address_one, 
    address_two, 
    city, 
    state_of_residence, 
    postal_code,
    phone_number,
    eligibility_ref, 
    relationship, 
    tags, 
    team_status,

    -- JSON
    
    -- Indicators
    legit as is_legit, 
	vip as is_vip,
    eligible as is_eligible,
    phoenix as is_phoenix, -- name ???
    paying_user_flag as is_paying_member,
    {{ build_is_gmail_indicator('email') }},

    -- Dates
    try_cast(date_of_birth as date) as date_of_birth_at,
    created_at,

    -- Metrics
    eligibility_check_count, 
	days_since_insurance_verified,
    pathway_count
    
from 
    {{ source('hh_db_rollups', 'users_view') }}
