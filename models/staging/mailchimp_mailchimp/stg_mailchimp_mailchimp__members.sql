select 

    -- Metadata
    ingested_at as _ingested_at,
    merge_fields as _merge_fields,

    -- Business Keys
    id as mailchimp_member_id,
    web_id,
    lower(email_address) as email_address,

    list_id as mailchimp_list_id,
    
    -- Misc Attibutes
    email_id, 
    email_type, 
    status, 
    unsubscribe_reason, 
    interests, 
    signup_ip,
    language, 
    email_client, 
    location_latitude, 
    location_longitude, 
    location_gmtoff, 
    location_dstoff, 
    location_country_code, 
    location_timezone, 
    last_note_id, 
    last_note_created_by, 
    last_note_content, 
    source, 
    tags_count, 

    -- JSON
    tags,

    -- Indicators
    is_vip,
    {{ build_is_gmail_indicator('email_address') }},

    -- Dates
    signup_date as signed_up_at, 
	last_change_time as last_changed_at,
    last_note_created_at

    -- Metrics

from 
    {{ source('mailchimp_mailchimp', 'members') }}
