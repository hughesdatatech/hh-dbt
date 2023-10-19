select 

    -- Metadata
    {{ select_debezium_metadata_cols() }},
    
    -- Business Keys
    id as member_id,
    uuid as member_uuid,
    email as email_address,
    marketing_id, -- not unique
    legal_first_name,
    first_name,
    last_name,
    uid as member_uid, -- same value as email_address

    client_id as customer_id,
    ga_client_id as ga_customer_id,
    team_id,
    first_team_id,
    employee_number,
    application_pathway_id,
    insurer_id,
    member_id as external_member_id,
    helpscout_customer_id,
    
    -- Misc Attributes
    gender,
    address_one,
    address_two,
    city,
    state_of_residence,
    postal_code,
    country,
    phone_number,
    hashed_password,
    access_token,
    avatar_file_name,
    avatar_content_type,
    avatar_file_size,
    sms_thread_id,
    bio,
    team_note,
    candidate_type,
    encrypted_password,
    reset_password_token,
    current_sign_in_ip,
    last_sign_in_ip,
    confirmation_token,
    unconfirmed_email,
    provider,
    hard_avatar_url,
    goals,
    one_time_token,
    secondary_emails,
    occupation,
    current_height,
    current_weight,
    contact_method,
    eligibility_employee_identifier,
    eligibility_note,
    unlock_token,
    dlp,
    discourse_username,
    discourse_api_key,
    discourse_bio,
    nickname,
    email_provider_identifier,
    
    -- JSON
    tokens,
    preferences,
    coaching_questions,
    
    -- Indicators
    legit as is_legit,
    phoenix as is_phoenix, -- TO DO: name?
    is_admin,
    employed as is_employed,
    show_discourse_bio as is_show_discourse_bio_enabled,
    show_level as is_show_level_enabled,
    show_location as is_show_location_enabled,
    sms_notification_permission as is_sms_notification_enabled,
    privacy_consent_checked as is_privacy_consent_checked,
    receive_insurance_coverage as has_receive_insurance_coverage, -- TO DO: name?
    phone_verified as is_phone_verified,
    involved as is_involved,
    {{ build_is_gmail_indicator('email') }},

    -- Dates 
    date_of_birth as date_of_birth_at,
    last_authentication as last_authentication_at,
    created_at,
    updated_at,
    last_seen as last_seen_at,
    avatar_updated_at,
    messages_seen_at,
    reset_password_sent_at,
    remember_created_at,
    current_sign_in_at,
    last_sign_in_at,
    confirmed_at,
    confirmation_sent_at,
    actionables_seen_at,
    profile_completed_at,
    one_time_token_expiry as one_time_token_expiry_at,
    locked_at,

    -- Metrics
    weight_loss_goal,
    point_sum,
    sign_in_count,
    failed_attempts

from 
    {{ source('hh_db_public', 'public_users') }}
