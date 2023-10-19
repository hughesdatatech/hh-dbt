select 

    -- Metadata
    {{ select_debezium_metadata_cols() }},

    -- Business Keys
    id as screening_id,
    identifier as screening_identifier,
   
    pathway_id, 
    user_id as member_id,
    priority_team_id,
    recruitment_id,
    first_recruitment_id,

    -- Misc Attributes
    holidays, 
    note,
    coaching_method, 
	applying_for,
    relevance,

    -- Arrays
    tags as tags_arr,
    rejection_reasons as rejection_reasons_arr,
    manual_review_reasons as manual_review_reasons_arr,
    clinical_flags as clinical_flags_arr,
		
    -- JSON
    stratification as stratification_struct,
    json_stratification as json_stratification_struct,
    screen as screen_struct, 
    comm_schedules as comm_schedules_struct, 
    user_info as user_info_struc,
    scores as scores_struct,
    calculated_metrics as calculated_metrics_struct,
    flags as flags_struct,
    knee_flags as knee_flags_struct,
    motivation as motivation_struct,
    start as start_struct,

    -- Indicators
    commitment_accepted as is_commitment_accepted, 
	committed as is_committed,
    {{ build_was_application_accepted_indicator() }},

    -- Dates
    {{ build_date_key('created_at', 'screening_created_at') }},
    created_at as screening_created_at,
    
    screening_failed_at, 
	invited_at,
    application_remind_email_sent_at,
    second_application_remind_email_sent_at, 
    joined_control_email_sent_at,
    invitation_email_sent_at,  
    app_received_email_sent_at, 
    onboarded_at, 
    auto_accepted_email_sent_at, 
    booking_reminder_sms_sent_at,
    shortlisted_at, 
    profile_remind_sms_sent_at, 
    insurance_info_email_sent_at, 
    closed_at, 
    legacy_remote_booked_at,
    coaching_module_final_reminder_sms_sent_at,
    resolved_at,
    online_coaching_session_conf_email_sent_at, 
    profile_remind_email_sent_at,
    joined_standby_email_sent_at, 
    earliest_comm_due as earliest_comm_due_at,
    coaching_remind_sms_sent_at, 
    updated_at, 
    coached_at,
    coaching_remind_email_sent_at,
    committed_at, 
    booked_at,
    rejection_email_sent_at,

    -- Metrics
    screen_page_reached,
    page_reached

from 
    {{ source('hh_db_public', 'public_screenings') }}
