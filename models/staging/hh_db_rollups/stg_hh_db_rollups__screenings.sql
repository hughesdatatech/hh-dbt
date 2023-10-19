select 
    
    -- Metadata
    op as _dbz_meta__op,
    
    -- Business Keys
    id as screenings_rollup_id,
    screening_id,
    identifier as screening_identifier,

    pathway_id,
    user_id as member_id,
    priority_team_id,
    recruitment_id,
    first_recruitment_id,

    -- Misc Attibutes
    holidays, 
    note,
    coaching_method, 
	applying_for,

    -- Not in public.screenings
    `basics.recent_surgery` as basics_recent_surgery,
    `basics.previous_pain_days` as basics_previous_pain_days,
    `basics.recent_injury` as basics_recent_injury,
    `basics.reason_injury` as basics_reason_injury,
    `basics.flags` as basics_flags,
    `basics.description` as basics_description,
    `impact.surgery_chance_next_year` as impact_surgery_chance_next_year,
    `impact.surgery_chance_next_two_years` as impact_surgery_chance_next_two_years,
    `impact.surgery_chance_next_five_years` as impact_surgery_chance_next_five_years,
    `impact.understand_condition_and_options` as impact_understand_condition_and_options,
    `impact.surgery_free_text` as impact_surgery_free_text,
    `impact.roland_morris` as impact_roland_morris,
    `impact.rmdq_11` as impact_rmdq_11,
    `screening.pain_type` as screening_pain_type,
    `screening.want_exercise_mat` as screening_want_exercise_mat,
    `red_flag_follow_ups.leg_numbness_3_0` as red_flag_follow_ups_leg_numbness_3_0,
    `red_flag_follow_ups.problems_with_urination_or_stool_0` as red_flag_follow_ups_problems_with_urination_or_stool_0,
    `red_flag_follow_ups.surgery_type` as red_flag_follow_ups_surgery_type,
    `red_flag_follow_ups.injury_type` as red_flag_follow_ups_injury_type,
    `red_flag_follow_ups.back_trauma_0` as red_flag_follow_ups_back_trauma_0,
    `red_flag_follow_ups.unexplained_weight_loss_0_1` as red_flag_follow_ups_unexplained_weight_loss_0_1,
    `red_flag_follow_ups.cancer_0` as red_flag_follow_ups_cancer_0,
    `red_flag_follow_ups.cancer_1` as red_flag_follow_ups_cancer_1,
    `red_flag_follow_ups.avoid_0` as red_flag_follow_ups_avoid_0,
    `red_flag_follow_ups.avoid_0_1` as red_flag_follow_ups_avoid_0_1,
    `user.email` as member_email_address,
    `user.confirm_email` as confirmed_member_email_address,
    `user.consent` as member_consent,
    `previous.pain.days` as previous_pain_days,
    `screening.pain_type.n` as screening_pain_type_n,
    `activity.exercise_amount` as activity_exercise_amount,

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

    -- Not in public.screenings
    try_cast(`has.low.back.pain` as boolean) as has_low_back_pain,

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
    relevance,
    page_reached,

    -- Not in public.screenings
    -- reals
    pct_affected,
    `prod.impairment.while.working` as prod_impairment_while_working,
    height,
    `screening.bmi` as screening_bmi,
    pct_missed,
    `prod.impairment.overall` as prod_impairment_overall,
    `prod.work.time.missed.due.to.health` as prod_work_time_missed_due_to_health,
    -- ints
    weight,
    `vas.pain.24.hr` as vas_pain_24_hr,
    `vas.pain.7.day` as vas_pain_7_day,
    `vas.pain` as vas_pain,
    `activity.exercise_strenuous` as activity_exercise_strenuous,
    `activity.exercise_moderate` as activity_exercise_moderate,
    `activity.exercise_mild` as activity_exercise_mild,
    health_prod_effect,
    hours_missed_health,
    hours_worked,
    `hospital.anxiety` as hospital_anxiety

from 
    {{ source('hh_db_rollups', 'screenings_view') }}
