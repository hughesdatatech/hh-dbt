{{ config(
    enabled=false
) }}

/* was not performing consistently */
{{ build_staging_raw_vault_model(
    model_name='rve_mailchimp_rollups__mailchimp_members_view', 
    is_incremental_load=False, 
    incremental_timestamp_column=null,
    unique_key=['id', 'web_id'],
    snapshot_strategy='check',
    staging_model_default_exception_cols=[],
    staging_model_order_by_cols=['ingested_at'],
    boolean_cols=[],
    reserved_cols=[],
    boolean_reserved_cols=[],
    jira_task_key='DEFAULT',
    dedupe=False
) }}
