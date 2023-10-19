{{ config(
    enabled=false
) }}

{{ build_staging_raw_vault_model(
    model_name='rve_mailchimp_mailchimp__members', 
    is_incremental_load=False, 
    incremental_timestamp_column='ingested_at',
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

