{{ config(
    enabled=false
) }}

{{ build_staging_raw_vault_model(
    model_name='rve_efile_rollups__eligible_lives', 
    is_incremental_load=False, 
    incremental_timestamp_column='updated_at',
    unique_key=['marketing_id', 'created_at'],
    snapshot_strategy='check',
    staging_model_default_exception_cols=[],
    staging_model_order_by_cols=['created_at'],
    boolean_cols=[],
    reserved_cols=[],
    boolean_reserved_cols=[],
    jira_task_key='DEFAULT',
    dedupe=False
) }}
