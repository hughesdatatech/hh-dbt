{{ config(
    enabled=false
) }}

{{ build_staging_raw_vault_model(
    model_name='rve_iterable_rollups__iterable_user', 
    is_incremental_load=False, 
    incremental_timestamp_column='_fivetran_synced',
    unique_key=['email'],
    snapshot_strategy='check',
    staging_model_default_exception_cols=[],
    staging_model_order_by_cols=['_fivetran_synced'],
    boolean_cols=[],
    reserved_cols=[],
    boolean_reserved_cols=[],
    jira_task_key='DEFAULT',
    dedupe=False
) }}
