{{ config(
  enabled=false
) }}

{{ build_staging_raw_vault_model(
    model_name='rve_xsrc_rollups__marketing_events_observed_forecasted', 
    is_incremental_load=False, 
    incremental_timestamp_column=null,
    unique_key=['activity_type'],
    snapshot_strategy='check',
    staging_model_default_exception_cols=[],
    staging_model_order_by_cols=['null'],
    boolean_cols=[],
    reserved_cols=[],
    boolean_reserved_cols=[],
    jira_task_key='DEFAULT',
    dedupe=False
) }}
