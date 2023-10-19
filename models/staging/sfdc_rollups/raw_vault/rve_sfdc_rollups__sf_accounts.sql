{{ config(
  enabled=false
) }}

{{ build_staging_raw_vault_model(
    model_name='rve_sfdc_rollups__sf_accounts', 
    is_incremental_load=False, 
    incremental_timestamp_column='insertion_date',
    unique_key=['account_id'],
    snapshot_strategy='check',
    staging_model_default_exception_cols=[],
    staging_model_order_by_cols=['insertion_date'],
    boolean_cols=[],
    reserved_cols=[],
    boolean_reserved_cols=[],
    jira_task_key='DEFAULT',
    dedupe=False
) }}
