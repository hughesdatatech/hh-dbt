{{ config(
    enabled=false
) }}

{{ build_staging_raw_vault_model(
    model_name='rve_hh_db_rollups__pathways_view', 
    is_incremental_load=False, 
    incremental_timestamp_column='last_refreshed_date',
    unique_key=['pathway_id'],
    snapshot_strategy='check',
    staging_model_default_exception_cols=[],
    staging_model_order_by_cols=['pathway_start_date'],
    boolean_cols=[],
    reserved_cols=[],
    boolean_reserved_cols=[],
    jira_task_key='DEFAULT',
    dedupe=False
) }}
