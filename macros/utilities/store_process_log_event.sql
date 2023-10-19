{% macro store_process_log_event(event='unknown', event_metadata='default') %}

    {% set logging_schema = get_logging_schema() %}

    {% if 'dbt_cloud_pr' not in logging_schema %}

        {% set select_query %}
            select 
                {{ get_run_started_timestamp_at() }} as _dbt_cloud_run_started_at,
                '{{ invocation_id }}' as _dbt_cloud_invocation_id,
                '{{ env_var("DBT_CLOUD_JOB_NAME", "manual") }}' as _dbt_cloud_job_name,
                '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _dbt_cloud_run_id,
                '{{ var("run_vars_metadata", "default") }}' as _dbt_cloud_run_vars_metadata,
                '{{ env_var("DBT_CLOUD_JOB_ID", "manual") }}' as _dbt_cloud_job_id,
                '{{ env_var("DBT_CLOUD_PROJECT_RELEASE_VERSION", "default") }}' as _dbt_cloud_project_release_version,
                '{{ event }}' as _event,
                '{{ event_metadata }}' as _event_metadata,
                current_timestamp() as _event_db_timestamp_created_at
        {% endset %}

        {% set create_query %}
            create table if not exists {{ logging_schema }}.{{ var("process_log") }} as {{ select_query }} where false;
        {% endset %}

        {% set insert_query %}
            insert into {{ logging_schema }}.{{ var("process_log") }} {{ select_query }};
        {% endset %}

        {% do run_query(create_query) %}

        {% do run_query(insert_query) %}

    {% endif %}

{% endmacro %}
