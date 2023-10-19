{% macro store_test_failures_history() %}

    {% set logging_schema = get_logging_schema() %}

    {% set audit_filter = "show tables in " ~ logging_schema ~ " like '_audit(?!.*_h$).*$'" %}

    {% set audit_tables = run_query(audit_filter).columns["tableName"].values() %}

    {% for audit_table in audit_tables %}

        {% set describe_filter = "describe detail " ~ logging_schema ~ "." ~ audit_table %}
        {% set table_sizes = run_query(describe_filter).columns["sizeInBytes"].values() %}
        
        {% for table_size in table_sizes %}

            {% if table_size != 0 %}      
                        
                {% set hist_table = audit_table ~ '_h' %}

                {% set select_query %}
                    select 
                        *, 
                        {{ get_run_started_timestamp_at() }} as _loaded_at,
                        '{{ invocation_id }}' as _dbt_cloud_invocation_id,
                        '{{ env_var("DBT_CLOUD_JOB_NAME", "manual") }}' as _dbt_cloud_job_name,
                        '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _dbt_cloud_run_id,
                        '{{ var("run_vars_metadata", "default") }}' as _dbt_cloud_run_vars_metadata,
                        '{{ env_var("DBT_CLOUD_JOB_ID", "manual") }}' as _dbt_cloud_job_id,
                        '{{ env_var("DBT_CLOUD_PROJECT_RELEASE_VERSION", "default") }}' as _dbt_cloud_project_release_version
                    from
                        {{ logging_schema }}.{{ audit_table }}
                {% endset %}

                {% set create_query %}
                    create table if not exists {{ logging_schema }}.{{ hist_table }} as {{ select_query }} where false;
                {% endset %}

                {% set insert_query %}
                    insert into {{ logging_schema }}.{{ hist_table }} {{ select_query }};
                {% endset %}

                {% do run_query(create_query) %}

                {% do run_query(insert_query) %}

                {{ store_process_log_event(event='store_test_failures_history', event_metadata=hist_table) }}

            {% endif %}

        {% endfor %}

    {% endfor %}

{% endmacro %}
