{% macro select_standard_metadata_cols(alias='', alias_as='null', use_null_hd=false) -%}

    {{ '_' ~ alias ~ '_rec_source' }} {{ 'as _' ~ alias_as ~ '_rec_source' if alias_as != 'null' }},
    {{ '_' ~ alias ~ '_loaded_at' }} {{ 'as _' ~ alias_as ~ '_loaded_at' if alias_as != 'null' }},
    {{ '_' ~ alias ~ '_model_version_identifier' }} {{ 'as _' ~ alias_as ~ '_model_version_identifier' if alias_as != 'null' }},
    {{ '_' ~ alias ~ '_dbt_cloud_invocation_id' }} {{ 'as _' ~ alias_as ~ '_dbt_cloud_invocation_id' if alias_as != 'null' }},
    {{ '_' ~ alias ~ '_dbt_cloud_job_name' }} {{ 'as _' ~ alias_as ~ '_dbt_cloud_job_name' if alias_as != 'null' }},
    {{ '_' ~ alias ~ '_dbt_cloud_run_id' }} {{ 'as _' ~ alias_as ~ '_dbt_cloud_run_id' if alias_as != 'null' }},
    {{ '_' ~ alias ~ '_dbt_cloud_run_vars_metadata' }} {{ 'as _' ~ alias_as ~ '_dbt_cloud_run_vars_metadata' if alias_as != 'null' }},
    {{ '_' ~ alias ~ '_dbt_cloud_job_id' }} {{ 'as _' ~ alias_as ~ '_dbt_cloud_job_id' if alias_as != 'null' }},
    {{ '_' ~ alias ~ '_dbt_cloud_project_release_version' }} {{ 'as _' ~ alias_as ~ '_dbt_cloud_project_release_version' if alias_as != 'null' }},
    {% if use_null_hd == true %}
        null::string {{ 'as _' ~ alias_as ~ '_hd' if alias_as != 'null' }}
    {% else %}
        {{ '_' ~ alias ~ '_hd' }} {{ 'as _' ~ alias_as ~ '_hd' if alias_as != 'null' }}
    {% endif %}

{%- endmacro %}
