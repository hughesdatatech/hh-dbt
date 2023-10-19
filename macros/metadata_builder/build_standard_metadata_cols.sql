{% macro build_standard_metadata_cols(
    unique_key=[],
    unique_key_name='null',
    hd_source_model='null',
    hd_name='',
    hd_except_cols=[],
    collision_key='',
    rec_source='',  
    alias='',
    model_version_identifier='default',
    use_null_hd=false
) -%}

    {{ rec_source }} as {{ alias }}_rec_source,
    {{ build_loaded_at(alias) }},
    '{{ model_version_identifier }}' as {{ alias }}_model_version_identifier,
    '{{ invocation_id }}' as {{ alias }}_dbt_cloud_invocation_id,
    '{{ env_var("DBT_CLOUD_JOB_NAME", "manual") }}' as {{ alias }}_dbt_cloud_job_name,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as {{ alias }}_dbt_cloud_run_id,
    '{{ var("run_vars_metadata", "default") }}' as {{ alias }}_dbt_cloud_run_vars_metadata,
    '{{ env_var("DBT_CLOUD_JOB_ID", "manual") }}' as {{ alias }}_dbt_cloud_job_id,
    '{{ env_var("DBT_CLOUD_PROJECT_RELEASE_VERSION", "default") }}' as {{ alias }}_dbt_cloud_project_release_version,
     {%- if hd_source_model != 'null' -%}
        {{ 
            build_hash_value(
                value=build_hash_diff(
                            cols=dbt_utils.get_filtered_columns_in_relation(
                                from=ref(hd_source_model),
                                except=hd_except_cols
                            )
                        ),
                alias=alias + '_hd'
            )
        }},
    {% endif %}
    {% if use_null_hd == true %}
        null::string as {{ alias + '_hd' }},
    {% endif %}
    {% if unique_key_name != 'null' %}
        {% if unique_key is string %} {# Implies the hash key was already built and passed in, so do not build it, just use it. #}
            {{ unique_key }} as {{ unique_key_name }}
        {% else %} {# Implies unique_key is an array of columns to hash, so build the hash key from those columns. #}
            {{ 
                build_hash_value(
                    value=build_hash_diff(
                                cols=unique_key
                            ),
                    alias=unique_key_name
                )
            }}
        {% endif %}
    {% endif %}

{%- endmacro -%}
