{# not used #}
{% macro ___build_dw_metadata_cols(
    source_schema='', 
    source_table='', 
    unique_key=[], 
    extracted_at_column='null', 
    collision_key='', 
    build_hd=false, 
    boolean_cols=[], 
    reserved_cols=[], 
    namesafe_cols=[], 
    rec_source='null', 
    jira_task_key='jira_task_key', 
    alias=''
) -%}
    
    {{ build_tenant_key(tenant_key=null, alias=alias) }},
    {% if unique_key is string %} {# Implies the hash key was already built and passed in, so do not build it, just use it. #}
        {{ unique_key }} as {{ alias }}_hk,
    {% else %} {# Implies unique_key is an array of columns to hash, so build the hash key from those columns. #}
        {{ 
            build_hash_value(
                value=build_hash_diff(
                            cols=unique_key
                        ),
                alias=alias + '_hk'
            )
        }},
    {% endif %}
    {% if rec_source != 'null' %}
        '{{ rec_source|replace("\"", "") }}' as {{ alias }}_rec_source,
    {% else %}
        {{ build_rec_source(source_schema, source_table, alias) }},
    {% endif %}
    {{ build_job_id(invocation_id, alias) }},
    {{ build_job_user_id(alias=alias) }},
    {{ build_jira_task_key(column=jira_task_key, alias=alias) }},
    {{ build_extracted_at(column=extracted_at_column, alias='null') if extracted_at_column != 'null' else build_loaded_at(alias='null') }} as {{ alias }}_extracted_at,
    {{ build_loaded_at(alias) }}
    {%- if build_hd and rec_source != 'null' %},
    {{ 
        build_hash_value(
            value=build_hash_diff(
                        cols=dbt_utils.get_filtered_columns_in_relation(
                            from=rec_source,
                            except=unique_key + boolean_cols + reserved_cols
                        ), 
                        boolean_cols=boolean_cols,
                        namesafe_cols=namesafe_cols
                    ),
            alias=alias + '_hd'
        )
    }}
    {% elif build_hd %},
    {{ 
        build_hash_value(
            value=build_hash_diff(
                        cols=dbt_utils.get_filtered_columns_in_relation(
                            from=source(source_schema, source_table),
                            except=unique_key + boolean_cols + reserved_cols
                        ), 
                        boolean_cols=boolean_cols,
                        namesafe_cols=namesafe_cols
                    ),
            alias=alias + '_hd'
        )
    }}
    {% endif %}

{%- endmacro -%}
