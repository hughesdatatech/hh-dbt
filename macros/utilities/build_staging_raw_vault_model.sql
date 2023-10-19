{%- macro build_staging_raw_vault_model(
    model_name=none, 
    is_incremental_load=False, 
    incremental_timestamp_column=null,
    unique_key=['id'],
    snapshot_strategy='check',
    staging_model_default_exception_cols=[],
    staging_model_order_by_cols=[],
    boolean_cols=[],
    reserved_cols=[],
    boolean_reserved_cols=[],
    jira_task_key='null',
    dedupe=False,
    use_backticks=False
) -%}
{# Parse schema and table from the model name. #}
{%- set source_schema__table = get_source_schema__table(model_name) -%}
{%- set source_schema = get_source_schema(source_schema__table) -%}
{%- set source_table = get_source_table(source_schema__table) -%}
{%- set source_table_relation = source(source_schema, source_table) -%}

{%- set build_hd = true -%}
{%- set extracted_at_column = 'null' -%}
{%- if snapshot_strategy == 'timestamp' -%}
    {%- set build_hd = false -%}
    {%- set extracted_at_column = var("extracted_at_default") -%}
{%- endif %}

{# Get special case boolean and reserved word colums applicable to the model. #}
{%- set namesafe_cols = [] -%}
{%- set boolean_namesafe_cols=[] -%}

{# Treat reserved word colums and boolean reserved word columns with double quotes #}
{%- for val in reserved_cols -%}
    {%- do namesafe_cols.append('"' + val + '"') -%}
{%- endfor -%}
{%- for val in boolean_reserved_cols -%}
    {%- do boolean_namesafe_cols.append('"' + val + '"') -%}
{%- endfor -%}

{# 
    Get the columns to be selected in the final output, excluding any default exception columns, plus any reserved and reserved boolean columns. 
    The reserved and reserved boolean columns will be included in the output via their respective namesafe lists, where they have been treated to make them selectable.
#}
{%- set select_cols = dbt_utils.get_filtered_columns_in_relation(source_table_relation, except=staging_model_default_exception_cols + reserved_cols + boolean_reserved_cols) -%}
{%- set dupe_key_count = 0 -%}
{%- if dedupe -%}
    {%- set dupe_key_count = get_dupe_key_count(source_table_relation, unique_key) -%}
{%- endif %}

{{
    config(
        materialized='ephemeral'
    )
}}

with

final as (

    select
        {{
            ___build_dw_metadata_cols(
                source_schema=source_schema,
                source_table=source_table,
                unique_key=unique_key,
                extracted_at_column=extracted_at_column,
                build_hd=build_hd,
                boolean_cols=boolean_cols + boolean_namesafe_cols,
                reserved_cols=reserved_cols + boolean_reserved_cols,
                namesafe_cols=namesafe_cols,
                jira_task_key=jira_task_key,
                alias='rve_' + source_schema__table
            )  
        }},
        {% if use_backticks %}
            `{{ select_cols | join('`, \n\t\t`') }}`
        {% else %}
            {{ select_cols | join(', \n\t\t') }}
        {% endif %}
        {{ ',' + namesafe_cols | join(', \n\t\t') if namesafe_cols | length > 0 }}
        {{ ',' + boolean_namesafe_cols | join(', \n\t\t') if boolean_namesafe_cols | length > 0 }}
        {%- if dupe_key_count > 0 -%}
        ,row_number() over (
            partition by {{ unique_key | join(',') }}
            order by {{ staging_model_order_by_cols | join(',') }}
        ) as row_num
        {%- endif %}
    from 
        {{ source_table_relation }}

        {%- if false %} {# TO DO: disabled until further testing can be done here #}

            where {{ incremental_timestamp_column }} <= '{{ run_started_at }}'

        {%- endif -%}

)

select
    {{ ___select_dw_metadata_cols(prefix='rve', alias=source_schema__table, hd=build_hd) }},
    {% if use_backticks %}
        `{{ select_cols | join('`, \n\t\t`') }}`
    {% else %}
        {{ select_cols | join(', \n\t\t') }}
    {% endif %}
    {{ ',' + namesafe_cols | join(', \n\t\t') if namesafe_cols | length > 0 }}
    {{ ',' + boolean_namesafe_cols | join(', \n\t\t') if boolean_namesafe_cols | length > 0 }}
from 
    final
where true
    {%- if dupe_key_count > 0 %}
    and row_num = 1 {# Dedupe based on latest record if dupes were detected #}
    {%- endif %}
    
    limit {{ env_var("DBT_CLOUD_TEST_ROW_LIMIT", "ALL") }}
{%- endmacro -%}
