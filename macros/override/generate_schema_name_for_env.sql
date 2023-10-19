{% macro generate_schema_name_for_env(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if 'dbt_cloud_pr' in custom_schema_name | string -%}

         {{ custom_schema_name | trim }}

    {%- elif (target.name == 'prod' and custom_schema_name is not none) -%}

        {{ custom_schema_name | trim }}

    {%- else -%}

        {{ default_schema }}

    {%- endif -%}

{%- endmacro %}
