{%- macro get_source_schema__table(model_name) -%}

    {%- set source_schema__table = '' -%}

    {%- if model_name.startswith('stg_') -%}
        {%- set source_schema__table = model_name.replace('stg_', '', 1) -%}
    {%- elif model_name.startswith('rv_') -%}
        {%- set source_schema__table = model_name.replace('rv_', '', 1) -%}
    {%- elif model_name.startswith('rve_') -%}
        {%- set source_schema__table = model_name.replace('rve_', '', 1) -%}
    {%- endif -%}

    {{ return(source_schema__table) }}

{%- endmacro -%}
