{% macro build_loaded_at(alias='') -%}

    {{ get_run_started_timestamp_at() }} {{ alias + '_loaded_at' if alias != 'null' }}

{%- endmacro %}
