{% macro build_rec_source(schema_name, table_name, alias) -%}

    '{{ source(schema_name, table_name) | replace("\"", "") | replace("`", "") }}' as {{ alias }}_rec_source

{%- endmacro %}
