{%- macro get_source_schema(source_schema__table) -%}

    {{ return(source_schema__table.split('__')[0]) }}

{%- endmacro -%}
