{%- macro get_source_table(source_schema__table) -%}

    {{ return(source_schema__table.split('__')[1]) }}

{%- endmacro -%}
