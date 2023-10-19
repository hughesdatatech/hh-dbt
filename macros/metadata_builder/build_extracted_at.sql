{% macro build_extracted_at(column=var("extracted_at_default"), alias='null') -%}

    {{ column }} {{ alias + '_extracted_at' if alias != 'null' }}

{%- endmacro %}
