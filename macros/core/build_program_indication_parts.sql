{% macro build_program_indication_parts(program_indication_identifier) %}

    split_part({{ program_indication_identifier }}, '_', 1) as program,
    split_part({{ program_indication_identifier }}, '_', 2) as indication

{%- endmacro -%}
