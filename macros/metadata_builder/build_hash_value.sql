{%- macro build_hash_value(value, alias='null') -%}

    try_cast(sha2({{ value }}, 256) as string) {{ alias if alias != 'null' }}

{%- endmacro -%}
