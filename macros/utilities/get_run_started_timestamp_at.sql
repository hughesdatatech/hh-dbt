{%- macro get_run_started_timestamp_at() -%}

    date_trunc('MILLISECOND', '{{ run_started_at }}')

{%- endmacro -%}
