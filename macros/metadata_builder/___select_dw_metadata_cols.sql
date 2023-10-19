{# not used #}
{% macro ___select_dw_metadata_cols(prefix='', alias='', hd=False) -%}

    {{ prefix ~ '_' ~ alias ~ '_tenant_key' }},
    {{ prefix ~ '_' ~ alias ~ '_hk' }},
    {{ prefix ~ '_' ~ alias ~ '_rec_source' }},
    {{ prefix ~ '_' ~ alias ~ '_job_id' }},
    {{ prefix ~ '_' ~ alias ~ '_job_user_id' }},
    {{ prefix ~ '_' ~ alias ~ '_jira_task_key' }},
    {{ prefix ~ '_' ~ alias ~ '_extracted_at' }},
    {{ prefix ~ '_' ~ alias ~ '_loaded_at' }}
    {{ ', ' ~ prefix ~ '_' ~ alias ~ '_hd' if hd }}

{%- endmacro %}
