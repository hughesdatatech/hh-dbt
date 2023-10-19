{%- macro build_tenant_key(tenant_key, alias) -%}

    '{{ tenant_key if tenant_key else var("tenant_keys")[1]}}' as {{ alias }}_tenant_key

{%- endmacro -%}
