{%- macro build_hash_diff(cols, boolean_cols=[], namesafe_cols=[]) -%}

    {%- for col in cols -%}
        nvl(trim(try_cast(`{{col}}` as string)), '') {%- if not loop.last %} || '{{ var("sanding_value") }}' || {% endif %}
    {%- endfor -%}
    
    {# 
        TO DO:
        The following have not been implemented for use with Databricks.
        Not sure if they are even required.    
    #}
    {%- if boolean_cols|length > 0 %}
        || '{{ var("sanding_value") }}' || 
        {%- for col in boolean_cols -%}
            nvl(trim(try_cast({{ col }}::smallint as string)), '') {%- if not loop.last %} || '{{ var("sanding_value") }}' || {% endif %}
        {%- endfor -%}
    {% endif %}
    {%- if namesafe_cols|length > 0 %}
        || '{{ var("sanding_value") }}' || 
        {%- for col in namesafe_cols -%}
            nvl(trim(try_cast({{ col }} as string)), '') {%- if not loop.last %} || '{{ var("sanding_value") }}' || {% endif %}
        {%- endfor -%}
    {% endif %}

{%- endmacro -%}
