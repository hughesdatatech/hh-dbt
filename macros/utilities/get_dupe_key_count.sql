{% macro get_dupe_key_count(model, unique_key) %}

    {%- set dupe_query -%}

        select 
            count(1) as dupe_key_count
        from (
            select 
            {% for col in unique_key %}
                {{ col }}{% if not loop.last %},{% endif -%}
            {% endfor %} 
            from {{ model }} 
            group by 
            {% for col in unique_key %}
                {{ col }}{% if not loop.last %},{% endif -%}
            {% endfor %}
            having 
                count(1) > 1
        )
    
    {%- endset -%}

    {%- set results = run_query(dupe_query) -%}

    {%- if execute -%}

        {%- set results_list = results.columns[0].values() -%}
        {%- set dupe_key_count = results_list[0] -%}

    {%- else -%}

        {%- set dupe_key_count = 0 -%}

    {%- endif -%}

    {{ return(dupe_key_count) }}

{% endmacro %}
