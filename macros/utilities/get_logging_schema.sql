{% macro get_logging_schema() %}

    {% set logging_schema = var("audit_schema") %}

    {% if target.schema != var("prod_schema") %}
        {% set logging_schema = target.schema %}
    {% endif %}
    
    {{ return(logging_schema) }}
    
{% endmacro %}
