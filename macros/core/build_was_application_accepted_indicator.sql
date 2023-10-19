{% macro build_was_application_accepted_indicator() %}

    case
        when relevance in (1, 2, 3)
            then true
        else false
    end as was_application_accepted

{% endmacro %}
