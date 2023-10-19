{% macro build_is_gmail_indicator(email_address) %}

     case
        when {{ email_address }} ilike '%gmail.com'
            then True
        else False
    end as is_gmail_address

{% endmacro %}
