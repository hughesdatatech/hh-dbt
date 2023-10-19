{% macro build_date_key(date_timestamp, alias) %}

    case 
        when try_cast(date_format({{ date_timestamp }}, 'yMMdd') as integer) < cast(date_format('{{ var("dim_dates_min_date") }}', 'yMMdd') as integer)
                or try_cast(date_format({{ date_timestamp }}, 'yMMdd') as integer) > cast(date_format('{{ var("dim_dates_max_date") }}', 'yMMdd') as integer)
            then cast(19000101 as integer)
        else nvl(try_cast(date_format({{ date_timestamp }}, 'yMMdd') as integer), 19000101) 
    end as {{ alias }}_key

{% endmacro %}
