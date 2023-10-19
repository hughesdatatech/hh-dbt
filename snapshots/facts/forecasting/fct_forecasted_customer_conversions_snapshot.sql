{% snapshot fct_forecasted_customer_conversions_snapshot %}

    {{
        config(
            target_schema=target.get('schema'),
            unique_key='forecasted_customer_conversion_key',
            strategy='timestamp',
            updated_at='_fct_forecasted_customer_conversions_loaded_at',
            invalidate_hard_deletes=True
        )
    }}

    select 
        * 
    from 
        {{ ref("fct_forecasted_customer_conversions") }}
    where true

{% endsnapshot %}
