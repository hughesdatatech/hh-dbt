{% snapshot fct_predicted_touchpoint_conversion_rates_snapshot %}

    {{
        config(
            target_schema=target.get('schema'),
            unique_key='touchpoint_key',
            strategy='timestamp',
            updated_at='_fct_predicted_touchpoint_conversion_rates_loaded_at',
            invalidate_hard_deletes=True
        )
    }}

    select 
        * 
    from 
        {{ ref("fct_predicted_touchpoint_conversion_rates") }}
    where true

{% endsnapshot %}
