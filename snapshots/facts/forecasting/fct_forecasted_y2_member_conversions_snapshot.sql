{% snapshot fct_forecasted_y2_member_conversions_snapshot %}

    {{
        config(
            target_schema=target.get('schema'),
            unique_key='forecasted_y2_member_conversion_key',
            strategy='timestamp',
            updated_at='_fct_forecasted_y2_member_conversions_loaded_at',
            invalidate_hard_deletes=True
        )
    }}

    select 
        * 
    from 
        {{ ref("fct_forecasted_y2_member_conversions") }}
    where true

{% endsnapshot %}
