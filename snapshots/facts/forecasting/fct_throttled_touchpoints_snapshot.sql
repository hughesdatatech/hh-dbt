{% snapshot fct_throttled_touchpoints_snapshot %}

    {{
        config(
            target_schema=target.get('schema'),
            unique_key='touchpoint_key',
            strategy='timestamp',
            updated_at='_fct_throttled_touchpoints_loaded_at',
            invalidate_hard_deletes=True
        )
    }}

    select 
        * 
    from 
        {{ ref("fct_throttled_touchpoints") }}
    where true

{% endsnapshot %}
