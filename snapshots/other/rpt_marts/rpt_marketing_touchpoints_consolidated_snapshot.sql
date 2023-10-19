{% snapshot rpt_marketing_touchpoints_consolidated_snapshot %}

    {{
        config(
            target_schema=target.get('schema'),
            unique_key='touchpoint_key',
            strategy='timestamp',
            updated_at='_fct_marketing_touchpoints_consolidated_loaded_at',
            invalidate_hard_deletes=True
        )
    }}

    select 
        * 
    from 
        {{ ref("rpt_marketing_touchpoints_consolidated") }}
    where true

{% endsnapshot %}
