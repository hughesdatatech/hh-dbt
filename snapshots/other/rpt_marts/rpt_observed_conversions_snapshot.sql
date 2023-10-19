{% snapshot rpt_observed_conversions_snapshot %}

    {{
        config(
            target_schema=target.get('schema'),
            unique_key='observed_conversion_key',
            strategy='timestamp',
            updated_at='_fct_observed_conversions_loaded_at',
            invalidate_hard_deletes=True
        )
    }}

    select 
        * 
    from 
        {{ ref("rpt_observed_conversions") }}
    where true

{% endsnapshot %}
