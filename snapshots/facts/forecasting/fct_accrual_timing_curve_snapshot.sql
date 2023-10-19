{% snapshot fct_accrual_timing_curve_snapshot %}

    {{
        config(
            target_schema=target.get('schema'),
            unique_key='accrual_timing_curve_key',
            strategy='timestamp',
            updated_at='_fct_accrual_timing_curve_loaded_at',
            invalidate_hard_deletes=True
        )
    }}

    select 
        * 
    from 
        {{ ref("fct_accrual_timing_curve") }}
    where true

{% endsnapshot %}
