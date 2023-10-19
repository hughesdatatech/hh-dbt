{% snapshot fct_customer_trailing_unattributed_multipliers_snapshot %}

    {{
        config(
            target_schema=target.get('schema'),
            unique_key='customer_trailing_unattributed_multiplier_key',
            strategy='timestamp',
            updated_at='_fct_customer_trailing_unattributed_multipliers_loaded_at',
            invalidate_hard_deletes=True
        )
    }}

    select 
        * 
    from 
        {{ ref("fct_customer_trailing_unattributed_multipliers") }}
    where true

{% endsnapshot %}
