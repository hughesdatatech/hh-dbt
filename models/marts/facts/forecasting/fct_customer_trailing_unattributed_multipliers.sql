{%- set unique_key = ['customer_id', 'activity_month_at_key'] -%}

with 

final as (

    select

        -- Metadata
        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='customer_trailing_unattributed_multiplier_key',
            hd_source_model='int_customer_trailing_unattributed_multipliers',
            hd_except_cols=unique_key,
            rec_source="'int_trailing_multipliers_aggregated | int_unattributed_multipliers_aggregated | py_unattributed_multipliers_regression'",
            alias='_fct_customer_trailing_unattributed_multipliers'
        ) }},

        -- Dimension Keys
        {{ build_hash_value(value=build_hash_diff(cols=['customer_id']), alias='customer_key') }},
        activity_month_at_key,

        -- Business Keys
        customer_id,
       
        -- Misc Attributes

        -- Indicators

        -- Dates
        activity_month_at,

        -- Metrics

        -- Trailing Associated
        associated_multiplier_decile,
        associated_trailing_multiplier,

        -- Trailing Unassociated
        unassociated_multiplier_decile,
        unassociated_trailing_daily_signups,

        -- Unattributed
        months_since_2020,
        unattributed_conversion_count,
        unattributed_multiplier,

        -- Predicted
        predicted_unattributed_multiplier

    from 
        {{ ref("int_customer_trailing_unattributed_multipliers") }}
    where true

)

select *
from final
where true
