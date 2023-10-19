{%- set unique_key = ['_meta_model_type', '_meta_model_version', '_meta_model_features', 'customer_id', 'touchpoint_id', 'in_holdout', 'activity_date_at'] -%}

with

final as (

    select

        -- Metadata
        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='forecasted_accrued_customer_touchpoint_conversion_key',
            hd_source_model='int_forecasted_accrued_customer_touchpoint_conversions',
            hd_except_cols=unique_key,
            rec_source="'int_forecasted_accrued_customer_touchpoint_conversions'",
            alias='_fct_forecasted_accrued_customer_touchpoint_conversions'
        ) }},
        _meta_model_type,
        _meta_model_version,
        _meta_model_features,

         -- Dimension Keys
        {{ build_hash_value(value=build_hash_diff(cols=['customer_id']), alias='customer_key') }},
        {{ build_date_key('activity_date_at', 'activity_date_at') }},

        -- Related Fact Keys
        -- touchpoint key?

        -- IDs
        customer_id,
        touchpoint_id,

        -- Misc Attributes
        in_holdout,

        -- Dates
        activity_date_at,

        forecasted_conversions

    from 
        {{ ref("int_forecasted_accrued_customer_touchpoint_conversions") }}
    where
        true

)

select *
from final
where true
