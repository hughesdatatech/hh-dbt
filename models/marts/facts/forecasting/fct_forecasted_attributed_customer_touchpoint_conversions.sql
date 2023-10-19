{%- set unique_key = ['touchpoint_id', 'activity_date_at', 'record_source'] -%}

with 

final as (

    select

        -- Metadata
        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='forecasted_attributed_customer_touchpoint_conversion_key',
            rec_source='record_source',
            alias='_fct_forecasted_attributed_customer_touchpoint_conversions'
        ) }},

        -- Dimension Keys
        {{ build_date_key('activity_date_at', 'activity_date_at') }},
        {{ build_hash_value(value=build_hash_diff(cols=['customer_id']), alias='customer_key') }},

        -- Related Fact Keys
        {{ build_hash_value(value=build_hash_diff(cols=['touchpoint_id']), alias='touchpoint_key') }},

        -- Business Keys
        touchpoint_id,
        customer_id,

        -- Misc Attributes
        attribution_type,
        program,
        sub_program,

        -- JSON
        detail_json,

        -- Indicators
        is_in_holdout,

        -- Dates
        activity_date_at,
        _fct_forecasted_accrued_customer_touchpoint_conversions_loaded_at,
        _fct_marketing_touchpoints_consolidated_loaded_at,

        -- Metrics
        movement_multiplier, 
        cancellation_multiplier,
        cannibalization_adjustment_multiplier,
        aq_shortening_adjustment_multiplier,
        automating_clinical_review_adjustment_multiplier,
        weight

    from 
        {{ ref("int_forecasted_attributed_customer_touchpoint_conversions") }}
    where true

)

select *
from final
where true
