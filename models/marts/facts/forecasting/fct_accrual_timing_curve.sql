{%- set unique_key = ['is_email_communication', 'is_customer_communication', 'days_to_conversion'] -%}

with 

final as (

    select

        -- Metadata
        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='accrual_timing_curve_key',
            hd_source_model='int_accrual_timing_curve',
            hd_except_cols=unique_key,
            rec_source="'int_accrual_timing_curve'",
            alias='_fct_accrual_timing_curve'
        ) }},

        -- Dimension Keys
        forecast_date_at_key,

        -- Business Keys

        -- Misc Attributes
        is_email_communication,
        is_customer_communication,

        -- Indicators

        -- Dates
        forecast_date_at,

        -- Metrics
        conversion_count,
        days_to_conversion,
        pct_of_medium

    from 
        {{ ref("int_accrual_timing_curve") }}
    where true

)

select *
from final
where true
