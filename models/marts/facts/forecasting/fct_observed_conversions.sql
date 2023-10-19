{%- set unique_key = ['touchpoint_id', 'marketing_id', 'member_id', 'screening_id', 'detail_json', 'activity_date_at'] -%}

with 

final as (

    select

        -- Metadata
        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='observed_conversion_key',
            hd_source_model='int_observed_conversions',
            hd_except_cols=unique_key,
            rec_source="'int_observed_conversions'",
            alias='_fct_observed_conversions'
        ) }},

        -- Dimension Keys
        {{ build_hash_value(value=build_hash_diff(cols=['member_id']), alias='member_key') }},
        {{ build_hash_value(value=build_hash_diff(cols=['customer_id']), alias='customer_key') }},
        {{ build_hash_value(value=build_hash_diff(cols=['account_id']), alias='account_key') }},
        activity_date_at_key,

         -- Related Fact Keys
        {{ build_hash_value(value=build_hash_diff(cols=['screening_id']), alias='screening_key') }},
        {{ build_hash_value(value=build_hash_diff(cols=['deployment_id']), alias='deployment_key') }},
        {{ build_hash_value(value=build_hash_diff(cols=['pathway_id']), alias='pathway_key') }},

        -- Business Keys
        touchpoint_id,
        member_id,
        screening_id, -- TO DO: What is this? Sort of like enrollment id?
        customer_id,
        account_id,
        deployment_id,
        pathway_id,
       
        -- Misc Attributes
        screener_outcome,
        program,
        sub_program,
        activity_type,
        attribution_type,
        trailing_attribution_type,
        detail_json,

        -- Indicators

        -- Dates
        activity_date_at,
        activity_timestamp_at,
        _fct_forecasted_accrued_customer_touchpoint_conversions_loaded_at,
        _fct_marketing_touchpoints_consolidated_loaded_at,

        -- Metrics
        conversion_count,
        attributed_conversion_count,
        trailing_conversion_count,
        unattributed_conversion_count,
        trailing_associated_conversion_count,
        trailing_unassociated_conversion_count,
        y2_signups_conversion_count,
        enso_conversion_count

    from 
        {{ ref("int_observed_conversions") }}
    where true

)

select *
from final
where true
