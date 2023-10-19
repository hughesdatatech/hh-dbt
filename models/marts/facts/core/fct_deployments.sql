{%- set unique_key = ['deployment_id'] -%}

with 

final as (

    select

        -- Metadata
        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='deployment_key',
            hd_source_model='int_deployments',
            hd_except_cols=unique_key,
            rec_source="'int_deployments'",
            alias='_fct_deployments'
        ) }},

        -- Dimension Keys
        {{ build_hash_value(value=build_hash_diff(cols=['account_id']), alias='account_key') }},
        deployment_launch_date_1_at_key,
        deployment_launch_date_2_at_key,
        deployment_launch_date_3_at_key,
        deployment_launch_2_date_at_key, 

        -- Business Keys
        deployment_id,
        account_id,

        -- Misc Attributes
        deployment_1_number,
        deployment_2_number,
        deployment_name,

        -- JSON

        -- Indicators

        -- Dates
        deployment_launch_date_1_at,
        deployment_launch_date_2_at,
        deployment_launch_date_3_at,
        deployment_launch_2_date_at, 

        -- Metrics
        hard_cap,
        emerging_covered_lives,
        annual_counter

    from 
        {{ ref("int_deployments") }} as dp -- TO DO: join to dim_accounts to get account_key if implementing type-2 scds
    where true

)

select *
from final
where true
