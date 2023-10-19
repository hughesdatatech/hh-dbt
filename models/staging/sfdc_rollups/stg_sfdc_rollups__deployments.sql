select 

    -- Metadata
    insertion_date as _source_meta_insertion_date,

    -- Business Keys
    deployment_id,

    account as account_id,

    -- Misc Attributes
    deployment_1_number,
    deployment_2_number,
    deployment_name,

    -- JSON

    -- Indicators

    -- Dates
    {{ build_date_key('launch_date_1', 'deployment_launch_date_1_at') }},
    try_cast(launch_date_1 as date) as deployment_launch_date_1_at,
    {{ build_date_key('launch_date_2', 'deployment_launch_date_2_at') }},
    launch_date_2 as deployment_launch_date_2_at,
    {{ build_date_key('launch_date_3', 'deployment_launch_date_3_at') }},
    launch_date_3 as deployment_launch_date_3_at,
    {{ build_date_key('launch_2_date', 'deployment_launch_2_date_at') }},
    launch_2_date as deployment_launch_2_date_at,

    -- Metrics
    nvl(try_cast(try_cast(cap_amount as double) as integer), 0) as hard_cap,
    emerging_total_covered_lives as emerging_covered_lives,
    nvl(try_cast(try_cast(annual_counter as double) as integer), 0) as annual_counter

from 
   {{ source('sfdc_rollups', 'sf_deployments') }}