with

final as (

    select 
        *
    from 
        {{ ref("stg_sfdc_rollups__deployments") }}
    where true
   
)

select *
from final
where true
