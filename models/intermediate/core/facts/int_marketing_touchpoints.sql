with

final as (

    select 
        *
    from 
        {{ ref("stg_sfdc_rollups__marketing_touchpoints") }}
    where true
   
)

select *
from final
where true
