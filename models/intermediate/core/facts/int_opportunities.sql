with

final as (

    select 
        *
    from 
        {{ ref("stg_sfdc_rollups__opportunities") }}
    where true
   
)

select *
from final
where true
