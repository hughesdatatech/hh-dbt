with 

final as (

    select 
        *
    from 
        {{ ref("int_chronic_base") }}
    where
        sub_program = 'womens_public_health'

)

select *
from final
where true
