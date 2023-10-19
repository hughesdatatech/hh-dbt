with 

final as (

    select 
        *
    from 
        {{ ref("int_chronic_base") }}
    where
        sub_program = 'core'

)

select *
from final
where true
