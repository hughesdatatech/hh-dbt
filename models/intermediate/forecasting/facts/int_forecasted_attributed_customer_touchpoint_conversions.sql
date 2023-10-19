with 

final as (

    select *
    from {{ ref("int_chronic_all") }}
    where true

    union all

    select *
    from {{ ref("int_acute_core") }}
    where true

)

select *
from final
where true
