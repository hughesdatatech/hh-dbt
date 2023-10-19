with

final as (

    select
        customer_id,
        '2023-03-27'::date as wph_program_started_at -- date when womens pelvic health launched
    from 
        {{ ref("int_customers_joined_to_indications") }}
    where true
        and is_chronic_program
        and is_pelvic_indication
    group by 1, 2
   
)

select *
from final
where true
