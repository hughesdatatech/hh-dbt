with

final as (

    select
        customer_id,
        '2023-02-20'::date cv_program_started_at -- date when computer vision launched
    from 
        {{ ref("int_customers_joined_to_indications") }}
    where true
        and is_chronic_program
        and (
            is_ankle_indication
            or is_wrist_indication
            or is_hand_indication
            or is_elbow_indication
            or is_foot_indication
        )
    group by 1, 2
   
)

select *
from final
where true
