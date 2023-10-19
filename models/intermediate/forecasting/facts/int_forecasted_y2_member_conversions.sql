with 

final as (

    select
        record_source,
        customer_id,
        marketing_id,
        member_uuid,
        attribution_type,
        program,
        sub_program,
        detail_json,
        activity_date_at,
        cannibalization_adjustment_multiplier,
        weight, 
        y2_renewal_rate,
        y1_onboarding_rate_for_y2
    from 
        {{ ref("int_y2_renewals") }}
    where true
        and endswith(record_source, 'y2_renewals')

)

select *
from final
where true
