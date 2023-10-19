with 

final as (

    select
        record_source,
        customer_id,
        attribution_type,
        program,
        sub_program,
        detail_json,
        activity_date_at,
        weight, 
        null::double as y2_renewal_rate,
        null::double as y1_onboarding_rate_for_y2
    from 
        {{ ref("int_trailing_unattributed") }}
    where true

    union all

    select
        record_source,
        customer_id,
        attribution_type,
        program,
        sub_program,
        detail_json,
        activity_date_at,
        weight, 
        y2_renewal_rate,
        null::double as y1_onboarding_rate_for_y2
    from 
        {{ ref("int_enso") }}
    where true

    union all
    
    select
        record_source,
        customer_id,
        attribution_type,
        program,
        sub_program,
        detail_json,
        activity_date_at,
        weight, 
        y2_renewal_rate,
        y1_onboarding_rate_for_y2
    from 
        {{ ref("int_y2_renewals") }}
    where true
        and endswith(record_source, 'y2_onboarding')

)

select *
from final
where true
