with 

final as (

    select
        record_source,
        attribution_type,
        program,
        null::string as sub_program,
        adjustment_type,
        detail_json,
        activity_date_at,
        weight
    from 
        {{ ref("int_unplanned_touchpoints") }}
    where true

    union all

    select
        record_source,
        attribution_type,
        program,
        null::string as sub_program,
        ''::string as adjustment_type,
        detail_json,
        activity_date_at,
        weight
    from 
        {{ ref("int_enso_hcsc_backlog") }}
    where true

    union all

    select 
        record_source,
        attribution_type,
        program,
        null::string as sub_program,
        adjustment_type,
        detail_json,
        activity_date_at,
        weight
    from 
        {{ ref("int_okr_additions") }}
    where true

)

select *
from final
where true
