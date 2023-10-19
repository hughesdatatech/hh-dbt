{{ config(
  enabled=false
) }}

/*
    Add in enso okr forecast.
*/

with

final as (

    select 
        'int_enso_okr' as record_source,
        'enso' as attribution_type,
        'chronic' as program,
        'core' as sub_program,
        to_json(
            named_struct(
                'attribution_type', attribution_type,
                'program', program,
                'sub_program', sub_program
            )
        ) as detail_json,
        okr.event_week::date as activity_date_at,
        okr.enso_forecast as weight
    from 
        {{ ref("ref_forecast_okr_additions") }} as okr
    where true

)

select *
from final
where true
