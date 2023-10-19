{%- set enso_data_start_at = "'2023-01-17'::date" -%}
{%- set enso_data_end_at = "'2023-01-31'::date" -%} 
 
with

base as (

    select distinct
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
        dt.week_start_date as activity_date_at
    from
        {{ ref("dim_dates") }} as dt
    where true
         and dt.date_day between {{ enso_data_start_at }} and {{ enso_data_end_at }}

),

final as (

    select 
        'int_enso_hcsc_backlog' as record_source,
        attribution_type,
        program,
        sub_program,
        detail_json,
        activity_date_at,
        coalesce(hcm.value, 0.0) as weight
    from
        base as b 
        {{ build_hardcoded_multiplier_join('enso_backlog_for_hcsc', 'b', true, true) }}
    
)

select *
from final
where true
