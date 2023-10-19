{%- set dummy_data_start_at = "'2023-01-01'::date" -%}
{%- set dummy_data_end_at = "'2023-12-31'::date" -%}

with

/*
    Template for dummy data used in the ctes below.
*/
dummy_template as (

    select
        'int_okr_additions' as record_source,
        'topline_adjustments' as attribution_type,
        'chronic' as program,
        dt.date_day as activity_date_at,
        0.001 as weight
    from 
        {{ ref("dim_dates") }} as dt
    where true 
        and dt.date_day between {{ dummy_data_start_at }} and {{ dummy_data_end_at }}
    
),

international_okr as (

    select
        record_source,
        attribution_type,
        program,
        'international' as adjustment_type,
        to_json(
            named_struct(
                'attribution_type', attribution_type,
                'program', program,
                'adjustment_type', adjustment_type
            )
        ) as detail_json,
        activity_date_at,
        weight
    from 
        dummy_template
    where true 
    
),

va_okr as (

    select
        record_source,
        attribution_type,
        program,
        'veterans_affairs' as adjustment_type,
        to_json(
            named_struct(
                'attribution_type', attribution_type,
                'program', program,
                'adjustment_type', adjustment_type
            )
        ) as detail_json,
        activity_date_at,
        weight
    from 
        dummy_template
    where true 
    
),

intra_year as (

    select
        record_source,
        attribution_type,
        program,
        'intra_year_launches' as adjustment_type,
        to_json(
            named_struct(
                'attribution_type', attribution_type,
                'program', program,
                'adjustment_type', adjustment_type
            )
        ) as detail_json,
        activity_date_at,
        weight
    from 
        dummy_template
    where true 
    
),

final as (

    select *
    from international_okr
    where true

    union all

    select *
    from va_okr
    where true

    union all

    select *
    from intra_year
    where true

)

select *
from final
where true
