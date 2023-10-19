{{ config(
  enabled=false
) }}

{%- set fcast_program_filter = "'chronic'" -%}
{%- set fcast_year_filter = '2022' -%}

with

all_fcast as (

    select
        customer_id,
        attribution_type,
        program,
        sub_program,
        activity_date_at,
        weight
    from 
        {{ ref("fct_forecasted_attributed_customer_touchpoint_conversions") }}
    where true
        and program = {{ fcast_program_filter}}
        and year(activity_date_at) = {{ fcast_year_filter }}

    union all

    select
        customer_id,
        attribution_type,
        program,
        sub_program,
        activity_date_at,
        weight
    from 
        {{ ref("int_trailing_unattributed") }}
    where true
        and program = {{ fcast_program_filter}}
        and year(activity_date_at) = {{ fcast_year_filter }}

    union all

    select
        customer_id,
        attribution_type,
        program,
        sub_program,
        activity_date_at,
        weight
    from 
        {{ ref("int_y2_renewals") }}
    where true
        and program = {{ fcast_program_filter}}
        and year(activity_date_at) = {{ fcast_year_filter }}

),

weekly_fcast as (

    select
        customer_id,
        attribution_type,
        program,
        sub_program,
        to_json(
            named_struct(
                'attribution_type', attribution_type,
                'program', program,
                'sub_program', sub_program,
                'okr_goal_addition', True
            )
        ) as detail_json,
        attribution_type = 'y2_signups' as is_y2_signup,
        date_trunc('week', activity_date_at) as activity_date_at,
        sum(weight) as weight
    from 
        all_fcast
    where true
    {{ dbt_utils.group_by(7) }}

),

final as (

    select 
        'int_weighted_okrs' as record_source,
        fc.customer_id,
        fc.attribution_type,
        fc.program,
        fc.sub_program,
        fc.detail_json,
        fc.activity_date_at,
        case 
            when fc.is_y2_signup
                then okr.y2_organic 
            else okr.legacy_chronic
        end * fc.weight / sum(fc.weight) over (partition by fc.activity_date_at, fc.is_y2_signup) as weight -- weight okr goal across attributed, unattributed, and trailing, and across customers
    from 
        weekly_fcast as fc
        left join {{ ref("ref_forecast_okr_additions") }} as okr
            on okr.event_week = fc.activity_date_at
    where true

)

select *
from final
where true