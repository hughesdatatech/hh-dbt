{%- set enso_data_start_at = "'2022-11-05'::date" -%}

with 

base as (

    select 
        'int_enso' as record_source,
        y2.customer_id,
        'enso' as attribution_type,
        y2.program,
        y2.sub_program,
        to_json(
            named_struct(
                'attribution_type', 'enso',
                'program', y2.program,
                'sub_program', y2.sub_program
            )
        ) as detail_json,
        y2.activity_date_at + interval 4 weeks as activity_date_at,
        sum(y2.weight * nvl(hcm.value, 0.0) / y2.y2_renewal_rate) as weight, -- only assume some % of subscriptions will sign up with enso, fixed by megan and enso team 
        max(y2.y2_renewal_rate) as y2_renewal_rate
    from
        {{ ref("int_y2_renewals_base") }} as y2
        {{ build_hardcoded_multiplier_join('pct_onboarded_y1_renewing_into_enso', 'y2', true, true) }}
    where true
        and y2.activity_date_at between {{ enso_data_start_at }} and (date_trunc('year', {{ get_run_started_date_at() }} + interval 2 years) - interval 1 days) -- 8 weeks before start of 2023 to enable lag above
    {{ dbt_utils.group_by(7) }}
),

/*
    Add in estimated lift for the state of NJ.
*/
final as (

    select
        record_source,
        customer_id,
        attribution_type,
        program,
        sub_program,
        detail_json,
        activity_date_at,
        case
            when customer_id = 162
                then weight + hcm.value
            else weight
        end as weight,
        y2_renewal_rate
    from 
        base
        {{ build_hardcoded_multiplier_join('enso_lift_sonj', 'base', true, true) }}
    where true

)

select *
from final
where true
