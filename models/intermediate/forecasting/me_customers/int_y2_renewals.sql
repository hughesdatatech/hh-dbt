{%- set y2_renewals_start_at = "'2023-01-01'::date" -%}

with

final as (

    select
        'int_y2_renewals | ' || y2.record_source as record_source,
        y2.customer_id,
        y2.marketing_id,
        y2.member_uuid,
        y2.attribution_type,
        y2.program,
        y2.sub_program,
        y2.detail_json,
        y2.activity_date_at,
        (1 - nvl(hcm.value, 0.0)) as cannibalization_adjustment_multiplier,
        y2.weight * cannibalization_adjustment_multiplier as weight, -- remove cannibalized Y2 signups that go into enso instead
        y2.y2_renewal_rate,
        y2.y1_onboarding_rate_for_y2
    from 
        {{ ref("int_y2_renewals_base") }} as y2 
        {{ build_hardcoded_multiplier_join('pct_y2_cannibalized_into_enso', 'y2', true, true) }}
            and y2.activity_date_at between {{ y2_renewals_start_at }} and (date_trunc('year', {{ get_run_started_date_at() }} + interval 2 years) - interval 1 days)
    where true

)

select *
from final
where true
