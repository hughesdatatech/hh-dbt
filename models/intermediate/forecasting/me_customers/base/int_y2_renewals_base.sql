{%- set subscription_ended_start_at = "'2022-01-01'::date" -%}
{%- set customer_id_exclusions = '(610, 634)' -%}

with 

subs as (

    select
        sub.pathway_id,
        sub.member_id,
        sub.subscription_ends_at as activity_date_at
    from 
        {{ ref("stg_billing_billing__billing_subscriptions") }} as sub
    where true 
        and sub.is_subscription_voided is not distinct from false
        and sub.subscription_year_count = 1

),

y2_renewals as (

     select
        mb.customer_id,
        mb.marketing_id,
        mb.member_uuid,
        sub.pathway_id,
        'y2_signups' as attribution_type,
        pw.program,
        'core' as sub_program,
        date_trunc('week', sub.activity_date_at) + interval 1 weeks as activity_date_at,
        coalesce(hcm.value, 0.0) as weight,
        weight as y2_renewal_rate,
        null::double as y1_onboarding_rate_for_y2
    from 
        subs as sub
        inner join {{ ref("dim_members") }} as mb
            on mb.member_id = sub.member_id
            and mb.member_uuid is not null
        inner join {{ ref("fct_pathways") }} as pw
            on pw.pathway_id = sub.pathway_id
        {{ build_hardcoded_multiplier_join('y2_renewal_rate', 'sub', true, true) }}
        where true
            and sub.activity_date_at between {{ subscription_ended_start_at }} and date_trunc('month', {{ get_run_started_date_at() }} ) + interval 1 years
            and pw.is_chronic_program -- only expect chronic subscriptions to renew, not acute or other
            and mb.customer_id not in {{ customer_id_exclusions }} -- TO DO: what are these? create exclusion indicator in dim_customers?

),

y2_renewals_with_json as (

    select 
        customer_id,
        marketing_id,
        member_uuid,
        attribution_type,
        program,
        sub_program,
        to_json(
            named_struct(
                'pathway_id', pathway_id,
                'attribution_type', attribution_type,
                'program', program,
                'sub_program', sub_program, -- TO DO: update this based on the pathway sub_program for 2024 forecast run
                'pct_renewal', weight
            )
        ) as detail_json,
        activity_date_at,
        weight,
        y2_renewal_rate,
        y1_onboarding_rate_for_y2
    from
        y2_renewals
    where true

),

{%- set all_chronic_filter -%}
    program = 'chronic'
    and activity_date_at between date_trunc('month', {{get_run_started_date_at() }}) and ((date_trunc('year', {{get_run_started_date_at() }}) + interval 2 years) - interval 1 days)
    and customer_id not in {{ customer_id_exclusions }} -- TO DO: what are these? create exclusion indicator in dim_customers?
{%- endset -%}

/*
    Get all chronic forecasts since we only expect chronic subscriptions to renew, not acute or other.
*/
all_chronic as (

    select
        customer_id,
        program,
        sub_program,
        activity_date_at,
        weight
    from 
        {{ ref("fct_forecasted_attributed_customer_touchpoint_conversions") }}
    where true
        -- limit data using common criteria defined above
        and {{ all_chronic_filter }}

    union all

    select
        customer_id,
        program,
        sub_program,
        activity_date_at,
        weight
    from 
        {{ ref("int_trailing_unattributed") }}
    where true
        -- limit data using common criteria defined above
        and {{ all_chronic_filter }}

),

/*
    Add in new users forecasted to sign up over remainder of year.
*/
y2_onboarding as (

    select
        me.customer_id,
        null::string as marketing_id,
        null::string as member_uuid,
        'y2_signups' as attribution_type,
        me.program,
        me.sub_program,
        me.activity_date_at + interval 1 years as activity_date_at,
        sum(me.weight * nvl(hcm.value, 0.0) * nvl(hcm_rr.value, 0.0)) as weight, -- rr values are the renewal rate as shown in above query, while the other 2 are the onboarding rate we expect to see since not all future accepted members will start their subscription
        nvl(max(hcm_rr.value), 0.0) as y2_renewal_rate,
        nvl(max(hcm.value), 0.0) as y1_onboarding_rate_for_y2
    from 
        all_chronic as me
        {{ build_hardcoded_multiplier_join('y2_renewal_rate', 'me', true, true, 'hcm_rr') }}
        {{ build_hardcoded_multiplier_join('y1_onboarding_rate_for_y2', 'me', true, true) }}
    where true
    {{ dbt_utils.group_by(7) }}

),

y2_onboarding_with_json as (

    select 
        customer_id,
        marketing_id,
        member_uuid,
        attribution_type,
        program,
        sub_program,
        to_json(
            named_struct(
                'attribution_type', attribution_type,
                'program', program,
                'sub_program', sub_program,
                'pct_renewal', y2_renewal_rate
            )
        ) as detail_json,
        activity_date_at,
        weight,
        y2_renewal_rate,
        y1_onboarding_rate_for_y2
    from 
        y2_onboarding
    where true

),

final as (

    select
        'int_y2_renewals_base | y2_renewals' as record_source, 
        *
    from 
        y2_renewals_with_json
    where 
        true

    union all 

    select
        'int_y2_renewals_base | y2_onboarding' as record_source, 
        *
    from 
        y2_onboarding_with_json
    where 
        true

)

select *
from final
where true
