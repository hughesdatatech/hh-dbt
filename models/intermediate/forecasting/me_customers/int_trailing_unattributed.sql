with 

associated_trailing_unattributed_base as (

    select 
        'associated_trailing_unattributed_base' as record_source,   
        customer_id,
        attribution_type,
        program,
        sub_program,
        detail_json,
        activity_date_at,
        sum(weight) as weight
    from  
        {{ ref("int_associated_trailing_unattributed_base") }}
    where true
    {{ dbt_utils.group_by(7) }}


),

unassociated_trailing_base as (

    select 
        cw.customer_id,
        'trailing' as attribution_type,
        'chronic' as program,
        'core' as sub_program,
        cw.activity_date_at,
        sum(tam.unassociated_trailing_daily_signups * cw.num_dates * (case when is_dmc_included then (1 - hcm.value ) else 1 end)) as weight
    from 
        {{ ref("int_customer_weeks_with_dmc_indicator") }} as cw
        inner join {{ ref("fct_customer_trailing_unattributed_multipliers") }} as tam 
            on cw.customer_id = tam.customer_id
            and date_trunc('month', cw.activity_date_at) = tam.activity_month_at
        {{ build_hardcoded_multiplier_join('acute_to_chronic_ratio', 'cw', true, true) }}
    where true
    {{ dbt_utils.group_by(5) }}

),

unassociated_trailing as (

    select
        'unassociated_trailing' as record_source, 
        customer_id,
        attribution_type,
        program,
        sub_program,
        to_json(
            named_struct(
                'forecast_run_date', {{ get_run_started_date_at() }},
                'attribution_type', attribution_type,
                'program', program,
                'sub_program', sub_program,
                'attribution_detail', 'unassociated'
            )
        ) as detail_json,
        activity_date_at,
        weight
    from 
        unassociated_trailing_base
    where true

),

final as (

    select *
    from associated_trailing_unattributed_base
    where true

    union all

    select *
    from unassociated_trailing
    where true


)

select 
    'int_trailing_unattributed | ' || f.record_source as record_source,
    f.customer_id,
    f.attribution_type,
    f.program,
    f.sub_program,
    f.detail_json,
    f.activity_date_at,
    f.weight
from 
    final as f
where 
    true
