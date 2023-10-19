with

renamed as (

    select
        customer_id,
        employee_count as _employee_count,
        total_family_count as _total_family_count,
        spouse_count as _spouse_count,
        female_count as _female_count,
        total_male_female_count as _total_male_female_count
    from 
        {{ ref("stg_efile_efile__customers_summary") }}
    where true
        and is_current_record
        and total_family_count > 0

),

final as (

    select 
        customer_id,
        sum(_employee_count)::integer as employee_count,
        sum(_total_family_count)::integer as total_family_count,
        sum(_spouse_count)::integer as spouse_count,
        sum(_female_count)::integer as female_count,
        sum(_total_male_female_count)::integer as total_male_female_count,
        (1.0 * employee_count / total_family_count)::double as pct_employee,
        (1.0 * spouse_count / total_family_count)::double as pct_spouse,
        (1.0 * female_count / total_male_female_count)::double as pct_female
    from 
        renamed
    where true
    group by 1

)

select *
from final
where true
