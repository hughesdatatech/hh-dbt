{# replaces demand_forecast/SQL/model_inputs/mod0-bob_averages.sql >>> bob_averages #}

with 

final as (

    select 
        (1.0 * sum(employee_count) / sum(total_family_count))::double as pct_employee,
        (1.0 * sum(spouse_count) / sum(total_family_count))::double as pct_spouse,
        (1.0 * sum(female_count) / sum(total_male_female_count))::double as pct_female
    from 
        {{ ref("int_bob_averages_by_customer") }}
    where true

)

select *
from final
where true
