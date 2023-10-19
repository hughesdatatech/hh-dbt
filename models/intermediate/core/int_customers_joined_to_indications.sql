with

final as (

    select 
        act.account_id,
        act.customer_id,
        pri.program_indication_identifier,
        {{ select_program_indicators() }}
    from 
        {{ ref("dim_accounts") }} as act 
        inner join {{ ref("stg_hh_db_public__recruitments") }} as re 
            on act.customer_id = re.customer_id
        inner join {{ ref("stg_hh_db_public__program_indications") }} as pri 
            on re.program_indication_id = pri.program_indication_id
    where true
        and re.ends_at is null

)

select *
from final
where true
