with 

billing_partnerships as (

    select
        insurers.customer_insurer_id,
        partnerships.partnership_id,
        partnerships.partnership_name,
        partnerships.allows_cohort_expansion
    from
        {{ ref("stg_hh_db_public__customers_insurers_partnerships") }} as insurers 
        inner join {{ ref("stg_hh_db_public__partnerships") }} as partnerships
            on insurers.partnership_id = partnerships.partnership_id
            and partnerships.partnership_type = 'billing'
    where true

),

final as (

    select 
        partnerships.partnership_name,
        customers.customer_name,
        insurers.insurer_name,
        cust_insurers.billing_type,
        template.billing_category,
        contract.contract_template_id,
        contract.contract_id,
        contract.start_date_at,
        contract.end_date_at,
        contract.chronic_price,
        contract.acute_price,
        partnerships.partnership_id,
        customers.customer_id,
        insurers.insurer_id,
        cust_insurers.customer_insurer_id,
        lob.line_of_business_name,
        partnerships.allows_cohort_expansion
    from 
        {{ ref("stg_hh_db_public__customers_insurers") }} as cust_insurers
        inner join {{ ref("stg_hh_db_contract__contract") }} as contract
            on cust_insurers.customer_insurer_id = contract.customer_insurer_id   
        inner join {{ ref("stg_hh_db_contract__contract_template") }} as template
            on contract.contract_template_id = template.contract_template_id
        left join {{ ref("stg_hh_db_public__customers") }} as customers
            on cust_insurers.customer_id = customers.customer_id
        left join {{ ref("stg_hh_db_public__insurers") }} as insurers 
            on cust_insurers.insurer_id = insurers.insurer_id
        left join billing_partnerships as partnerships
            on cust_insurers.customer_insurer_id = partnerships.customer_insurer_id
        left join {{ ref("stg_hh_db_public__line_of_business") }} as lob
            on cust_insurers.line_of_business_id = lob.line_of_business_id
    where true
    group by all

)

select *
from final
where true
