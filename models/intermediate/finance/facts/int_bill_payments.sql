with

final as (

    select

        bills.*,

        remits.remit_id,
        remits.payment_date_at,
        remits.patient_payment_amount,
        remits.payer_name,
        remits.payment_number,
        remits.icn,
        remits.remit_payment_type

    from 
        {{ ref("int_submitted_bills") }} as bills
        inner join {{ ref("stg_billing_billing__remits") }} as remits 
            on bills.submission_id = remits.submission_id

)

select *
from final
where true
