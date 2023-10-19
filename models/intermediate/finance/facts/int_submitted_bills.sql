with

final as (

    select

        partnerships.partnership_name,
        partnerships.partnership_id,
        partnerships.customer_name,
        partnerships.customer_id,
        partnerships.insurer_name,
        partnerships.insurer_id,
        partnerships.billing_type,
        partnerships.billing_category,
        partnerships.line_of_business_name,

        subscriptions.customer_insurer_id,
        subscriptions.subscription_id,
        subscriptions.member_id,
        subscriptions.pathway_id,

        pathways.program,
        pathways.indication,
        pathways.pathway_accepted_at,
        pathways.first_et_at,

        subscriptions.subscription_starts_at,
        subscriptions.subscription_ends_at,
        date_format(subscriptions.subscription_starts_at, 'yyyy-MM') as subscription_month_starts_at,
        concat(
            date_format(subscriptions.subscription_starts_at, 'yyyy'), '-Q', 
            date_format(subscriptions.subscription_starts_at, 'Q')
        ) as subscription_quarter_starts_at,
        subscriptions.subscription_created_at,
        subscriptions.subscription_year_count,
        subscriptions.is_subscription_voided,
        subscriptions.subscription_voided_at,

        bills.bill_id,
        bills.claim_identifier,
        bills.bill_created_at,
        bills.bill_from_date_at,
        bills.bill_to_date_at,
        bills.billing_month_at,
        bills.bill_payment_type,
        bills.base_charge, 

        submissions.submission_id,
        submissions.transaction_id,
        submissions.submission_date_at,
        date_format(submissions.submission_date_at, 'yyyy-MM') as submission_year_month_at,
        submissions.submission_amount,
        submissions.is_submission_voided::int as voided_submission_count,
        submissions.submission_status,
        submissions.submission_status_updated_at,
        case 
            when write_offs.submission_id is not null 
                then 1 
            else 0 
        end::int as write_off_count,
        write_offs.write_off_id,
        write_offs.write_off_created_at,
        write_offs.write_off_reason,
        write_offs.write_off_amount,
        case 
            when archive.submission_id is not null 
                then 1 
            else 0 
        end::int as archive_count,
        archive.archive_id,
        archive.archive_created_at,
        archive.archive_reason,
        archive.archive_amount

    from 
        {{ ref("stg_billing_billing__billing_subscriptions") }} as subscriptions
        inner join {{ ref("stg_hh_db_rollups__pathways") }} as pathways
            on subscriptions.pathway_id = pathways.pathway_id
        inner join {{ ref("int_partnerships_joined_to_customers_insurers") }} as partnerships 
            on subscriptions.customer_insurer_id = partnerships.customer_insurer_id
            and subscriptions.contract_id = partnerships.contract_id
        inner join {{ ref("stg_billing_billing__bills") }} as bills 
            on subscriptions.subscription_id = bills.subscription_id
        inner join {{ ref("stg_billing_billing__submissions") }} as submissions 
            on bills.bill_id = submissions.bill_id
        left join {{ ref("stg_billing_billing__write_offs") }} as write_offs 
            on submissions.submission_id = write_offs.submission_id
        left join {{ ref("stg_billing_billing__archive") }} as archive 
            on submissions.submission_id = archive.submission_id

)

select * 
from final
where true
