with 

subscriptions as (

    select
        subscription_id,
        customer_insurer_id,
        contract_id,
        pathway_id,
        member_id,
        is_subscription_voided,
        subscription_voided_at,
        subscription_starts_at,
        date_format(subscription_starts_at, 'yyyy-MM') as subscription_month_starts_at,
        concat(date_format(subscription_starts_at, 'yyyy'),'-Q', date_format(subscription_starts_at, 'Q')) as subscription_quarter_starts_at,
        subscription_ends_at, 
        subscription_created_at,
        subscription_year_count
    from 
        {{ ref("stg_billing_billing__billing_subscriptions") }} 
    where true

),

submissions as (

    select 
        bills.subscription_id,
        count(submissions.bill_id)::int as total_submissions_count,
        sum(submissions.submission_amount)::double as total_submissions_amount,
        max(submissions.submission_date_at) as last_submission_date_at
    from 
        {{ ref("stg_billing_billing__bills") }} as bills
        inner join {{ ref("stg_billing_billing__submissions") }} as submissions 
            on bills.bill_id = submissions.bill_id
        left join {{ ref("stg_billing_billing__archive") }} as archive 
            on submissions.submission_id = archive.submission_id
    where true
        and submissions.is_valid_submission_status
        and archive.submission_id is null
    group by 1
    
),

remits as (

    select 
        bills.subscription_id,
        max(remits.remit_response_date_at::date) as last_payment_date_at,
        sum(remits.patient_payment_amount)::double as total_payments_amount
    from 
        {{ ref("stg_billing_billing__bills") }} as bills
        inner join {{ ref("stg_billing_billing__submissions") }} as submissions
            on bills.bill_id = submissions.bill_id
        inner join {{ ref("stg_billing_billing__remits") }} as remits 
            on submissions.submission_id = remits.submission_id
    where true
    group by 1

),

internal_holds as (

    select 
        bills.subscription_id,
        min(holds.hold_date_at) as hold_date_at,
        max(holds.hold_cleared_date_at) as hold_cleared_date_at,
        array_agg(holds.hold_type) as hold_type,
        array_agg(holds.hold_note) as hold_note
    from 
        {{ ref("stg_billing_billing__bills") }} as bills
        inner join {{ ref("stg_billing_billing__internal_holds") }} as holds 
            on bills.bill_id = holds.bill_id
    where true
        and holds.hold_cleared_date_at is null
    group by 1
   
),

applied_discounts as (

    select 
        discounts.source_id as subscription_id,
        discounts.discount_type,
        discounts.tier,
        discounts.discount_amount::double,
        details.subscription_tier 
    from 
        {{ ref("stg_billing_billing__applied_discounts") }} as discounts
        left join {{ ref("stg_hh_db_contract__contract_discount_details") }} as details 
            on discounts.discount_detail_id = details.discount_detail_id
    where true
        and discounts.discount_status = 'assigned'
        and discounts.source_name = 'subscriptions'

),

-- Program Discounting
program_access as (

    select 
        *
    from 
        applied_discounts
    where true
        and discount_type = 'program_access'

),

innovation_credits as (

    select 
        *
    from 
        applied_discounts
    where true
        and discount_type = 'innovation_credits'

),

billing_caps as (

    select 
        *
    from 
        applied_discounts
    where true
        and discount_type = 'billing_caps'
        and tier = 2

),

volume_based_discounts as (

    select 
        *
    from 
        applied_discounts
    where true
        and discount_type = 'volume_based_discounts'

),

all_discounts as (

    select 
        subscription_id,
        array_agg(discount_type) as discounts_applied
    from 
        applied_discounts
    where true 
    group by 1

),

small_cohort_groups as (

    select 
        partnerships.partnership_name,
        partnerships.customer_name,
        pathways.program_cohort_grouping,
        subscriptions.subscription_month_starts_at,
        concat(partnerships.partnership_name, '-', pathways.program_cohort_grouping, '-', subscriptions.subscription_month_starts_at) as cohort,
        count(subscriptions.subscription_id)::int as subscription_count
    from 
        subscriptions
        inner join {{ ref("int_partnerships_joined_to_customers_insurers") }} as partnerships 
            on subscriptions.customer_insurer_id = partnerships.customer_insurer_id
            and subscriptions.contract_id = partnerships.contract_id
        inner join {{ ref("stg_hh_db_rollups__pathways") }} as pathways
            on subscriptions.pathway_id = pathways.pathway_id
    where true
        and partnerships.allows_cohort_expansion
    {{ dbt_utils.group_by(n=5) }}
    having count(subscriptions.subscription_id) < 10

),

final as (

    select distinct

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
        subscriptions.subscription_month_starts_at,
        subscriptions.subscription_quarter_starts_at,
        subscriptions.subscription_created_at,
        subscriptions.subscription_year_count,
        subscriptions.is_subscription_voided,
        subscriptions.subscription_voided_at,

        case 
            when bills.subscription_id is not null 
                then 1 
            else 0
        end::int as bill_count,

        case 
            when submissions.subscription_id is not null 
                then 1
            else 0 
        end::int as submission_count,
        submissions.total_submissions_count,
        submissions.total_submissions_amount::double,
        submissions.last_submission_date_at,

        case
            when remits.subscription_id is not null and remits.total_payments_amount > 0 
                then 1 
            else 0 
        end::int as payment_count,
        remits.total_payments_amount,
        remits.last_payment_date_at,

        case 
            when internal_holds.subscription_id is not null 
                then 1 
            else 0 
        end::int as internal_hold_count,
        internal_holds.hold_date_at,
        internal_holds.hold_cleared_date_at,
        internal_holds.hold_type,
        internal_holds.hold_note,

        case
            when pathways.program_cohort_grouping = 'chronic-surgery'
                then partnerships.chronic_price
            else partnerships.acute_price
        end as contract_price,
        coalesce(program_access.discount_amount * .01 * contract_price, 0)::double as program_access_discount_pct,
        coalesce(innovation_credits.discount_amount, 0.0) as innovation_credits_discount_amount,
        coalesce(billing_caps.discount_amount, 0.0) as billing_caps_discount_amount,
        coalesce(volume_based_discounts.discount_amount, 0.0) as volume_based_discount_amount,
        -- combine program access, innovation credits, billing caps, and volume based discounts
        program_access_discount_pct + innovation_credits_discount_amount + billing_caps_discount_amount + volume_based_discount_amount as total_discounts,
        contract_price - total_discounts as final_price,
        all_discounts.discounts_applied,

        concat(
            program_access.subscription_tier, innovation_credits.subscription_tier, 
            billing_caps.subscription_tier, volume_based_discounts.subscription_tier
        ) as volume_based_subscription_tier,
        coalesce(
            small_cohort_groups.cohort, 
            concat(
                coalesce(partnerships.partnership_name, 'Direct') , '-' , partnerships.customer_name, '-', 
                pathways.program_cohort_grouping, '-', subscriptions.subscription_month_starts_at
            )
        ) as cohort

    from 
        subscriptions
        inner join {{ ref("stg_hh_db_rollups__pathways") }} as pathways
            on subscriptions.pathway_id = pathways.pathway_id
        inner join {{ ref("int_partnerships_joined_to_customers_insurers") }} as partnerships
            on subscriptions.customer_insurer_id = partnerships.customer_insurer_id
            and subscriptions.contract_id = partnerships.contract_id
        left join {{ ref("stg_billing_billing__bills") }} as bills
            on subscriptions.subscription_id = bills.subscription_id
        left join submissions 
            on subscriptions.subscription_id = submissions.subscription_id
        left join remits
            on subscriptions.subscription_id = remits.subscription_id
        left join internal_holds
            on subscriptions.subscription_id = internal_holds.subscription_id
        left join program_access
            on subscriptions.subscription_id = program_access.subscription_id
        left join innovation_credits
            on subscriptions.subscription_id = innovation_credits.subscription_id
        left join billing_caps
            on subscriptions.subscription_id = billing_caps.subscription_id
        left join volume_based_discounts 
            on subscriptions.subscription_id = volume_based_discounts.subscription_id
        left join all_discounts 
            on subscriptions.subscription_id = all_discounts.subscription_id
        left join small_cohort_groups
            on partnerships.partnership_name = small_cohort_groups.partnership_name
            and partnerships.customer_name = small_cohort_groups.customer_name
            and pathways.program_cohort_grouping = small_cohort_groups.program_cohort_grouping
            and subscriptions.subscription_month_starts_at = small_cohort_groups.subscription_month_starts_at
    where true

)

select *
from final
where true
