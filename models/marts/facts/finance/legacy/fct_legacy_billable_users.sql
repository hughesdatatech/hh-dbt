{%- set unique_key = ['subscription_id'] -%}

with 

final as (

    select

        -- metadata
        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='legacy_billable_user_key',
            rec_source="'fct_billable_subscriptions'",
            alias='_fct_legacy_billable_users'
        ) }},

        {{ build_common_legacy_buca_columns(second_col_group=false) }}

        bill_count as bill_generated,

        submission_count as submission,
        total_submissions_count as submission_count,
        total_submissions_amount as total_submission,
        last_submission_date_at as last_submission_date,

        payment_count as payment,
        total_payments_amount as total_payment,
        last_payment_date_at as last_payment_date,

        internal_hold_count as internal_hold,
        hold_date_at as hold_date,
        hold_cleared_date_at as hold_cleared_date,
        hold_type,
        hold_note,

        contract_price,
        program_access_discount_pct as program_access_discount,
        innovation_credits_discount_amount as innovation_credits_discount,
        billing_caps_discount_amount as  billing_caps_discount,
        volume_based_discount_amount as vbd_discount_total,
        total_discounts,
        final_price,
        discounts_applied,

        volume_based_subscription_tier as vbd_subscription_tier,
        cohort

    from 
        {{ ref("fct_billable_subscriptions") }}
    where true

)

select *
from final
where true
