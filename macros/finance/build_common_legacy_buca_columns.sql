{% macro build_common_legacy_buca_columns(second_col_group=true) %}

        {# First group of common columns, used by billable users, itemized remits, and itemized submissions models. #}
        partnership_name as partnership,
        partnership_id,
        customer_name as client,
        customer_id as client_id,
        insurer_name as insurer,
        insurer_id,
        billing_type as billing_type,
        billing_category as billing_category,
        line_of_business_name as line_of_business,

        customer_insurer_id as clients_insurer_id,
        subscription_id as subscription_id,
        member_id as user_id,
        pathway_id as pathway_id,

        program,
        indication,
        pathway_accepted_at as pathway_accepted_date,
        first_et_at as first_et,

        subscription_starts_at as subscription_start,
        subscription_ends_at as subscription_end,
        subscription_month_starts_at as subscription_month,
        subscription_quarter_starts_at as subscription_quarter,
        subscription_created_at as subscription_created,
        subscription_year_count as user_program_subscription_year,
        is_subscription_voided as subscription_void,
        subscription_voided_at as subscription_void_date,

        {# Second group of common columns, used by itemized remits, and itemized submissions models. #}
        {% if second_col_group == true %}

        bill_id as bill_id,
        claim_identifier as claim_identifier,
        bill_created_at as claim_created,
        bill_from_date_at as claim_from,
        bill_to_date_at as claim_to,
        billing_month_at as billing_month,
        bill_payment_type as claim_phase,
        base_charge as base_charge,

        submission_id as submission_id,
        transaction_id as transaction_id,
        submission_date_at as submission_date,
        submission_year_month_at as submission_year_month,
        submission_amount,
        voided_submission_count as submission_void,
        submission_status as status,
        submission_status_updated_at as status_update_at,
        
        write_off_count as written_off,
        write_off_id as write_off_id,
        write_off_created_at as write_off_date,
        write_off_reason,
        write_off_amount,

        archive_count as archived,
        archive_id as archive_id,
        archive_created_at as archive_date,
        archive_reason as archive_reason,
        archive_amount as archive_amount

        {% endif %}

{%- endmacro -%}
