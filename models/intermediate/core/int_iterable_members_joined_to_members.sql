{# replaces SQL/scheduled_sends/ss1-identify_iterable_data.sql >>> marketing_id_mapping #}
with

iterable_members as (

    select 
        imemb.email_address as iterable_email_address,
        imemb.is_gmail_address as is_iterable_gmail_address,
        imemb.marketing_id as iterable_marketing_id,
        imemb.member_id as iterable_member_id,
        imemb.member_uuid as iterable_member_uuid,
        imemb.customer_id as iterable_customer_id,
        imemb.lower_customer_identifier as iterable_customer_identifier,
        imemb.lower_customer_identifier_short as iterable_customer_identifier_short,
        nvl(cust_identifier.customer_id, imemb.customer_id) as customer_id,
        max(imemb.updated_at) as updated_at,
        max(imemb.signed_up_at) as signed_up_at
    from
        {{ ref("stg_iterable_rollups__members") }} as imemb
        left join {{ ref("stg_hh_db_public__customers")}} as cust_identifier
            on imemb.lower_customer_identifier = cust_identifier.customer_identifier
    where true
        and not is_placeholder_email_address
    {{ dbt_utils.group_by(n=9) }}

),

staged_members as (

    select
        imemb.iterable_email_address,
        imemb.is_iterable_gmail_address,
        imemb.iterable_marketing_id,
        imemb.iterable_member_id,
        imemb.iterable_member_uuid,
        imemb.iterable_customer_identifier,
        imemb.iterable_customer_identifier_short,
        imemb.customer_id,
        imemb.updated_at,
        imemb.signed_up_at,
        rmemb.email_address,
        rmemb.marketing_id,
        rmemb.member_id,
        rmemb.member_uuid,
        coalesce(imemb.iterable_marketing_id, rmemb.marketing_id) as final_marketing_id,
        coalesce(imemb.iterable_member_uuid, rmemb.member_uuid) as final_member_uuid,
        case
            when len(imemb.iterable_customer_identifier) > 0 
                    and imemb.customer_id is null
                then true
            else false
        end has_no_match_on_identifier
    from 
        iterable_members as imemb
        left join {{ ref('stg_hh_db_rollups__members') }} as rmemb
            on imemb.iterable_member_id = rmemb.member_id 
            or imemb.iterable_member_uuid = rmemb.member_uuid
    where true

),

final as (

    select
        memb.*,
        case
            when memb.has_no_match_on_identifier
                    and xwalk.master_customer_id is not null
                then xwalk.master_customer_id
            else memb.customer_id
        end as master_customer_id
    from 
        staged_members as memb
        left join {{ ref("ref_iterable_customers_to_master_customers_xwalk")}} as xwalk
            on memb.iterable_customer_identifier_short = xwalk.iterable_lower_customer_identifier_short
    where true

)

select *
from final
where true
