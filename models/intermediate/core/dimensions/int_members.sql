with

base as (

    select

        -- Identifiers
        member_id,
        member_uuid,
        email_address,
        marketing_id,
        first_name, 
        last_name,

        -- Related IDs
        customer_id,
        null::string as customer_name

    from 
        {{ ref("stg_hh_db_public__members") }}

),

rollups as (

    select

        -- Identifiers
        member_id,
        member_uuid,
        email_address,
        marketing_id,
        first_name, 
        last_name,

        -- Related IDs
        customer_id,
        customer_name

    from 
        {{ ref("stg_hh_db_rollups__members")}}

), 

combined as (

    select

        -- Business Keys
        nvl(bu.member_id, ru.member_id) as member_id,
        nvl(bu.member_uuid, ru.member_uuid) as member_uuid,
        nvl(bu.marketing_id, ru.marketing_id) as marketing_id,
       
        -- Related IDs
        nvl(bu.customer_id, ru.customer_id) as customer_id,
        nvl(bu.customer_name, ru.customer_name) as customer_name,

        -- Misc Attributes
        nvl(bu.first_name, ru.first_name) as first_name,
        nvl(bu.last_name, ru.last_name) as last_name,
        nvl(bu.email_address, ru.email_address) as email_address,

        -- Retain original values for data quality checks
        bu.member_id as _public_members_member_id,
        bu.member_uuid as _public_members_member_uuid,
        bu.marketing_id as _public_members_member_marketing_id,
        bu.customer_id as _public_members_member_customer_id,
        bu.customer_name as _public_members_member_customer_name,
        bu.first_name as _public_members_member_first_name,
        bu.last_name as _public_members_member_last_name,
        bu.email_address as  _public_members_member_email_address,

        ru.member_id as _rollups_members_member_id,
        ru.member_uuid as _rollups_members_member_uuid,
        ru.marketing_id as _rollups_members_member_marketing_id,
        ru.customer_id as _rollups_members_member_customer_id,
        ru.customer_name as _rollups_members_member_customer_name,
        ru.first_name as _rollups_members_member_first_name,
        ru.last_name as _rollups_members_member_last_name,
        ru.email_address as  _rollups_members_member_email_address

    from 
        base as bu 
        left join rollups as ru 
            on bu.member_id = ru.member_id
    where true

),

final as (

    select
        c.*,
        e.marketing_id as efile_marketing_id,
        e.customer_id as efile_customer_id,
        e.account_name as efile_account_name,
        e.insurer_name as efile_insurer_name, 
        e.relationship as efile_relationship,
        e.postal_code as efile_postal_code, 
	    e.sex as efile_sex,
        e.has_email as has_email_efile, 
        e.has_phone_number as has_phone_number_efile, 
        e.has_address as has_address_efile, 
        e.is_18_plus as is_18_plus_efile, 
        e.is_active as is_active_efile, 
        e._created_at as efile_created_at, 
	    e._updated_at as efile_updated_at,
        e.age as efile_age,
         'stg_hh_db_public__members | stg_hh_db_rollups__members | stg_efile_rollups__eligible_lives' as record_source
    from 
        combined as c 
        left join {{ ref("stg_efile_rollups__eligible_lives") }} as e 
            on c.customer_id = e.customer_id
            and c.marketing_id = e.marketing_id

)

select *
from final
where true
