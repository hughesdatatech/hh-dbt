{%- set members_columns = dbt_utils.get_filtered_columns_in_relation(from=ref("int_members"), except=[]) -%}
{%- set unique_key = ['member_id'] -%}

with 

members as (

    select 
        *
    from 
        {{ ref("int_members") }}
    where true

    union all

    select 
    {% for column_name in members_columns %}
        {{ 'null,' if column_name != 'record_source' }}
    {% endfor %}
        'system' as record_source

),

final as (

    select 

        -- Metadata
        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='member_key',
            hd_source_model='int_members',
            hd_except_cols=unique_key,
            rec_source='record_source',
            alias='_dim_members'
        ) }},

        -- Business Keys
        member_id,
        member_uuid,
        marketing_id,
       
        -- Related IDs
        customer_id,
        customer_name,

        -- Misc Attributes
        first_name,
        last_name,
        email_address,
        record_source

    from 
        members 
    where true

)

select *
from final
where true
