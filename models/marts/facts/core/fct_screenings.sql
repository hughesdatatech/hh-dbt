{%- set unique_key = ['screening_id'] -%}

with 

final as (

    select

        -- Metadata
        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='screening_key',
            hd_source_model='int_screenings',
            hd_except_cols=unique_key,
            rec_source="'int_screenings'",
            alias='_fct_screenings'
        ) }},

        -- Dimension Keys
        {{ build_hash_value(value=build_hash_diff(cols=['member_id']), alias='member_key') }},
        sc.screening_created_at_key,

        -- Business Keys
        sc.screening_id,
        sc.member_id,
        sc.pathway_id,
       
        -- Misc Attributes
        
        -- Indicators
        sc.was_application_accepted,

        -- Dates
        sc.screening_created_at
    
        -- Metrics

    from 
        {{ ref("int_screenings") }} as sc -- TO DO: join to dim_members to get member_key if implementing type-2 scds
    where true

)

select *
from final
where true
