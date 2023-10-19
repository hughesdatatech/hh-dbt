{%- set unique_key = ['pathway_id'] -%}

with 

final as (

    select

        -- Metadata
        {{ build_standard_metadata_cols(
            unique_key=unique_key,
            unique_key_name='pathway_key',
            hd_source_model='int_pathways',
            hd_except_cols=unique_key,
            rec_source="'int_pathways'",
            alias='_fct_pathways'
        ) }},

        -- Dimension Keys
        {{ build_hash_value(value=build_hash_diff(cols=['member_id']), alias='member_key') }},

        -- Business Keys
        pt.pathway_id,
        pt.member_id,
       
        -- Misc Attributes
        program,
        indication,
        
        -- Indicators
        {{ select_program_indicators() }}

        -- Dates
    
        -- Metrics

    from 
        {{ ref("int_pathways") }} as pt -- TO DO: join to dim_members to get member_key if implementing type-2 scds
    where true

)

select *
from final
where true
