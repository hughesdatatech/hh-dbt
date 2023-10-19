with

final as (

    select

        -- Business Keys
        pt.pathway_id,
        pt.member_id,

        -- Misc Attributes,
        program,
        indication,

        -- Indicators
        {{ select_program_indicators() }}
        
        -- Dates
        
        -- Metrics 

    from 
        {{ ref("stg_hh_db_rollups__pathways") }} as pt 
    where true
    
)

select *
from final
where true
