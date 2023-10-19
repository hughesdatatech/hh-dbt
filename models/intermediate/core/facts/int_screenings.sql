with

final as (

    select

        -- Business Keys
        sc.screening_id,
        sc.member_id,
        sc.pathway_id,

        -- Misc Attributes

        -- Indicators
        sc.was_application_accepted,
        
        -- Dates
        sc.screening_created_at_key,
        sc.screening_created_at

        -- Metrics
        
    from 
        {{ ref("stg_hh_db_rollups__screenings") }} as sc 
    where true
    
)

select *
from final
where true
