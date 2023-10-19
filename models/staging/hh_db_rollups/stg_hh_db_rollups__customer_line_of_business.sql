select 

    -- Metadata
    -- TO DO: need source metadata, extracted timestamp?

    -- Business Keys
    client_id as customer_id,
    line_of_business,
    lower(line_of_business) as lower_line_of_business,

    -- Misc Attibutes

    -- JSON
    
    -- Indicators
    case
        when lower_line_of_business in ('medicare', 'fully insured', 'medicaid')
            then True
        else False
    end as is_fully_insured

    -- Dates

    -- Metrics

from 
    {{ source('hh_db_rollups', 'client_line_of_business') }}
