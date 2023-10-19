select 
   *
from 
   {{ source('xsrc_rollups', 'marketing_touchpoints_historical_dbt') }}
where true
    and touchpoint_id is distinct from 'touchpoint_id'
