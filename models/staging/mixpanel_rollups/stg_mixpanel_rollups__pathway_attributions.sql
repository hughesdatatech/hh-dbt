select 

    -- Metadata
    -- using occurred_at for data freshness

    -- Business Keys
    pathway_id,

    user_id as member_id,
	user_uuid as member_uuid,
	client as customer_name,
    marketing_id,
    template_id,
    insert_id,

    -- Misc Attibutes
    medium, 
    source, 
    campaign, 
    template, 
    content, 
    term, 
    sequence_no, 
    exp_name, 
    exp_variant, 
    mailformat, 
    aud_segment, 
    template_name, 
    experience, 
    referrer,

    -- JSON

    -- Indicators

    -- Dates
    screening_created_at,
	occurred_at

    -- Metrics

from 
   {{ source('mixpanel_rollups', 'mixpanel_pathway_attribution') }}
