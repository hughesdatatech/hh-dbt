select 

    -- Metadata
    _rescued_data,
    -- TO DO: need source metadata, extracted timestamp?
    
    -- Business Keys
    mailchimp_crosswalk as mailchimp_customer_list_xwalk_id,

    recipients__list_name as recipients_list_name, 
    client_id as customer_id, 
    list_id as mailchimp_list_id
		
    -- Misc Attibutes

    -- JSON

    -- Indicators

    -- Dates

    -- Metrics

from 
    {{ source('mailchimp_rollups', 'mailchimp_crosswalk') }}
