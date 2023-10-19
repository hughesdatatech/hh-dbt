select 

    -- Metadata
    -- TO DO: need source metadata, extracted timestamp?

    -- Business Keys
    client_id as customer_id,
    insurer_id,	
    insurance_coverage_id,
    user_id	as member_id,

    -- Misc Attributes
    client as client_name,	
    insurer as insurer_name,
    subscriber_id,
    group_number,

    first_name,	
    last_name,
    sex,
    address,
    city,
    state,
    zip as postal_code,	
    relationship,

    dep_first_name as dependent_first_name,
    dep_last_name as dependent_last_name,
    dep_sex as dependent_sex,
    dep_address as dependent_address,
    dep_city as dependent_city,
    dep_state as dependent_state,
    dep_zip as dependent_postal_code,

    bin_number,
    carrier_id,
    person_code,	
    source as record_source_code,	
    rx_pcn,
    rx_grp,

    membership as membership_code,
    person_agn,	
    person_number,	
    invitation_code,
    contract as contract_code,

    -- JSON
    efile_record as efile_record_struct,	

    -- Indicators
    eligible as is_eligible,
    current	as is_current,
    eligible_changed as is_eligible_changed,	

    -- Dates
    created_at,
    dob as date_of_birth_at,
    dep_dob as dependent_date_of_birth_at

    -- Metrics
        
from 
   {{ source('xsrc_rollups', 'insurance_checks') }}
