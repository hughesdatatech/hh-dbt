{{ config(
    enabled=false
) }}

/* 
    Not currently implemented.
    Was not performing consistently, due to "not in" clause in view against users table?
*/
select 
    *
from 
    {{ source('mailchimp_rollups', 'mailchimp_members_view') }}
