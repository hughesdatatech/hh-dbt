{% set eligible_lives_threshold = 100 %}

with

marketing_touchpoints as (

    select
        *
    from 
        {{ ref("fct_marketing_touchpoints") }} as mt -- TO DO: be aware of changing data if implemented 
    where true
        and customer_id is not null -- TO DO: need tests on this
        -- Note: the following filters will include non-scheduled touchpoints
        and deployment_id is not null
        and touchpoint_name is not null
        and touchpoint_sent_at is not null

),

accounts as (

    select
        account_key,
        customer_other_vendor_sends_emails,
        eligible_members
    from 
        {{ ref("dim_accounts") }}
    where true

),

opportunities as (

    select 
        account_id,
        max(opportunity_covered_lives) as account_max_opportunity_covered_lives
    from
        {{ ref("stg_sfdc_rollups__opportunities") }}
    group by
        1
    having 
        max(opportunity_covered_lives) > 0

),

/*
    Get all Customers and flag any "real" ones without any efile records, or those that do not meet eligible_lives threshold.
*/
customers as (

    select
        customer_id,
        scheduled_send_email_address_count,
        scheduled_send_non_gmail_address_count,
        scheduled_send_non_gmail_address_pct,
        scheduled_send_source_system,
        case
            when not is_vip_in_identifier
                    and not is_test_customer
                    and (is_missing_efile or efile_eligible_lives <= {{ eligible_lives_threshold }})
                then true
            else false
        end as is_missing_efile_below_threshold
    from 
        {{ ref("dim_customers") }}
    where true 

),

touchpoints_consolidated as (

    select 

        -- Dimension Keys
        mt.touchpoint_sent_at_key, 
        mt.account_key,
        mt.customer_key,

        -- Related Fact Keys
        mt.deployment_key,
        mt.opportunity_key,
        
        -- Business Keys
        mt.touchpoint_id,
        mt.account_id,
        mt.customer_id,
        mt.deployment_id,
        mt.opportunity_id,
        
        -- Misc Attributes
        mt.touchpoint_name, 
        mt.touchpoint_number, 
        mt.touchpoint_medium, 
        mt.touchpoint_marketing_activity_status, 
        coalesce(mt.touchpoint_sender_staged, act.customer_other_vendor_sends_emails, 'Hinge Health') as touchpoint_sender, -- TO DO: check, moved 'Hinge Health' to here from scheduled_send_types?
        lower(touchpoint_sender) as lower_touchpoint_sender,
        mt.first_or_repeat_deployment,
        mt.content_type,

        -- Indicators
        mt.is_email_communication,
        mt.is_mailer_communication,
        mt.is_customer_communication,
        mt.is_womens_pelvic_health,
        case
            when lower_touchpoint_sender = 'hinge health'
                then true
            else false
        end is_touchpoint_sender_hh,

        -- Dates
        mt.touchpoint_sent_at,
        
        -- Metrics	
        nvl(op.account_max_opportunity_covered_lives, 0) as account_max_opportunity_covered_lives,
        mt.touchpoint_partial_population,
        act.eligible_members,

        {# replaces demand_forecast/SQL/scheduled_sends/ss2-scheduled_send_prep_tables.sql >>> scheduled_send_types #}
        mt.was_touchpoint_scheduled,
        case
            when mt.was_touchpoint_scheduled 
                    and cust.scheduled_send_source_system = 'mailchimp'
                    and mt.is_email_communication
                    and is_touchpoint_sender_hh
                    and (mt.first_or_repeat_deployment is distinct from 1 or mt.touchpoint_number is distinct from 1) -- only allowed on touchpoint 2+ (assume not available for first ever touchpoint)
                then 'email_with_mailchimp_list'
            when mt.was_touchpoint_scheduled
                    and cust.scheduled_send_source_system = 'iterable'
                    and mt.is_email_communication
                    and is_touchpoint_sender_hh
                    and (mt.first_or_repeat_deployment is distinct from 1 or mt.touchpoint_number is distinct from 1) -- only allowed on touchpoint 2+ (assume not available for first ever touchpoint)
                then 'email_with_iterable_list'
            when mt.was_touchpoint_scheduled
                    and (cust.is_missing_efile_below_threshold or (mt.first_or_repeat_deployment = 1 and coalesce(mt.touchpoint_number, 1) = 1))
                then 'noefile'
            when mt.was_touchpoint_scheduled
                    and cust.scheduled_send_source_system is null
                    and mt.is_email_communication
                    and is_touchpoint_sender_hh
                    and (mt.first_or_repeat_deployment is distinct from 1 or mt.touchpoint_number is distinct from 1) -- only allowed on touchpoint 2+ (assume not available for first ever touchpoint)
                then 'email_without_mailchimp_list'
            when mt.was_touchpoint_scheduled
                    and not mt.is_email_communication
                    and is_touchpoint_sender_hh
                    and (mt.first_or_repeat_deployment is distinct from 1 or mt.touchpoint_number is distinct from 1) -- only allowed on touchpoint 2+ (assume not available for first ever touchpoint)
                then 'non_email_hh_send_w_efile'
            when mt.was_touchpoint_scheduled
                    and lower_touchpoint_sender in ('client', 'other vendor')
                then 'other_sender'
        end as scheduled_send_type, -- formerly called segment
        mt.was_touchpoint_sent_during_q2_2022,
        cust.scheduled_send_email_address_count,
        cust.scheduled_send_non_gmail_address_count,
        cust.scheduled_send_non_gmail_address_pct,
        cust.scheduled_send_source_system

    from 
        marketing_touchpoints as mt
        inner join accounts as act
            on mt.account_key = act.account_key
        left join opportunities as op
            on mt.account_id = op.account_id
        left join customers as cust 
            on mt.customer_id = cust.customer_id
        where true 

),

{# scheduled sends calculation #}
{# replaces demand_forecast/SQL/scheduled_sends/ss3-calculate_scheduled_sends.sql >>> output scheduled sends to marketing_activities #}
{%- set common_select_fields = 'touchpoint_id, customer_id, is_womens_pelvic_health, account_id, deployment_id, touchpoint_sent_at_key, touchpoint_sent_at' %}

scheduled_touchpoints as (

    select
        *
    from 
       touchpoints_consolidated
    where true
        and was_touchpoint_scheduled
           
),

/*
    This cte is only required due to the gmail deliverability issue associated with Q2 2022 where we cannot reach members using gmail. 
    Normally we should only consider the email fill rate instead of the full case statement based on touchpoint send date below.
*/
agg_bob_fill_rates as (
    
    select 
        sum(member_relationship_pct * non_gmail_members_pct) as q2_rate,
        sum(member_relationship_pct * members_w_email_pct) as email_rate
    from
        {{ ref("int_bob_fill_rates") }}

),

/*
    Begin: email_with_mailchimp_list, email_with_iterable_list
*/

email_with_both_lists as (

    select
        {{ common_select_fields }},
        case 
            when touchpoint_partial_population is not null 
                    and touchpoint_partial_population > 0
                then 1.0 * touchpoint_partial_population
            when customer_id = 122 -- abbott has incorrect efile relationships, only client who is impacted
                then eligible_members * 
                        case 
                            when was_touchpoint_sent_during_q2_2022 
                                then (select q2_rate from agg_bob_fill_rates) 
                            else (select email_rate from agg_bob_fill_rates) 
                        end
            when eligible_members > 0 
                    and eligible_members < (0.25 * scheduled_send_email_address_count) -- account for pilot clients as formula eligible users in SFDC is more accurate here
                then eligible_members * 
                        case 
                            when was_touchpoint_sent_during_q2_2022 
                                then (select q2_rate from agg_bob_fill_rates) 
                            else (select email_rate from agg_bob_fill_rates) 
                        end
            when (eligible_members * .25) > -- account for clients moving out of their pilot phase
                    case 
                        when was_touchpoint_sent_during_q2_2022 
                            then scheduled_send_non_gmail_address_count 
                        else scheduled_send_email_address_count 
                    end 
                then eligible_members * 
                        case 
                            when was_touchpoint_sent_during_q2_2022 
                                then (select q2_rate from agg_bob_fill_rates) 
                            else (select email_rate from agg_bob_fill_rates) 
                        end
            /*
                Take the smaller of the bob fill rate and the observed number of lives.
                Least statement is needed since we do not purge old members, so clients with high turnover have MC lists with 2 - 3 x their actual volume of employees.
            */
            else least(
                    eligible_members * 
                        case 
                            when was_touchpoint_sent_during_q2_2022 
                                then (select q2_rate from agg_bob_fill_rates) 
                            else (select email_rate from agg_bob_fill_rates) 
                        end,
                    case 
                        when was_touchpoint_sent_during_q2_2022 
                            then scheduled_send_non_gmail_address_count
                        else scheduled_send_email_address_count
                    end
                )
        end as approximate_scheduled_send_count, -- formerly known as weight in order to put all activity_type records into a single table
        to_json(
            named_struct(
                'scheduled_send_type', scheduled_send_type,
                'forecast_run_date', date_trunc('hour', {{ get_run_started_timestamp_at() }}),
                'touchpoint_data_date', {{ get_run_started_timestamp_at() }},
                '2022q2_gmail_deliverability_haircut_applied', was_touchpoint_sent_during_q2_2022,
                'bob_average_estimate', eligible_members * case when was_touchpoint_sent_during_q2_2022 then (select q2_rate from agg_bob_fill_rates) else (select email_rate from agg_bob_fill_rates) end,
                'total_email_count', scheduled_send_email_address_count,
                'non_gmail_count', scheduled_send_non_gmail_address_count
            )
        ) as detail_json
    from
        scheduled_touchpoints
    where true 
        and scheduled_send_type in ('email_with_mailchimp_list', 'email_with_iterable_list')

),

/*
    Begin: email_without_mailchimp_list
*/

/*
    Separate cte to speed up query in next step, instead of using partition clause and distinct.
*/
email_without_mailchimp_list_agg as (

    select
        touchpoint_id as agg_touchpoint_id,
        count(1) as record_count,
        sum(
            case 
                when was_touchpoint_sent_during_q2_2022 
                    then scheduled_send_non_gmail_address_pct 
                else bfr.members_w_email_pct 
            end
        ) as email_pct
    from 
        scheduled_touchpoints as mt
        inner join {{ ref("stg_efile_rollups__eligible_lives") }} as el
            on el.customer_id = mt.customer_id 
            and (el.is_active or el._updated_at > mt.touchpoint_sent_at::timestamp) -- ensure record was still active at send
            and coalesce(el._created_at, '2000-01-01'::timestamp) <= mt.touchpoint_sent_at::timestamp -- ensure record was created before send
            and el.is_18_plus --ensure user is 18+
        left join {{ ref("int_bob_fill_rates") }} as bfr
            on bfr.relationship = el.relationship
    where true 
        and scheduled_send_type in ('email_without_mailchimp_list')
    group by 1

),

email_without_mailchimp_list as (

    select
        {{ common_select_fields }},
        round(
            case 
                when touchpoint_partial_population is not null 
                        and touchpoint_partial_population > 0
                    then 1.0 * touchpoint_partial_population
                when mt.customer_id = 122 -- abbott has incorrect efile relationships, only client who is impacted
                    then eligible_members * 
                            case 
                                when was_touchpoint_sent_during_q2_2022 
                                    then (select q2_rate from agg_bob_fill_rates) 
                                else (select email_rate from agg_bob_fill_rates) 
                            end
                when eligible_members > 0 
                        and eligible_members < (.25 * agg.record_count) -- account for pilot clients as formula eligible users in SFDC is more accurate here
                    then eligible_members * 
                            case 
                                when was_touchpoint_sent_during_q2_2022 
                                    then (select q2_rate from agg_bob_fill_rates) 
                                else (select email_rate from agg_bob_fill_rates) 
                            end 
                when (eligible_members * .25) > agg.record_count -- account for clients moving out of their pilot phase
                    then eligible_members * 
                            case 
                                when was_touchpoint_sent_during_q2_2022 
                                    then (select q2_rate from agg_bob_fill_rates) 
                                else (select email_rate from agg_bob_fill_rates) 
                            end
                else least(
                        eligible_members * 
                            case 
                                when was_touchpoint_sent_during_q2_2022 
                                    then (select q2_rate from agg_bob_fill_rates) 
                                else (select email_rate from agg_bob_fill_rates) 
                            end,
                        agg.email_pct
                    ) 
            end
        ) as approximate_scheduled_send_count,
        to_json(
            named_struct(
                'scheduled_send_type', scheduled_send_type,
                'forecast_run_date', date_trunc('hour', {{ get_run_started_timestamp_at() }}),
                'touchpoint_data_date', {{ get_run_started_timestamp_at() }},
                '2022q2_gmail_deliverability_haircut_applied', was_touchpoint_sent_during_q2_2022,
                'bob_average_estimate', eligible_members * case when was_touchpoint_sent_during_q2_2022 then (select q2_rate from agg_bob_fill_rates) else (select email_rate from agg_bob_fill_rates) end
            )
        ) as detail_json
    from
        scheduled_touchpoints as mt
        inner join email_without_mailchimp_list_agg as agg on mt.touchpoint_id = agg.agg_touchpoint_id

),

/*
    Begin: non_email_hh_send_w_efile
*/

ee_bob_fill_rates as (

    select 
        max(member_relationship_pct) as max_efile_pct,
        avg(member_relationship_pct) as avg_efile_pct,
        sum(member_relationship_pct * members_w_email_pct) as email_rate
    from 
        {{ ref("int_bob_fill_rates") }}
    where true 
        and relationship = 'EE'
),

/*
    Separate cte to speed up query in next step, instead of using partition clause and distinct.
*/
non_email_hh_send_w_efile_agg as (

    select
        touchpoint_id as agg_touchpoint_id,
        count(1) as record_count
    from 
        scheduled_touchpoints as mt
        inner join {{ ref("stg_efile_rollups__eligible_lives") }} as el
            on el.customer_id = mt.customer_id 
            and (el.is_active or el._updated_at > mt.touchpoint_sent_at::timestamp) -- ensure record was still active at send
            and coalesce(el._created_at, '2000-01-01'::timestamp) <= mt.touchpoint_sent_at::timestamp -- ensure record was created before send
            and el.is_18_plus -- ensure user is 18+
            and el.relationship = 'EE'
    where true 
        and scheduled_send_type in ('non_email_hh_send_w_efile')
    group by 1

),

non_email_hh_send_w_efile as (

    select
        {{ common_select_fields }},
        case 
            when touchpoint_partial_population is not null 
                    and touchpoint_partial_population > 0
                then 1.0 * touchpoint_partial_population
            when mt.customer_id = 122 -- abbott has incorrect efile relationships, only client who is impacted
                then eligible_members * 
                    (select max_efile_pct from ee_bob_fill_rates)
            when eligible_members > 0 
                    and eligible_members <  (.25 * agg.record_count)
                then eligible_members * 
                    (select max_efile_pct from ee_bob_fill_rates) -- account for pilot clients as formula eligible users in SFDC is more accurate here
            when (eligible_members * .25) > agg.record_count
                then eligible_members * 
                    (select max_efile_pct from ee_bob_fill_rates) -- account for clients moving out of their pilot phase
            else 
                least(
                    eligible_members * 
                        (select max_efile_pct from ee_bob_fill_rates), 
                    agg.record_count
                ) 
        end as approximate_scheduled_send_count,
        to_json(
            named_struct(
                'scheduled_send_type', scheduled_send_type,
                'forecast_run_date', date_trunc('hour', {{ get_run_started_timestamp_at() }}),
                'touchpoint_data_date', {{ get_run_started_timestamp_at() }}
            )
        ) as detail_json
    from
        scheduled_touchpoints as mt
        inner join non_email_hh_send_w_efile_agg as agg on mt.touchpoint_id = agg.agg_touchpoint_id

),

/*
    Begin: other_sender
*/

/*
    Separate cte to speed up query in next step, instead of using partition clause and distinct.
*/
other_sender_agg as (

    select
        touchpoint_id as agg_touchpoint_id,
        count(1) as record_count,
        count(
            case 
                when el.relationship = 'EE' 
                    then el.marketing_id 
            end
        ) as ee_marketing_id_count,
        sum(bfr.members_w_email_pct) as email_pct
    from 
        scheduled_touchpoints as mt
        inner join {{ ref("stg_efile_rollups__eligible_lives") }} as el
            on el.customer_id = mt.customer_id 
            and (el.is_active or el._updated_at > mt.touchpoint_sent_at::timestamp) -- ensure record was still active at send
            and coalesce(el._created_at, '2000-01-01'::timestamp) <= mt.touchpoint_sent_at::timestamp -- ensure record was created before send
            and el.is_18_plus --ensure user is 18+
        left join {{ ref("int_bob_fill_rates") }} as bfr
            on bfr.relationship = el.relationship
            and bfr.relationship = 'EE'
    where true 
        and scheduled_send_type in ('other_sender')
    group by 1

),

other_sender as (

    select
        {{ common_select_fields }},
        case 
            when touchpoint_partial_population is not null 
                    and touchpoint_partial_population > 0
                then 1.0 * touchpoint_partial_population
            when mt.customer_id = 122 -- abbott has incorrect efile relationships, only client who is impacted
                then eligible_members * 
                    (select max_efile_pct from ee_bob_fill_rates)
            when eligible_members > 0 
                    and eligible_members < (.25 * agg.record_count)
                then eligible_members * 
                    (select max_efile_pct from ee_bob_fill_rates) -- account for pilot clients as formula eligible users in SFDC is more accurate here
            when (eligible_members * .25) > agg.record_count
                then eligible_members * 
                    (select max_efile_pct from ee_bob_fill_rates) -- account for clients moving out of their pilot phase
            when is_email_communication
                then least(
                        eligible_members * 
                            (select email_rate from ee_bob_fill_rates), 
                        agg.email_pct
                    )
            else least(
                    eligible_members * 
                        (select max_efile_pct from ee_bob_fill_rates), 
                    agg.ee_marketing_id_count
                ) 
        end as approximate_scheduled_send_count,
        to_json(
            named_struct(
                'scheduled_send_type', scheduled_send_type,
                'forecast_run_date', date_trunc('hour', {{ get_run_started_timestamp_at() }}),
                'touchpoint_data_date', {{ get_run_started_timestamp_at() }}
            )
        ) as detail_json
    from
        scheduled_touchpoints as mt
        inner join other_sender_agg as agg on mt.touchpoint_id = agg.agg_touchpoint_id

),

/*
    Combine all marketing_touchpoint / scheduled send records identified in the ctes above. 
*/
combined_sends as (

    select *
    from email_with_both_lists
    where true

    union all

    select *
    from email_without_mailchimp_list
    where true

    union all

    select *
    from non_email_hh_send_w_efile
    where true
    
    union all

    select *
    from other_sender
    where true

),

/*
    Identify any marketing_touchpoint records not captured in the combined cte above.
*/
missing_touchpoints as (

    select
        mt.*
    from
        scheduled_touchpoints as mt 
        left join combined_sends as cs 
            on mt.touchpoint_id = cs.touchpoint_id
    where true
        and cs.touchpoint_id is null

),

missing_or_noefile as (

    select
        {{ common_select_fields }},
        round(
            case
                when touchpoint_partial_population is not null 
                        and touchpoint_partial_population > 0
                    then 1.0 * touchpoint_partial_population
                when coalesce(account_max_opportunity_covered_lives, 0) > 0
                        and is_email_communication
                    then least(
                            eligible_members * 
                                case 
                                    when was_touchpoint_sent_during_q2_2022 
                                        then (select q2_rate from agg_bob_fill_rates) 
                                    else (select email_rate from agg_bob_fill_rates) 
                                end,
                            account_max_opportunity_covered_lives *
                                case 
                                    when was_touchpoint_sent_during_q2_2022 
                                        then (select q2_rate from agg_bob_fill_rates) 
                                    else (select email_rate from agg_bob_fill_rates) 
                                end * 
                                least(
                                    (select 1.0 * count(case when is_18_plus then 1 end) / count(1) from {{ ref("stg_efile_rollups__eligible_lives") }} where is_active), -- current % of bob efile that is 18+
                                    .75
                                ) 
                            )
                when coalesce(account_max_opportunity_covered_lives, 0) > 0
                    then least(
                            eligible_members * 
                                (select avg_efile_pct from ee_bob_fill_rates),
                            account_max_opportunity_covered_lives * 
                                (select avg_efile_pct from ee_bob_fill_rates) * -- current employee percentage for all non-email mediums
                                least(
                                    (select 1.0 * count(case when is_18_plus then 1 end) / count(1) from {{ ref("stg_efile_rollups__eligible_lives") }} where is_active), -- current % of bob efile that is 18+
                                    .75
                                )
                            )
                else .75 * eligible_members
            end
        ) as approximate_scheduled_send_count,
        to_json(
            named_struct(
                'scheduled_send_type', nvl(scheduled_send_type, 'null'),
                'forecast_run_date', date_trunc('hour', {{ get_run_started_timestamp_at() }}),
                'touchpoint_data_date', {{ get_run_started_timestamp_at() }}
            )
        ) as detail_json
    from 
        missing_touchpoints
    where true

),

efile_female_pcts as ( -- TO DO: check to see if this has been created in a new model which can be reused.

    select distinct
        customer_id,
        1.0 * sum(female_count) over (partition by customer_id) /
            sum(female_count + male_count) over (partition by customer_id) as client_female_pct,
         1.0 * sum(female_count) over () /
            sum(female_count + male_count) over () as total_female_pct
    from
        {{ ref("stg_efile_efile__customers_summary") }}
    where true
        and is_current_record
   
),

combined_sends_with_missing as (

    select *
    from combined_sends
    where true
    
    union all

    select *
    from missing_or_noefile
    where true

),

final_scheduled as (

    select
        cs.touchpoint_id,
        cs.detail_json as scheduled_send_detail_json,
        case 
            when cs.is_womens_pelvic_health 
                then case 
                    when ef.client_female_pct is not distinct from null 
                        then (select max(total_female_pct) from efile_female_pcts) *
                            approximate_scheduled_send_count 
                    else ef.client_female_pct * cs.approximate_scheduled_send_count 
                end
            else cs.approximate_scheduled_send_count 
        end as approximate_scheduled_send_count
    from
        combined_sends_with_missing as cs
        left join efile_female_pcts as ef 
            on cs.customer_id = ef.customer_id

),

final as (

    select
        tc.*,
        fs.scheduled_send_detail_json,
        fs.approximate_scheduled_send_count
    from 
        touchpoints_consolidated as tc
        left join final_scheduled as fs 
            on tc.touchpoint_id = fs.touchpoint_id

)

select *
from final
where true
