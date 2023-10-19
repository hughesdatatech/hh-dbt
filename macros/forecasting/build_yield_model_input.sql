{%- macro build_yield_model_input(all_touchpoints=true, deployment_identifier=null, max_conversion_rate=null) -%}

{%- set scheduled_send_min_threshold = 100 -%}
{%- set touchpoint_max_sent_at = "'2023-06-01'" -%}

with 

/*
    Get all touchpoints with at least 101 scheduled sends.
*/
touchpoints as (

    select
        mt.touchpoint_id,
        mt.touchpoint_sent_at,
        mt.customer_id,
        mt.touchpoint_medium,
        mt.approximate_scheduled_send_count
    from 
        {{ ref("fct_marketing_touchpoints_consolidated") }} as mt
    where true
        and mt.approximate_scheduled_send_count > {{ scheduled_send_min_threshold }}
        and (
            mt.is_mailer_communication
            or mt.is_email_communication
            or mt.is_customer_communication
        )
    {%- if not all_touchpoints %}
        and touchpoint_sent_at between '{{ var("yield_training_start_touchpoint_sent_at") }}' and {{ touchpoint_max_sent_at }}
    {%- endif %}
           
),

/*
    Get all observed conversions that are not y2_signups.
*/
conversions as (

    select
        oc.touchpoint_id,
        sum(oc.conversion_count)::int as conversion_count
    from 
        {{ ref("fct_observed_conversions") }} as oc
        left join {{ ref("fct_screenings") }} as sc 
            on oc.screening_key = sc.screening_key
        left join {{ ref("fct_pathways") }} as pt 
            on oc.pathway_key = pt.pathway_key
    where true
        and oc.attribution_type is distinct from 'y2_signups'
        and (sc.was_application_accepted or sc.screening_id is not null)
        and pt.is_chronic_program
    group by 1

),

conv_rates as (

    select
        mt.touchpoint_id,
        mt.touchpoint_sent_at,
        mt.customer_id,
        mt.touchpoint_medium,
        mt.approximate_scheduled_send_count,
        nvl(oc.conversion_count, 0) as conversion_count,
        1.0 * nvl(oc.conversion_count, 0) / mt.approximate_scheduled_send_count as base_conversion_rate,
        case
            when base_conversion_rate > 0.035 
                then 0.004
            else base_conversion_rate
        end as conversion_rate
    from touchpoints as mt
    left join conversions as oc 
        on mt.touchpoint_id = oc.touchpoint_id 

),

final as (

    select 
        
        -- Related IDs
        mt.touchpoint_id,
        mt.customer_id,

        -- Misc Attributes (not used as model input)
        cust.customer_name,
        mt.touchpoint_medium,
        lower(mt.content_type) as lower_content_type,
        dep.annual_counter,
        mt.first_or_repeat_deployment,
        mt.first_or_repeat_deployment - 1 as repeat_deployment,
        mt.touchpoint_sent_at,

        -- Anything below here that is not a Metric will be pivoted so that it can be used as input to the model
        case
            when mt.is_customer_communication 
                then '1'
            when mt.touchpoint_number is null or mt.touchpoint_number < 1 
                then '3+'
            when mt.touchpoint_number < 3 
                then mt.touchpoint_number::varchar(2)
            when mt.touchpoint_number >= 3 
                then '3+'
            else '3+'
        end as growth_touchpoint_number,
        case 
            when mt.is_customer_communication 
                then '1'
            when lower_content_type = 'seasonal' 
                    or mt.touchpoint_number = 0 
                    or lower_content_type = 'womens pelvic health' 
                    or lower_content_type = 'supplemental marketing' 
                then 'Seasonal'
            when mt.touchpoint_number >= 3 
                then '3+'
            when mt.touchpoint_number = 1 
                then '1'
            when mt.touchpoint_number = 2 
                then '2'
            else '3+'
        end as touchpoint_number,
        nvl(act.customer_industry, 'Unknown') as customer_industry,
        nvl(act.population_type, 'Unknown') as population_type,
        case 
            when act.customer_membership_size in ('100,000+', 'Greater than 50,000')
                then '50,000+'
            when act.customer_membership_size in ('5,001-25,000', '10,000-49,000')
                then '10,000-49,999'
            when act.customer_membership_size is null
                then 'Unknown'
            else act.customer_membership_size
        end as customer_membership_size,
        nvl(act.customer_tier, 'Tier 4') as customer_tier,
       
        -- Dates
        'Q' || extract(quarter from mt.touchpoint_sent_at)::varchar(1) as quarter_of_send_date,
        extract(month from mt.touchpoint_sent_at)::varchar(2) as month_of_send_date,

        -- Indicators
        mt.is_email_communication::integer as is_email_communication,
        mt.is_customer_communication::integer as is_customer_communication,
        mt.is_touchpoint_sender_hh::integer as is_touchpoint_sender_hh,
        nvl(cust.is_fully_insured, false)::integer as is_customer_fully_insured, -- TO DO: this can be null when the customer_id is null. Should those cutomers be excluded?
        case 
            when mt.touchpoint_sent_at >= '2023-01-01' 
                then 1 
            else 0
        end::integer as is_new_customer_incentive,

        -- Metrics
        t.conversion_count,
        mt.approximate_scheduled_send_count,
        act.total_covered_lives,
        nvl(efile.pct_employee, bob.pct_employee) as pct_employee,
        nvl(efile.pct_spouse, bob.pct_spouse) as pct_spouse,
        nvl(efile.pct_female, bob.pct_female) as pct_female,
        round(datediff(day, coalesce(act.first_closed_at, act.initial_deployment_launched_at), mt.touchpoint_sent_at) / 30) as age_of_touchpoint,
        t.conversion_rate,
        first_value(t.conversion_rate) over(partition by mt.customer_id, mt.touchpoint_medium order by mt.touchpoint_sent_at asc) as first_conversion_rate,
        coalesce(
            lag(t.conversion_rate) over(partition by mt.customer_id, mt.touchpoint_medium order by mt.touchpoint_sent_at asc),
            case 
                when row_number() over (partition by mt.customer_id, mt.touchpoint_medium order by mt.touchpoint_sent_at asc) > 1 
                    then first_value(t.conversion_rate) over(partition by mt.customer_id, mt.touchpoint_medium order by t.conversion_rate is null, mt.touchpoint_sent_at desc) 
                else t.conversion_rate 
            end) 
        as last_conversion_rate

    from 
        {{ ref("fct_marketing_touchpoints_consolidated") }} as mt
        inner join {{ ref("dim_accounts") }} as act 
            on mt.account_key = act.account_key
        inner join {{ ref("dim_customers") }} as cust 
            on mt.customer_key = cust.customer_key
        inner join {{ ref("fct_deployments") }} as dep 
            on mt.deployment_key = dep.deployment_key
    {%- if not all_touchpoints %}
        inner join conv_rates as t 
            on mt.touchpoint_id = t.touchpoint_id
    {%- else %}
        left join conv_rates as t 
            on mt.touchpoint_id = t.touchpoint_id
            and mt.touchpoint_sent_at <= {{ touchpoint_max_sent_at }}
    {%- endif %}
        left join {{ ref("int_bob_averages_by_customer") }} as efile
            on mt.customer_id = efile.customer_id
        left join {{ ref("int_bob_averages_total")}} as bob 
            on 1 = 1
    where true 
        and (
            mt.is_mailer_communication
            or mt.is_email_communication
            or mt.is_customer_communication
        )
        and mt.was_touchpoint_scheduled
    {%- if not all_touchpoints %}
        and mt.first_or_repeat_deployment = {{ deployment_identifier }}
        and t.conversion_rate < {{ max_conversion_rate }}
    {%- endif %}

)

select *
from final
where true

{%- endmacro -%}
