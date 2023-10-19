{% set start_date %}

   cast('{{ var("dim_dates_min_date") }}' as date)

{% endset %}

{% set end_date %}

   cast('{{ var("dim_dates_max_date") }}' as date)

{% endset %}

with 

base_dates as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date=start_date,
        end_date=end_date
    )
    }}

),

dates_with_prior_year_dates as (

    select
        cast(d.date_day as date) as date_day,
        cast(timestampadd(year, -1, d.date_day) as date) as prior_year_date_day,
        cast(timestampadd(day, -364, d.date_day) as date) as prior_year_over_year_date_day
    from
    	base_dates d

    union all

    -- Add placeholder for missing dates
    select
        cast('1900-01-01' as date),
        cast(timestampadd(year, -1, cast('1900-01-01' as date)) as date),
        cast(timestampadd(day, -364, cast('1900-01-01' as date)) as date)

)

select
    try_cast(date_format(d.date_day, 'yMMdd') as integer) as date_key,
    d.date_day,
    d.date_day::timestamp as date_day_timestamp,
    cast(timestampadd(day, -1, d.date_day) as date) as prior_date_day,
    cast(timestampadd(day, 1, d.date_day) as date) as next_date_day,
    d.prior_year_date_day as prior_year_date_day,
    d.prior_year_over_year_date_day,

    extract(DOW from d.date_day) as day_of_week,
    extract(DOW_ISO from d.date_day) as day_of_week_iso,
    date_format(d.date_day, 'EEEE') as day_of_week_name,
    date_format(d.date_day, 'EEE') as day_of_week_name_short,
    extract(DAY from d.date_day) as day_of_month,
    extract(DOY from d.date_day) as day_of_year,

    cast(date_trunc('week', d.date_day) as date) as week_start_date,
    cast(
        timestampadd(day, -1, timestampadd(week, 1, date_trunc('week', d.date_day)))
        as date) as week_end_date,
    cast(date_trunc('week', d.prior_year_over_year_date_day) as date) as prior_year_week_start_date,
    cast(
        timestampadd(day, -1, timestampadd(week, 1, date_trunc('week', d.prior_year_over_year_date_day)))
        as date) as prior_year_week_end_date,
    cast(date_part('week', d.date_day) as integer) as week_of_year,

    --cast(date_trunc('isoweek', d.date_day) as date) as iso_week_start_date,
    --cast(timestampadd(day, 6, cast(date_trunc('isoweek', d.date_day) as date)) as date) as iso_week_end_date,
    --cast(date_trunc('isoweek', d.prior_year_over_year_date_day) as date) as prior_year_iso_week_start_date,
    --cast(timestampadd(day, 6, cast(date_trunc('isoweek', d.prior_year_over_year_date_day) as date)) as date) as prior_year_iso_week_end_date,
    --cast(date_part('isoweek', d.date_day) as integer) as iso_week_of_year,

    cast(date_part('week', d.prior_year_over_year_date_day) as integer) as prior_year_week_of_year,
    --cast(date_part('isoweek', d.prior_year_over_year_date_day) as integer) as prior_year_iso_week_of_year,

    cast(date_part('month', d.date_day) as integer) as month_of_year,
    date_format(d.date_day, 'MMMM')  as month_name,
    date_format(d.date_day, 'MMM')  as month_name_short,

    cast(date_trunc('month', d.date_day) as date) as month_start_date,
    cast(cast(
        timestampadd(day, -1, timestampadd(month, 1, date_trunc('month', d.date_day)))
        as date) as date) as month_end_date,

    cast(date_trunc('month', d.prior_year_date_day) as date) as prior_year_month_start_date,
    cast(cast(
        timestampadd(day, -1, timestampadd(month, 1, date_trunc('month', d.prior_year_date_day)))
        as date) as date) as prior_year_month_end_date,

    cast(date_part('quarter', d.date_day) as integer) as quarter_of_year,
    cast(date_trunc('quarter', d.date_day) as date) as quarter_start_date,
    cast(cast(
        timestampadd(day, -1, timestampadd(quarter, 1, date_trunc('quarter', d.date_day)))
        as date) as date) as quarter_end_date,

    cast(date_part('year', d.date_day) as integer) as year_number,
    cast(date_trunc('year', d.date_day) as date) as year_start_date,
    cast(cast(
        timestampadd(day, -1, timestampadd(year, 1, date_trunc('year', d.date_day)))
        as date) as date) as year_end_date
from
    dates_with_prior_year_dates d
order by 1
