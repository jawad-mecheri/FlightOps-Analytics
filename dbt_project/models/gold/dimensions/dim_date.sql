{{ config(materialized='table') }}

with date_range as (
  select 
    DATE('2009-01-01') as min_date,
    DATE('2020-12-12') as max_date
),

dates as (
  select 
    DATE_ADD(min_date, INTERVAL seq DAY) as date_full
  from date_range,
  unnest(GENERATE_ARRAY(0, DATE_DIFF(max_date, min_date, DAY))) as seq
),

years as (
  select distinct EXTRACT(YEAR FROM date_full) as yr 
  from dates
),

us_holidays as (
  -- New Year's Day
  select yr, DATE(yr, 1, 1) as holiday_date, "New Year's Day" as holiday_name from years
  union all
  -- Martin Luther King Jr. Day: third Monday of January
  select yr, 
    DATE_ADD(
      DATE_ADD(DATE(yr, 1, 1), INTERVAL MOD(9 - EXTRACT(DAYOFWEEK FROM DATE(yr, 1, 1)), 7) DAY),
      INTERVAL 14 DAY
    ) as holiday_date, 
    "Martin Luther King Jr. Day" as holiday_name 
  from years
  union all
  -- Presidents' Day: third Monday of February
  select yr, 
    DATE_ADD(
      DATE_ADD(DATE(yr, 2, 1), INTERVAL MOD(9 - EXTRACT(DAYOFWEEK FROM DATE(yr, 2, 1)), 7) DAY),
      INTERVAL 14 DAY
    ) as holiday_date, 
    "Presidents' Day" as holiday_name 
  from years
  union all
  -- Memorial Day: last Monday of May
  select yr, 
    DATE_SUB(
      LAST_DAY(DATE(yr, 5, 1)),
      INTERVAL MOD(EXTRACT(DAYOFWEEK FROM LAST_DAY(DATE(yr, 5, 1))) + 5, 7) DAY
    ) as holiday_date, 
    "Memorial Day" as holiday_name 
  from years
  union all
  -- Independence Day
  select yr, DATE(yr, 7, 4) as holiday_date, "Independence Day" as holiday_name from years
  union all
  -- Labor Day: first Monday of September
  select yr, 
    DATE_ADD(DATE(yr, 9, 1), INTERVAL MOD(9 - EXTRACT(DAYOFWEEK FROM DATE(yr, 9, 1)), 7) DAY) as holiday_date, 
    "Labor Day" as holiday_name 
  from years
  union all
  -- Columbus Day: second Monday of October
  select yr, 
    DATE_ADD(
      DATE_ADD(DATE(yr, 10, 1), INTERVAL MOD(9 - EXTRACT(DAYOFWEEK FROM DATE(yr, 10, 1)), 7) DAY),
      INTERVAL 7 DAY
    ) as holiday_date, 
    "Columbus Day" as holiday_name 
  from years
  union all
  -- Veterans Day
  select yr, DATE(yr, 11, 11) as holiday_date, "Veterans Day" as holiday_name from years
  union all
  -- Thanksgiving Day: fourth Thursday of November
  select yr, 
    DATE_ADD(
      DATE_ADD(DATE(yr, 11, 1), INTERVAL MOD(5 - EXTRACT(DAYOFWEEK FROM DATE(yr, 11, 1)) + 7, 7) DAY),
      INTERVAL 21 DAY
    ) as holiday_date, 
    "Thanksgiving Day" as holiday_name 
  from years
  union all
  -- Christmas Day
  select yr, DATE(yr, 12, 25) as holiday_date, "Christmas Day" as holiday_name from years
)

select
  FORMAT_DATE('%Y%m%d', d.date_full) as date_key,
  d.date_full,
  EXTRACT(YEAR FROM d.date_full) as year,
  EXTRACT(MONTH FROM d.date_full) as month,
  EXTRACT(DAY FROM d.date_full) as day,
  EXTRACT(WEEK FROM d.date_full) as week_of_year,
  EXTRACT(DAYOFWEEK FROM d.date_full) as day_of_week,
  case 
    when h.holiday_date is not null then true 
    else false 
  end as holiday_flag,
  h.holiday_name
from dates d
left join us_holidays h
  on d.date_full = h.holiday_date
