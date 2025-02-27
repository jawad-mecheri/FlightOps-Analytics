{{ config(materialized='table') }}

with minutes as (
  select minute_value
  from unnest(GENERATE_ARRAY(0, 1439)) as minute_value
)

select
  minute_value as time_key,
  LPAD(CAST(FLOOR(minute_value / 60) AS STRING), 2, '0') || ':' ||
  LPAD(CAST(MOD(minute_value, 60) AS STRING), 2, '0') as time_value,
  CAST(FLOOR(minute_value / 60) as INT64) as hour,
  MOD(minute_value, 60) as minute,
  case 
    when FLOOR(minute_value / 60) between 5 and 11 then 'Matin'
    when FLOOR(minute_value / 60) between 12 and 16 then 'Après-midi'
    when FLOOR(minute_value / 60) between 17 and 20 then 'Soir'
    else 'Nuit'
  end as period_of_day
from minutes
