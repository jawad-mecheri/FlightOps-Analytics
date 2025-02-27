
with base as (
  select
    date_key, 
    origin_airport_code,
    destination_airport_code,
    carrier_code,
    flight_id,
    DEP_DELAY,
    ARR_DELAY,
    CARRIER_DELAY,
    WEATHER_DELAY,
    NAS_DELAY,
    SECURITY_DELAY,
    LATE_AIRCRAFT_DELAY
  from {{ ref('fact_flight_operations') }}
  where date_key is not null
),

agg as (
  select
    date_key as agg_date_key,
    origin_airport_code as origin_airport_key,
    destination_airport_code as destination_airport_key,
    carrier_code as carrier_key,
    count(flight_id) as flight_count,
    avg(DEP_DELAY) as avg_dep_delay,
    avg(ARR_DELAY) as avg_arr_delay,
    -- exclure les arrivées en avance
    sum(case when ARR_DELAY > 0 then ARR_DELAY else 0 end) as total_delay_minutes,
    avg(CARRIER_DELAY) as avg_carrier_delay,
    avg(WEATHER_DELAY) as avg_weather_delay,
    avg(NAS_DELAY) as avg_nas_delay,
    avg(SECURITY_DELAY) as avg_security_delay,
    avg(LATE_AIRCRAFT_DELAY) as avg_late_aircraft_delay
  from base
  group by
    date_key,
    origin_airport_code,
    destination_airport_code,
    carrier_code
)

select * from agg
