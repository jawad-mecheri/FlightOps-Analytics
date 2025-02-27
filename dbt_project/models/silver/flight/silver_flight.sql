
with final_2009 as (
  select
    2009 as flight_year,
    FL_DATE,
    op_carrier,
    OP_CARRIER_FL_NUM,
    ORIGIN,
    DEST,
    CAST(CRS_DEP_TIME as INT64) as CRS_DEP_TIME,
    CASE 
    WHEN CAST(DEP_TIME AS STRING) = '2400' THEN 0
    ELSE CAST(DEP_TIME AS INT64)
    END AS DEP_TIME,
    CAST(DEP_DELAY as INT64) as DEP_DELAY,
    CAST(TAXI_OUT as INT64) as TAXI_OUT,
    CAST(WHEELS_OFF as INT64) as WHEELS_OFF,
    CAST(WHEELS_ON as INT64) as WHEELS_ON,
    CAST(TAXI_IN as INT64) as TAXI_IN,
    CAST(CRS_ARR_TIME as INT64) as CRS_ARR_TIME,
    CAST(ARR_TIME as INT64) as ARR_TIME,
    CAST(ARR_DELAY as INT64) as ARR_DELAY,
    CAST(CANCELLED as INT64)as CANCELLED,
    CANCELLATION_CODE,
    CAST(DIVERTED as INT64) as DIVERTED,
    CAST(CRS_ELAPSED_TIME as INT64) as CRS_ELAPSED_TIME,
    CAST(ACTUAL_ELAPSED_TIME as INT64) as ACTUAL_ELAPSED_TIME,
    CAST(AIR_TIME as INT64) as AIR_TIME,
    CAST(DISTANCE as INT64) as DISTANCE,
    CAST(CARRIER_DELAY as INT64) as CARRIER_DELAY,
    CAST(WEATHER_DELAY as INT64) as WEATHER_DELAY,
    CAST(NAS_DELAY as INT64) as NAS_DELAY,
    CAST(SECURITY_DELAY as INT64) as SECURITY_DELAY,
    CAST(LATE_AIRCRAFT_DELAY as INT64) as LATE_AIRCRAFT_DELAY
  from {{ ref('stg_flight_2009') }}
),

final_2010 as (
  select
    2010 as flight_year,
    FL_DATE,
    op_carrier,
    OP_CARRIER_FL_NUM,
    ORIGIN,
    DEST,
    CAST(CRS_DEP_TIME as INT64) as CRS_DEP_TIME,
    CASE 
    WHEN CAST(DEP_TIME AS STRING) = '2400' THEN 0
    ELSE CAST(DEP_TIME AS INT64)
    END AS DEP_TIME,
    CAST(DEP_DELAY as INT64) as DEP_DELAY,
    CAST(TAXI_OUT as INT64) as TAXI_OUT,
    CAST(WHEELS_OFF as INT64) as WHEELS_OFF,
    CAST(WHEELS_ON as INT64) as WHEELS_ON,
    CAST(TAXI_IN as INT64) as TAXI_IN,
    CAST(CRS_ARR_TIME as INT64) as CRS_ARR_TIME,
    CAST(ARR_TIME as INT64) as ARR_TIME,
    CAST(ARR_DELAY as INT64) as ARR_DELAY,
    CAST(CANCELLED as INT64)as CANCELLED,
    CANCELLATION_CODE,
    CAST(DIVERTED as INT64) as DIVERTED,
    CAST(CRS_ELAPSED_TIME as INT64) as CRS_ELAPSED_TIME,
    CAST(ACTUAL_ELAPSED_TIME as INT64) as ACTUAL_ELAPSED_TIME,
    CAST(AIR_TIME as INT64) as AIR_TIME,
    CAST(DISTANCE as INT64) as DISTANCE,
    CAST(CARRIER_DELAY as INT64) as CARRIER_DELAY,
    CAST(WEATHER_DELAY as INT64) as WEATHER_DELAY,
    CAST(NAS_DELAY as INT64) as NAS_DELAY,
    CAST(SECURITY_DELAY as INT64) as SECURITY_DELAY,
    CAST(LATE_AIRCRAFT_DELAY as INT64) as LATE_AIRCRAFT_DELAY
  from {{ ref('stg_flight_2010') }}
),

final_2011 as (
  select
    2011 as flight_year,
    FL_DATE,
    op_carrier,
    OP_CARRIER_FL_NUM,
    ORIGIN,
    DEST,
    CAST(CRS_DEP_TIME as INT64) as CRS_DEP_TIME,
    CASE 
    WHEN CAST(DEP_TIME AS STRING) = '2400' THEN 0
    ELSE CAST(DEP_TIME AS INT64)
    END AS DEP_TIME,
    CAST(DEP_DELAY as INT64) as DEP_DELAY,
    CAST(TAXI_OUT as INT64) as TAXI_OUT,
    CAST(WHEELS_OFF as INT64) as WHEELS_OFF,
    CAST(WHEELS_ON as INT64) as WHEELS_ON,
    CAST(TAXI_IN as INT64) as TAXI_IN,
    CAST(CRS_ARR_TIME as INT64) as CRS_ARR_TIME,
    CAST(ARR_TIME as INT64) as ARR_TIME,
    CAST(ARR_DELAY as INT64) as ARR_DELAY,
    CAST(CANCELLED as INT64)as CANCELLED,
    CANCELLATION_CODE,
    CAST(DIVERTED as INT64) as DIVERTED,
    CAST(CRS_ELAPSED_TIME as INT64) as CRS_ELAPSED_TIME,
    CAST(ACTUAL_ELAPSED_TIME as INT64) as ACTUAL_ELAPSED_TIME,
    CAST(AIR_TIME as INT64) as AIR_TIME,
    CAST(DISTANCE as INT64) as DISTANCE,
    CAST(CARRIER_DELAY as INT64) as CARRIER_DELAY,
    CAST(WEATHER_DELAY as INT64) as WEATHER_DELAY,
    CAST(NAS_DELAY as INT64) as NAS_DELAY,
    CAST(SECURITY_DELAY as INT64) as SECURITY_DELAY,
    CAST(LATE_AIRCRAFT_DELAY as INT64) as LATE_AIRCRAFT_DELAY
  from {{ ref('stg_flight_2011') }}
),

final_2012 as (
  select
    2012 as flight_year,
    FL_DATE,
    op_carrier,
    OP_CARRIER_FL_NUM,
    ORIGIN,
    DEST,
    CAST(CRS_DEP_TIME as INT64) as CRS_DEP_TIME,
    CASE 
    WHEN CAST(DEP_TIME AS STRING) = '2400' THEN 0
    ELSE CAST(DEP_TIME AS INT64)
    END AS DEP_TIME,
    CAST(DEP_DELAY as INT64) as DEP_DELAY,
    CAST(TAXI_OUT as INT64) as TAXI_OUT,
    CAST(WHEELS_OFF as INT64) as WHEELS_OFF,
    CAST(WHEELS_ON as INT64) as WHEELS_ON,
    CAST(TAXI_IN as INT64) as TAXI_IN,
    CAST(CRS_ARR_TIME as INT64) as CRS_ARR_TIME,
    CAST(ARR_TIME as INT64) as ARR_TIME,
    CAST(ARR_DELAY as INT64) as ARR_DELAY,
    CAST(CANCELLED as INT64)as CANCELLED,
    CANCELLATION_CODE,
    CAST(DIVERTED as INT64) as DIVERTED,
    CAST(CRS_ELAPSED_TIME as INT64) as CRS_ELAPSED_TIME,
    CAST(ACTUAL_ELAPSED_TIME as INT64) as ACTUAL_ELAPSED_TIME,
    CAST(AIR_TIME as INT64) as AIR_TIME,
    CAST(DISTANCE as INT64) as DISTANCE,
    CAST(CARRIER_DELAY as INT64) as CARRIER_DELAY,
    CAST(WEATHER_DELAY as INT64) as WEATHER_DELAY,
    CAST(NAS_DELAY as INT64) as NAS_DELAY,
    CAST(SECURITY_DELAY as INT64) as SECURITY_DELAY,
    CAST(LATE_AIRCRAFT_DELAY as INT64) as LATE_AIRCRAFT_DELAY
  from {{ ref('stg_flight_2012') }}
),

final_2013 as (
  select
    2013 as flight_year,
    FL_DATE,
    op_carrier,
    OP_CARRIER_FL_NUM,
    ORIGIN,
    DEST,
    CAST(CRS_DEP_TIME as INT64) as CRS_DEP_TIME,
    CASE 
    WHEN CAST(DEP_TIME AS STRING) = '2400' THEN 0
    ELSE CAST(DEP_TIME AS INT64)
    END AS DEP_TIME,
    CAST(DEP_DELAY as INT64) as DEP_DELAY,
    CAST(TAXI_OUT as INT64) as TAXI_OUT,
    CAST(WHEELS_OFF as INT64) as WHEELS_OFF,
    CAST(WHEELS_ON as INT64) as WHEELS_ON,
    CAST(TAXI_IN as INT64) as TAXI_IN,
    CAST(CRS_ARR_TIME as INT64) as CRS_ARR_TIME,
    CAST(ARR_TIME as INT64) as ARR_TIME,
    CAST(ARR_DELAY as INT64) as ARR_DELAY,
    CAST(CANCELLED as INT64)as CANCELLED,
    CANCELLATION_CODE,
    CAST(DIVERTED as INT64) as DIVERTED,
    CAST(CRS_ELAPSED_TIME as INT64) as CRS_ELAPSED_TIME,
    CAST(ACTUAL_ELAPSED_TIME as INT64) as ACTUAL_ELAPSED_TIME,
    CAST(AIR_TIME as INT64) as AIR_TIME,
    CAST(DISTANCE as INT64) as DISTANCE,
    CAST(CARRIER_DELAY as INT64) as CARRIER_DELAY,
    CAST(WEATHER_DELAY as INT64) as WEATHER_DELAY,
    CAST(NAS_DELAY as INT64) as NAS_DELAY,
    CAST(SECURITY_DELAY as INT64) as SECURITY_DELAY,
    CAST(LATE_AIRCRAFT_DELAY as INT64) as LATE_AIRCRAFT_DELAY
  from {{ ref('stg_flight_2013') }}
),

final_2014 as (
  select
    2014 as flight_year,
    FL_DATE,
    op_carrier,
    OP_CARRIER_FL_NUM,
    ORIGIN,
    DEST,
    CAST(CRS_DEP_TIME as INT64) as CRS_DEP_TIME,
    CASE 
    WHEN CAST(DEP_TIME AS STRING) = '2400' THEN 0
    ELSE CAST(DEP_TIME AS INT64)
    END AS DEP_TIME,
    CAST(DEP_DELAY as INT64) as DEP_DELAY,
    CAST(TAXI_OUT as INT64) as TAXI_OUT,
    CAST(WHEELS_OFF as INT64) as WHEELS_OFF,
    CAST(WHEELS_ON as INT64) as WHEELS_ON,
    CAST(TAXI_IN as INT64) as TAXI_IN,
    CAST(CRS_ARR_TIME as INT64) as CRS_ARR_TIME,
    CAST(ARR_TIME as INT64) as ARR_TIME,
    CAST(ARR_DELAY as INT64) as ARR_DELAY,
    CAST(CANCELLED as INT64)as CANCELLED,
    CANCELLATION_CODE,
    CAST(DIVERTED as INT64) as DIVERTED,
    CAST(CRS_ELAPSED_TIME as INT64) as CRS_ELAPSED_TIME,
    CAST(ACTUAL_ELAPSED_TIME as INT64) as ACTUAL_ELAPSED_TIME,
    CAST(AIR_TIME as INT64) as AIR_TIME,
    CAST(DISTANCE as INT64) as DISTANCE,
    CAST(CARRIER_DELAY as INT64) as CARRIER_DELAY,
    CAST(WEATHER_DELAY as INT64) as WEATHER_DELAY,
    CAST(NAS_DELAY as INT64) as NAS_DELAY,
    CAST(SECURITY_DELAY as INT64) as SECURITY_DELAY,
    CAST(LATE_AIRCRAFT_DELAY as INT64) as LATE_AIRCRAFT_DELAY
  from {{ ref('stg_flight_2014') }}
),

final_2015 as (
  select
    2015 as flight_year,
    FL_DATE,
    op_carrier,
    OP_CARRIER_FL_NUM,
    ORIGIN,
    DEST,
    CAST(CRS_DEP_TIME as INT64) as CRS_DEP_TIME,
    CASE 
    WHEN CAST(DEP_TIME AS STRING) = '2400' THEN 0
    ELSE CAST(DEP_TIME AS INT64)
    END AS DEP_TIME,
    CAST(DEP_DELAY as INT64) as DEP_DELAY,
    CAST(TAXI_OUT as INT64) as TAXI_OUT,
    CAST(WHEELS_OFF as INT64) as WHEELS_OFF,
    CAST(WHEELS_ON as INT64) as WHEELS_ON,
    CAST(TAXI_IN as INT64) as TAXI_IN,
    CAST(CRS_ARR_TIME as INT64) as CRS_ARR_TIME,
    CAST(ARR_TIME as INT64) as ARR_TIME,
    CAST(ARR_DELAY as INT64) as ARR_DELAY,
    CAST(CANCELLED as INT64)as CANCELLED,
    CANCELLATION_CODE,
    CAST(DIVERTED as INT64) as DIVERTED,
    CAST(CRS_ELAPSED_TIME as INT64) as CRS_ELAPSED_TIME,
    CAST(ACTUAL_ELAPSED_TIME as INT64) as ACTUAL_ELAPSED_TIME,
    CAST(AIR_TIME as INT64) as AIR_TIME,
    CAST(DISTANCE as INT64) as DISTANCE,
    CAST(CARRIER_DELAY as INT64) as CARRIER_DELAY,
    CAST(WEATHER_DELAY as INT64) as WEATHER_DELAY,
    CAST(NAS_DELAY as INT64) as NAS_DELAY,
    CAST(SECURITY_DELAY as INT64) as SECURITY_DELAY,
    CAST(LATE_AIRCRAFT_DELAY as INT64) as LATE_AIRCRAFT_DELAY
  from {{ ref('stg_flight_2015') }}
),

final_2016 as (
  select
    2016 as flight_year,
    FL_DATE,
    op_carrier,
    OP_CARRIER_FL_NUM,
    ORIGIN,
    DEST,
    CAST(CRS_DEP_TIME as INT64) as CRS_DEP_TIME,
    CASE 
    WHEN CAST(DEP_TIME AS STRING) = '2400' THEN 0
    ELSE CAST(DEP_TIME AS INT64)
    END AS DEP_TIME,
    CAST(DEP_DELAY as INT64) as DEP_DELAY,
    CAST(TAXI_OUT as INT64) as TAXI_OUT,
    CAST(WHEELS_OFF as INT64) as WHEELS_OFF,
    CAST(WHEELS_ON as INT64) as WHEELS_ON,
    CAST(TAXI_IN as INT64) as TAXI_IN,
    CAST(CRS_ARR_TIME as INT64) as CRS_ARR_TIME,
    CAST(ARR_TIME as INT64) as ARR_TIME,
    CAST(ARR_DELAY as INT64) as ARR_DELAY,
    CAST(CANCELLED as INT64)as CANCELLED,
    CANCELLATION_CODE,
    CAST(DIVERTED as INT64) as DIVERTED,
    CAST(CRS_ELAPSED_TIME as INT64) as CRS_ELAPSED_TIME,
    CAST(ACTUAL_ELAPSED_TIME as INT64) as ACTUAL_ELAPSED_TIME,
    CAST(AIR_TIME as INT64) as AIR_TIME,
    CAST(DISTANCE as INT64) as DISTANCE,
    CAST(CARRIER_DELAY as INT64) as CARRIER_DELAY,
    CAST(WEATHER_DELAY as INT64) as WEATHER_DELAY,
    CAST(NAS_DELAY as INT64) as NAS_DELAY,
    CAST(SECURITY_DELAY as INT64) as SECURITY_DELAY,
    CAST(LATE_AIRCRAFT_DELAY as INT64) as LATE_AIRCRAFT_DELAY
  from {{ ref('stg_flight_2016') }}
),

final_2017 as (
  select
    2017 as flight_year,
    FL_DATE,
    op_carrier,
    OP_CARRIER_FL_NUM,
    ORIGIN,
    DEST,
    CAST(CRS_DEP_TIME as INT64) as CRS_DEP_TIME,
    CASE 
    WHEN CAST(DEP_TIME AS STRING) = '2400' THEN 0
    ELSE CAST(DEP_TIME AS INT64)
    END AS DEP_TIME,
    CAST(DEP_DELAY as INT64) as DEP_DELAY,
    CAST(TAXI_OUT as INT64) as TAXI_OUT,
    CAST(WHEELS_OFF as INT64) as WHEELS_OFF,
    CAST(WHEELS_ON as INT64) as WHEELS_ON,
    CAST(TAXI_IN as INT64) as TAXI_IN,
    CAST(CRS_ARR_TIME as INT64) as CRS_ARR_TIME,
    CAST(ARR_TIME as INT64) as ARR_TIME,
    CAST(ARR_DELAY as INT64) as ARR_DELAY,
    CAST(CANCELLED as INT64)as CANCELLED,
    CANCELLATION_CODE,
    CAST(DIVERTED as INT64) as DIVERTED,
    CAST(CRS_ELAPSED_TIME as INT64) as CRS_ELAPSED_TIME,
    CAST(ACTUAL_ELAPSED_TIME as INT64) as ACTUAL_ELAPSED_TIME,
    CAST(AIR_TIME as INT64) as AIR_TIME,
    CAST(DISTANCE as INT64) as DISTANCE,
    CAST(CARRIER_DELAY as INT64) as CARRIER_DELAY,
    CAST(WEATHER_DELAY as INT64) as WEATHER_DELAY,
    CAST(NAS_DELAY as INT64) as NAS_DELAY,
    CAST(SECURITY_DELAY as INT64) as SECURITY_DELAY,
    CAST(LATE_AIRCRAFT_DELAY as INT64) as LATE_AIRCRAFT_DELAY
  from {{ ref('stg_flight_2017') }}
),

final_2018 as (
  select
    2018 as flight_year,
    FL_DATE,
    op_carrier,
    OP_CARRIER_FL_NUM,
    ORIGIN,
    DEST,
    CAST(CRS_DEP_TIME as INT64) as CRS_DEP_TIME,
    CASE 
    WHEN CAST(DEP_TIME AS STRING) = '2400' THEN 0
    ELSE CAST(DEP_TIME AS INT64)
    END AS DEP_TIME,
    CAST(DEP_DELAY as INT64) as DEP_DELAY,
    CAST(TAXI_OUT as INT64) as TAXI_OUT,
    CAST(WHEELS_OFF as INT64) as WHEELS_OFF,
    CAST(WHEELS_ON as INT64) as WHEELS_ON,
    CAST(TAXI_IN as INT64) as TAXI_IN,
    CAST(CRS_ARR_TIME as INT64) as CRS_ARR_TIME,
    CAST(ARR_TIME as INT64) as ARR_TIME,
    CAST(ARR_DELAY as INT64) as ARR_DELAY,
    CAST(CANCELLED as INT64)as CANCELLED,
    CANCELLATION_CODE,
    CAST(DIVERTED as INT64) as DIVERTED,
    CAST(CRS_ELAPSED_TIME as INT64) as CRS_ELAPSED_TIME,
    CAST(ACTUAL_ELAPSED_TIME as INT64) as ACTUAL_ELAPSED_TIME,
    CAST(AIR_TIME as INT64) as AIR_TIME,
    CAST(DISTANCE as INT64) as DISTANCE,
    CAST(CARRIER_DELAY as INT64) as CARRIER_DELAY,
    CAST(WEATHER_DELAY as INT64) as WEATHER_DELAY,
    CAST(NAS_DELAY as INT64) as NAS_DELAY,
    CAST(SECURITY_DELAY as INT64) as SECURITY_DELAY,
    CAST(LATE_AIRCRAFT_DELAY as INT64) as LATE_AIRCRAFT_DELAY
  from {{ ref('stg_flight_2018') }}
),

final_2019 as (
  select
    2019 as flight_year,
    FL_DATE,
    OP_UNIQUE_CARRIER as op_carrier,
    OP_CARRIER_FL_NUM,
    ORIGIN,
    DEST,
    CASE 
    WHEN CAST(DEP_TIME AS STRING) = '2400' THEN 0
    ELSE CAST(DEP_TIME AS INT64)
    END AS DEP_TIME,
    CAST(DEP_DELAY as INT64) as DEP_DELAY,
    -- Calcul de CRS_DEP_TIME en INT64 en combinant heure et minute avec conversion de "2400" en "0000"
    EXTRACT(HOUR FROM TIME_SUB(
      PARSE_TIME("%H%M", CASE WHEN CAST(DEP_TIME AS STRING) = '2400' THEN '0000'
                              ELSE LPAD(CAST(DEP_TIME AS STRING), 4, '0') END),
      INTERVAL CAST(DEP_DELAY AS INT64) MINUTE)) * 100 +
    EXTRACT(MINUTE FROM TIME_SUB(
      PARSE_TIME("%H%M", CASE WHEN CAST(DEP_TIME AS STRING) = '2400' THEN '0000'
                              ELSE LPAD(CAST(DEP_TIME AS STRING), 4, '0') END),
      INTERVAL CAST(DEP_DELAY AS INT64) MINUTE)) as CRS_DEP_TIME,
    CAST(TAXI_OUT as INT64) as TAXI_OUT,
    CAST(WHEELS_OFF as INT64) as WHEELS_OFF,
    CAST(WHEELS_ON as INT64) as WHEELS_ON,
    CAST(TAXI_IN as INT64) as TAXI_IN,
    -- Calcul de CRS_ARR_TIME en INT64 en combinant heure et minute avec conversion de "2400" en "0000"
    EXTRACT(HOUR FROM TIME_SUB(
      PARSE_TIME("%H%M", CASE WHEN CAST(ARR_TIME AS STRING) = '2400' THEN '0000'
                              ELSE LPAD(CAST(ARR_TIME AS STRING), 4, '0') END),
      INTERVAL CAST(ARR_DELAY AS INT64) MINUTE)) * 100 +
    EXTRACT(MINUTE FROM TIME_SUB(
      PARSE_TIME("%H%M", CASE WHEN CAST(ARR_TIME AS STRING) = '2400' THEN '0000'
                              ELSE LPAD(CAST(ARR_TIME AS STRING), 4, '0') END),
      INTERVAL CAST(ARR_DELAY AS INT64) MINUTE)) as CRS_ARR_TIME,
    CAST(ARR_TIME as INT64) as ARR_TIME,
    CAST(ARR_DELAY as INT64) as ARR_DELAY,
    0 as CANCELLED,
    CAST(null as STRING) as CANCELLATION_CODE,
    0 as DIVERTED,
    (CRS_ARR_TIME - CRS_DEP_TIME) as CRS_ELAPSED_TIME,
    null as ACTUAL_ELAPSED_TIME,
    CAST(AIR_TIME as INT64) as AIR_TIME,
    CAST(DISTANCE as INT64) as DISTANCE,
    CAST(CARRIER_DELAY as INT64) as CARRIER_DELAY,
    CAST(WEATHER_DELAY as INT64) as WEATHER_DELAY,
    CAST(NAS_DELAY as INT64) as NAS_DELAY,
    CAST(SECURITY_DELAY as INT64) as SECURITY_DELAY,
    CAST(LATE_AIRCRAFT_DELAY as INT64) as LATE_AIRCRAFT_DELAY
  from {{ ref('stg_flight_2019') }}
),
final as ( select *
from final_2009
union all
select * from final_2010
union all
select * from final_2011
union all
select * from final_2012
union all
select * from final_2013
union all
select * from final_2014
union all
select * from final_2015
union all
select * from final_2016
union all
select * from final_2017
union all
select * from final_2018
union all
select * from final_2019
)

select * from final 
where 
      FL_DATE is not null and
      op_carrier is not null and
      OP_CARRIER_FL_NUM is not null and
      ORIGIN is not null and
      DEST is not null and
      DEP_TIME is not null and
      DEP_DELAY is not null and
      TAXI_OUT is not null and
      WHEELS_OFF is not null and
      WHEELS_ON is not null and
      TAXI_IN is not null and
      ARR_TIME is not null and
      ARR_DELAY is not null and
      AIR_TIME is not null and
      DISTANCE is not null and 
      CRS_DEP_TIME is not null and 
      CRS_ARR_TIME is not null and 
      DEP_TIME >= 0 and DEP_TIME <=2400 and 
      CRS_ELAPSED_TIME > 0
 