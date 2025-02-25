{{ config(materialized='table') }}

with source_data as (
    select *
    from {{ ref('silver_flight') }}
)

select
  GENERATE_UUID() as flight_id,
  FL_DATE,
  flight_year,
  op_carrier,
  OP_CARRIER_FL_NUM,
  ORIGIN,
  DEST,

  -- DEP_TIME
  CASE 
    WHEN CAST(DEP_TIME AS STRING) = '2400' THEN "00:00"
    ELSE FORMAT_TIME(
      "%H:%M",
      PARSE_TIME("%H%M", LPAD(CAST(DEP_TIME AS STRING), 4, '0'))
    )
  END AS dep_time,

  DEP_DELAY,
  TAXI_OUT,

  -- WHEELS_OFF
  CASE
    WHEN CAST(WHEELS_OFF AS STRING) = '2400' THEN "00:00"
    ELSE FORMAT_TIME(
      "%H:%M",
      PARSE_TIME("%H%M", LPAD(CAST(WHEELS_OFF AS STRING), 4, '0'))
    )
  END AS wheels_off,

  -- WHEELS_ON
  CASE
    WHEN CAST(WHEELS_ON AS STRING) = '2400' THEN "00:00"
    ELSE FORMAT_TIME(
      "%H:%M",
      PARSE_TIME("%H%M", LPAD(CAST(WHEELS_ON AS STRING), 4, '0'))
    )
  END AS wheels_on,

  TAXI_IN,

  -- CRS_ARR_TIME
  CASE
    WHEN CAST(CRS_ARR_TIME AS STRING) = '2400' THEN "00:00"
    ELSE FORMAT_TIME(
      "%H:%M",
      PARSE_TIME("%H%M", LPAD(CAST(CRS_ARR_TIME AS STRING), 4, '0'))
    )
  END AS crs_arr_time,

  -- ARR_TIME
  CASE
    WHEN CAST(ARR_TIME AS STRING) = '2400' THEN "00:00"
    ELSE FORMAT_TIME(
      "%H:%M",
      PARSE_TIME("%H%M", LPAD(CAST(ARR_TIME AS STRING), 4, '0'))
    )
  END AS arr_time,

  ARR_DELAY,
  CANCELLED,
  CANCELLATION_CODE,
  DIVERTED,
  CRS_ELAPSED_TIME,
  ACTUAL_ELAPSED_TIME,
  AIR_TIME,

  -- Conversion de la distance de miles en kilomètres
  DISTANCE * 1.60934 as distance_km,

  CARRIER_DELAY,
  WEATHER_DELAY,
  NAS_DELAY,
  SECURITY_DELAY,
  LATE_AIRCRAFT_DELAY

from source_data
where
  FL_DATE is not null
  and op_carrier is not null
  and OP_CARRIER_FL_NUM is not null
  and ORIGIN is not null
  and DEST is not null
  and DEP_TIME is not null
  and DEP_DELAY is not null
  and TAXI_OUT is not null
  and WHEELS_OFF is not null
  and WHEELS_ON is not null
  and TAXI_IN is not null
  and ARR_TIME is not null
  and ARR_DELAY is not null
  and AIR_TIME is not null
  and DISTANCE is not null
  and CRS_ELAPSED_TIME is not null
  and CRS_ARR_TIME is not null
  and DEP_TIME >= 0 and DEP_TIME <= 2400
  and CRS_ELAPSED_TIME > 0
