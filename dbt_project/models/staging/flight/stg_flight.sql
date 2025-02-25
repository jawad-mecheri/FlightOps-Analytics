{{ config(materialized='table') }}

with union_data as (

    -- Année 2009
    select
        2009 as flight_year,
        FL_DATE,
        OP_CARRIER as op_carrier,
        OP_CARRIER_FL_NUM,
        ORIGIN,
        DEST,
        CAST(CRS_DEP_TIME as INT64) as CRS_DEP_TIME,
        CAST(DEP_TIME as INT64) as DEP_TIME,
        CAST(DEP_DELAY as INT64) as DEP_DELAY,
        CAST(TAXI_OUT as INT64) as TAXI_OUT,
        CAST(WHEELS_OFF as INT64) as WHEELS_OFF,
        CAST(WHEELS_ON as INT64) as WHEELS_ON,
        CAST(TAXI_IN as INT64) as TAXI_IN,
        CAST(CRS_ARR_TIME as INT64) as CRS_ARR_TIME,
        CAST(ARR_TIME as INT64) as ARR_TIME,
        CAST(ARR_DELAY as INT64) as ARR_DELAY,
        IF(CAST(CANCELLED as INT64) = 1, true, false) as CANCELLED,
        CANCELLATION_CODE,
        IF(CAST(DIVERTED as INT64) = 1, true, false) as DIVERTED,
        CAST(CRS_ELAPSED_TIME as INT64) as CRS_ELAPSED_TIME,
        CAST(ACTUAL_ELAPSED_TIME as INT64) as ACTUAL_ELAPSED_TIME,
        CAST(AIR_TIME as INT64) as AIR_TIME,
        CAST(DISTANCE as INT64) as DISTANCE,
        CAST(CARRIER_DELAY as INT64) as CARRIER_DELAY,
        CAST(WEATHER_DELAY as INT64) as WEATHER_DELAY,
        CAST(NAS_DELAY as INT64) as NAS_DELAY,
        CAST(SECURITY_DELAY as INT64) as SECURITY_DELAY,
        CAST(LATE_AIRCRAFT_DELAY as INT64) as LATE_AIRCRAFT_DELAY
    from {{ source('raw_flight_data', '2009') }}
    where
      FL_DATE is not null and
      OP_CARRIER is not null and
      OP_CARRIER_FL_NUM is not null and
      ORIGIN is not null and
      DEST is not null and
      CRS_DEP_TIME is not null and
      DEP_TIME is not null and
      DEP_DELAY is not null and
      TAXI_OUT is not null and
      WHEELS_OFF is not null and
      WHEELS_ON is not null and
      TAXI_IN is not null and
      CRS_ARR_TIME is not null and
      ARR_TIME is not null and
      ARR_DELAY is not null and
      CANCELLED is not null and
      DIVERTED is not null and
      CRS_ELAPSED_TIME is not null and
      CRS_ELAPSED_TIME >= 0 and
      ACTUAL_ELAPSED_TIME is not null and
      AIR_TIME is not null and
      DISTANCE is not null

    union all

    -- Année 2010
    select
        2010 as flight_year,
        FL_DATE,
        OP_CARRIER as op_carrier,
        OP_CARRIER_FL_NUM,
        ORIGIN,
        DEST,
        CAST(CRS_DEP_TIME as INT64) as CRS_DEP_TIME,
        CAST(DEP_TIME as INT64) as DEP_TIME,
        CAST(DEP_DELAY as INT64) as DEP_DELAY,
        CAST(TAXI_OUT as INT64) as TAXI_OUT,
        CAST(WHEELS_OFF as INT64) as WHEELS_OFF,
        CAST(WHEELS_ON as INT64) as WHEELS_ON,
        CAST(TAXI_IN as INT64) as TAXI_IN,
        CAST(CRS_ARR_TIME as INT64) as CRS_ARR_TIME,
        CAST(ARR_TIME as INT64) as ARR_TIME,
        CAST(ARR_DELAY as INT64) as ARR_DELAY,
        IF(CAST(CANCELLED as INT64) = 1, true, false) as CANCELLED,
        CANCELLATION_CODE,
        IF(CAST(DIVERTED as INT64) = 1, true, false) as DIVERTED,
        CAST(CRS_ELAPSED_TIME as INT64) as CRS_ELAPSED_TIME,
        CAST(ACTUAL_ELAPSED_TIME as INT64) as ACTUAL_ELAPSED_TIME,
        CAST(AIR_TIME as INT64) as AIR_TIME,
        CAST(DISTANCE as INT64) as DISTANCE,
        CAST(CARRIER_DELAY as INT64) as CARRIER_DELAY,
        CAST(WEATHER_DELAY as INT64) as WEATHER_DELAY,
        CAST(NAS_DELAY as INT64) as NAS_DELAY,
        CAST(SECURITY_DELAY as INT64) as SECURITY_DELAY,
        CAST(LATE_AIRCRAFT_DELAY as INT64) as LATE_AIRCRAFT_DELAY
    from {{ source('raw_flight_data', '2010') }}
    where
      FL_DATE is not null and
      OP_CARRIER is not null and
      OP_CARRIER_FL_NUM is not null and
      ORIGIN is not null and
      DEST is not null and
      CRS_DEP_TIME is not null and
      DEP_TIME is not null and
      DEP_DELAY is not null and
      TAXI_OUT is not null and
      WHEELS_OFF is not null and
      WHEELS_ON is not null and
      TAXI_IN is not null and
      CRS_ARR_TIME is not null and
      ARR_TIME is not null and
      ARR_DELAY is not null and
      CANCELLED is not null and
      DIVERTED is not null and
      CRS_ELAPSED_TIME is not null and
      CRS_ELAPSED_TIME >= 0 and
      ACTUAL_ELAPSED_TIME is not null and
      AIR_TIME is not null and
      DISTANCE is not null

    union all

    -- Année 2011
    select
        2011 as flight_year,
        FL_DATE,
        OP_CARRIER as op_carrier,
        OP_CARRIER_FL_NUM,
        ORIGIN,
        DEST,
        CAST(CRS_DEP_TIME as INT64) as CRS_DEP_TIME,
        CAST(DEP_TIME as INT64) as DEP_TIME,
        CAST(DEP_DELAY as INT64) as DEP_DELAY,
        CAST(TAXI_OUT as INT64) as TAXI_OUT,
        CAST(WHEELS_OFF as INT64) as WHEELS_OFF,
        CAST(WHEELS_ON as INT64) as WHEELS_ON,
        CAST(TAXI_IN as INT64) as TAXI_IN,
        CAST(CRS_ARR_TIME as INT64) as CRS_ARR_TIME,
        CAST(ARR_TIME as INT64) as ARR_TIME,
        CAST(ARR_DELAY as INT64) as ARR_DELAY,
        IF(CAST(CANCELLED as INT64) = 1, true, false) as CANCELLED,
        CANCELLATION_CODE,
        IF(CAST(DIVERTED as INT64) = 1, true, false) as DIVERTED,
        CAST(CRS_ELAPSED_TIME as INT64) as CRS_ELAPSED_TIME,
        CAST(ACTUAL_ELAPSED_TIME as INT64) as ACTUAL_ELAPSED_TIME,
        CAST(AIR_TIME as INT64) as AIR_TIME,
        CAST(DISTANCE as INT64) as DISTANCE,
        CAST(CARRIER_DELAY as INT64) as CARRIER_DELAY,
        CAST(WEATHER_DELAY as INT64) as WEATHER_DELAY,
        CAST(NAS_DELAY as INT64) as NAS_DELAY,
        CAST(SECURITY_DELAY as INT64) as SECURITY_DELAY,
        CAST(LATE_AIRCRAFT_DELAY as INT64) as LATE_AIRCRAFT_DELAY
    from {{ source('raw_flight_data', '2011') }}
    where
      FL_DATE is not null and
      OP_CARRIER is not null and
      OP_CARRIER_FL_NUM is not null and
      ORIGIN is not null and
      DEST is not null and
      CRS_DEP_TIME is not null and
      DEP_TIME is not null and
      DEP_DELAY is not null and
      TAXI_OUT is not null and
      WHEELS_OFF is not null and
      WHEELS_ON is not null and
      TAXI_IN is not null and
      CRS_ARR_TIME is not null and
      ARR_TIME is not null and
      ARR_DELAY is not null and
      CANCELLED is not null and
      DIVERTED is not null and
      CRS_ELAPSED_TIME is not null and
      CRS_ELAPSED_TIME >= 0 and
      ACTUAL_ELAPSED_TIME is not null and
      AIR_TIME is not null and
      DISTANCE is not null

    union all

    -- Année 2012
    select
        2012 as flight_year,
        FL_DATE,
        OP_CARRIER as op_carrier,
        OP_CARRIER_FL_NUM,
        ORIGIN,
        DEST,
        CAST(CRS_DEP_TIME as INT64) as CRS_DEP_TIME,
        CAST(DEP_TIME as INT64) as DEP_TIME,
        CAST(DEP_DELAY as INT64) as DEP_DELAY,
        CAST(TAXI_OUT as INT64) as TAXI_OUT,
        CAST(WHEELS_OFF as INT64) as WHEELS_OFF,
        CAST(WHEELS_ON as INT64) as WHEELS_ON,
        CAST(TAXI_IN as INT64) as TAXI_IN,
        CAST(CRS_ARR_TIME as INT64) as CRS_ARR_TIME,
        CAST(ARR_TIME as INT64) as ARR_TIME,
        CAST(ARR_DELAY as INT64) as ARR_DELAY,
        IF(CAST(CANCELLED as INT64) = 1, true, false) as CANCELLED,
        CANCELLATION_CODE,
        IF(CAST(DIVERTED as INT64) = 1, true, false) as DIVERTED,
        CAST(CRS_ELAPSED_TIME as INT64) as CRS_ELAPSED_TIME,
        CAST(ACTUAL_ELAPSED_TIME as INT64) as ACTUAL_ELAPSED_TIME,
        CAST(AIR_TIME as INT64) as AIR_TIME,
        CAST(DISTANCE as INT64) as DISTANCE,
        CAST(CARRIER_DELAY as INT64) as CARRIER_DELAY,
        CAST(WEATHER_DELAY as INT64) as WEATHER_DELAY,
        CAST(NAS_DELAY as INT64) as NAS_DELAY,
        CAST(SECURITY_DELAY as INT64) as SECURITY_DELAY,
        CAST(LATE_AIRCRAFT_DELAY as INT64) as LATE_AIRCRAFT_DELAY
    from {{ source('raw_flight_data', '2012') }}
    where
      FL_DATE is not null and
      OP_CARRIER is not null and
      OP_CARRIER_FL_NUM is not null and
      ORIGIN is not null and
      DEST is not null and
      CRS_DEP_TIME is not null and
      DEP_TIME is not null and
      DEP_DELAY is not null and
      TAXI_OUT is not null and
      WHEELS_OFF is not null and
      WHEELS_ON is not null and
      TAXI_IN is not null and
      CRS_ARR_TIME is not null and
      ARR_TIME is not null and
      ARR_DELAY is not null and
      CANCELLED is not null and
      DIVERTED is not null and
      CRS_ELAPSED_TIME is not null and
      CRS_ELAPSED_TIME >= 0 and
      ACTUAL_ELAPSED_TIME is not null and
      AIR_TIME is not null and
      DISTANCE is not null

    union all

    -- Année 2013
    select
        2013 as flight_year,
        FL_DATE,
        OP_CARRIER as op_carrier,
        OP_CARRIER_FL_NUM,
        ORIGIN,
        DEST,
        CAST(CRS_DEP_TIME as INT64) as CRS_DEP_TIME,
        CAST(DEP_TIME as INT64) as DEP_TIME,
        CAST(DEP_DELAY as INT64) as DEP_DELAY,
        CAST(TAXI_OUT as INT64) as TAXI_OUT,
        CAST(WHEELS_OFF as INT64) as WHEELS_OFF,
        CAST(WHEELS_ON as INT64) as WHEELS_ON,
        CAST(TAXI_IN as INT64) as TAXI_IN,
        CAST(CRS_ARR_TIME as INT64) as CRS_ARR_TIME,
        CAST(ARR_TIME as INT64) as ARR_TIME,
        CAST(ARR_DELAY as INT64) as ARR_DELAY,
        IF(CAST(CANCELLED as INT64) = 1, true, false) as CANCELLED,
        CANCELLATION_CODE,
        IF(CAST(DIVERTED as INT64) = 1, true, false) as DIVERTED,
        CAST(CRS_ELAPSED_TIME as INT64) as CRS_ELAPSED_TIME,
        CAST(ACTUAL_ELAPSED_TIME as INT64) as ACTUAL_ELAPSED_TIME,
        CAST(AIR_TIME as INT64) as AIR_TIME,
        CAST(DISTANCE as INT64) as DISTANCE,
        CAST(CARRIER_DELAY as INT64) as CARRIER_DELAY,
        CAST(WEATHER_DELAY as INT64) as WEATHER_DELAY,
        CAST(NAS_DELAY as INT64) as NAS_DELAY,
        CAST(SECURITY_DELAY as INT64) as SECURITY_DELAY,
        CAST(LATE_AIRCRAFT_DELAY as INT64) as LATE_AIRCRAFT_DELAY
    from {{ source('raw_flight_data', '2013') }}
    where
      FL_DATE is not null and
      OP_CARRIER is not null and
      OP_CARRIER_FL_NUM is not null and
      ORIGIN is not null and
      DEST is not null and
      CRS_DEP_TIME is not null and
      DEP_TIME is not null and
      DEP_DELAY is not null and
      TAXI_OUT is not null and
      WHEELS_OFF is not null and
      WHEELS_ON is not null and
      TAXI_IN is not null and
      CRS_ARR_TIME is not null and
      ARR_TIME is not null and
      ARR_DELAY is not null and
      CANCELLED is not null and
      DIVERTED is not null and
      CRS_ELAPSED_TIME is not null and
      CRS_ELAPSED_TIME >= 0 and
      ACTUAL_ELAPSED_TIME is not null and
      AIR_TIME is not null and
      DISTANCE is not null

    union all

    -- Année 2014
    select
        2014 as flight_year,
        FL_DATE,
        OP_CARRIER as op_carrier,
        OP_CARRIER_FL_NUM,
        ORIGIN,
        DEST,
        CAST(CRS_DEP_TIME as INT64) as CRS_DEP_TIME,
        CAST(DEP_TIME as INT64) as DEP_TIME,
        CAST(DEP_DELAY as INT64) as DEP_DELAY,
        CAST(TAXI_OUT as INT64) as TAXI_OUT,
        CAST(WHEELS_OFF as INT64) as WHEELS_OFF,
        CAST(WHEELS_ON as INT64) as WHEELS_ON,
        CAST(TAXI_IN as INT64) as TAXI_IN,
        CAST(CRS_ARR_TIME as INT64) as CRS_ARR_TIME,
        CAST(ARR_TIME as INT64) as ARR_TIME,
        CAST(ARR_DELAY as INT64) as ARR_DELAY,
        IF(CAST(CANCELLED as INT64) = 1, true, false) as CANCELLED,
        CANCELLATION_CODE,
        IF(CAST(DIVERTED as INT64) = 1, true, false) as DIVERTED,
        CAST(CRS_ELAPSED_TIME as INT64) as CRS_ELAPSED_TIME,
        CAST(ACTUAL_ELAPSED_TIME as INT64) as ACTUAL_ELAPSED_TIME,
        CAST(AIR_TIME as INT64) as AIR_TIME,
        CAST(DISTANCE as INT64) as DISTANCE,
        CAST(CARRIER_DELAY as INT64) as CARRIER_DELAY,
        CAST(WEATHER_DELAY as INT64) as WEATHER_DELAY,
        CAST(NAS_DELAY as INT64) as NAS_DELAY,
        CAST(SECURITY_DELAY as INT64) as SECURITY_DELAY,
        CAST(LATE_AIRCRAFT_DELAY as INT64) as LATE_AIRCRAFT_DELAY
    from {{ source('raw_flight_data', '2014') }}
    where
      FL_DATE is not null and
      OP_CARRIER is not null and
      OP_CARRIER_FL_NUM is not null and
      ORIGIN is not null and
      DEST is not null and
      CRS_DEP_TIME is not null and
      DEP_TIME is not null and
      DEP_DELAY is not null and
      TAXI_OUT is not null and
      WHEELS_OFF is not null and
      WHEELS_ON is not null and
      TAXI_IN is not null and
      CRS_ARR_TIME is not null and
      ARR_TIME is not null and
      ARR_DELAY is not null and
      CANCELLED is not null and
      DIVERTED is not null and
      CRS_ELAPSED_TIME is not null and
      CRS_ELAPSED_TIME >= 0 and
      ACTUAL_ELAPSED_TIME is not null and
      AIR_TIME is not null and
      DISTANCE is not null

    union all

    -- Année 2015
    select
        2015 as flight_year,
        FL_DATE,
        OP_CARRIER as op_carrier,
        OP_CARRIER_FL_NUM,
        ORIGIN,
        DEST,
        CAST(CRS_DEP_TIME as INT64) as CRS_DEP_TIME,
        CAST(DEP_TIME as INT64) as DEP_TIME,
        CAST(DEP_DELAY as INT64) as DEP_DELAY,
        CAST(TAXI_OUT as INT64) as TAXI_OUT,
        CAST(WHEELS_OFF as INT64) as WHEELS_OFF,
        CAST(WHEELS_ON as INT64) as WHEELS_ON,
        CAST(TAXI_IN as INT64) as TAXI_IN,
        CAST(CRS_ARR_TIME as INT64) as CRS_ARR_TIME,
        CAST(ARR_TIME as INT64) as ARR_TIME,
        CAST(ARR_DELAY as INT64) as ARR_DELAY,
        IF(CAST(CANCELLED as INT64) = 1, true, false) as CANCELLED,
        CANCELLATION_CODE,
        IF(CAST(DIVERTED as INT64) = 1, true, false) as DIVERTED,
        CAST(CRS_ELAPSED_TIME as INT64) as CRS_ELAPSED_TIME,
        CAST(ACTUAL_ELAPSED_TIME as INT64) as ACTUAL_ELAPSED_TIME,
        CAST(AIR_TIME as INT64) as AIR_TIME,
        CAST(DISTANCE as INT64) as DISTANCE,
        CAST(CARRIER_DELAY as INT64) as CARRIER_DELAY,
        CAST(WEATHER_DELAY as INT64) as WEATHER_DELAY,
        CAST(NAS_DELAY as INT64) as NAS_DELAY,
        CAST(SECURITY_DELAY as INT64) as SECURITY_DELAY,
        CAST(LATE_AIRCRAFT_DELAY as INT64) as LATE_AIRCRAFT_DELAY
    from {{ source('raw_flight_data', '2015') }}
    where
      FL_DATE is not null and
      OP_CARRIER is not null and
      OP_CARRIER_FL_NUM is not null and
      ORIGIN is not null and
      DEST is not null and
      CRS_DEP_TIME is not null and
      DEP_TIME is not null and
      DEP_DELAY is not null and
      TAXI_OUT is not null and
      WHEELS_OFF is not null and
      WHEELS_ON is not null and
      TAXI_IN is not null and
      CRS_ARR_TIME is not null and
      ARR_TIME is not null and
      ARR_DELAY is not null and
      CANCELLED is not null and
      DIVERTED is not null and
      CRS_ELAPSED_TIME is not null and
      CRS_ELAPSED_TIME >= 0 and
      ACTUAL_ELAPSED_TIME is not null and
      AIR_TIME is not null and
      DISTANCE is not null

    union all

    -- Année 2016
    select
        2016 as flight_year,
        FL_DATE,
        OP_CARRIER as op_carrier,
        OP_CARRIER_FL_NUM,
        ORIGIN,
        DEST,
        CAST(CRS_DEP_TIME as INT64) as CRS_DEP_TIME,
        CAST(DEP_TIME as INT64) as DEP_TIME,
        CAST(DEP_DELAY as INT64) as DEP_DELAY,
        CAST(TAXI_OUT as INT64) as TAXI_OUT,
        CAST(WHEELS_OFF as INT64) as WHEELS_OFF,
        CAST(WHEELS_ON as INT64) as WHEELS_ON,
        CAST(TAXI_IN as INT64) as TAXI_IN,
        CAST(CRS_ARR_TIME as INT64) as CRS_ARR_TIME,
        CAST(ARR_TIME as INT64) as ARR_TIME,
        CAST(ARR_DELAY as INT64) as ARR_DELAY,
        IF(CAST(CANCELLED as INT64) = 1, true, false) as CANCELLED,
        CANCELLATION_CODE,
        IF(CAST(DIVERTED as INT64) = 1, true, false) as DIVERTED,
        CAST(CRS_ELAPSED_TIME as INT64) as CRS_ELAPSED_TIME,
        CAST(ACTUAL_ELAPSED_TIME as INT64) as ACTUAL_ELAPSED_TIME,
        CAST(AIR_TIME as INT64) as AIR_TIME,
        CAST(DISTANCE as INT64) as DISTANCE,
        CAST(CARRIER_DELAY as INT64) as CARRIER_DELAY,
        CAST(WEATHER_DELAY as INT64) as WEATHER_DELAY,
        CAST(NAS_DELAY as INT64) as NAS_DELAY,
        CAST(SECURITY_DELAY as INT64) as SECURITY_DELAY,
        CAST(LATE_AIRCRAFT_DELAY as INT64) as LATE_AIRCRAFT_DELAY
    from {{ source('raw_flight_data', '2016') }}
    where
      FL_DATE is not null and
      OP_CARRIER is not null and
      OP_CARRIER_FL_NUM is not null and
      ORIGIN is not null and
      DEST is not null and
      CRS_DEP_TIME is not null and
      DEP_TIME is not null and
      DEP_DELAY is not null and
      TAXI_OUT is not null and
      WHEELS_OFF is not null and
      WHEELS_ON is not null and
      TAXI_IN is not null and
      CRS_ARR_TIME is not null and
      ARR_TIME is not null and
      ARR_DELAY is not null and
      CANCELLED is not null and
      DIVERTED is not null and
      CRS_ELAPSED_TIME is not null and
      CRS_ELAPSED_TIME >= 0 and
      ACTUAL_ELAPSED_TIME is not null and
      AIR_TIME is not null and
      DISTANCE is not null

    union all

    -- Année 2017
    select
        2017 as flight_year,
        FL_DATE,
        OP_CARRIER as op_carrier,
        OP_CARRIER_FL_NUM,
        ORIGIN,
        DEST,
        CAST(CRS_DEP_TIME as INT64) as CRS_DEP_TIME,
        CAST(DEP_TIME as INT64) as DEP_TIME,
        CAST(DEP_DELAY as INT64) as DEP_DELAY,
        CAST(TAXI_OUT as INT64) as TAXI_OUT,
        CAST(WHEELS_OFF as INT64) as WHEELS_OFF,
        CAST(WHEELS_ON as INT64) as WHEELS_ON,
        CAST(TAXI_IN as INT64) as TAXI_IN,
        CAST(CRS_ARR_TIME as INT64) as CRS_ARR_TIME,
        CAST(ARR_TIME as INT64) as ARR_TIME,
        CAST(ARR_DELAY as INT64) as ARR_DELAY,
        IF(CAST(CANCELLED as INT64) = 1, true, false) as CANCELLED,
        CANCELLATION_CODE,
        IF(CAST(DIVERTED as INT64) = 1, true, false) as DIVERTED,
        CAST(CRS_ELAPSED_TIME as INT64) as CRS_ELAPSED_TIME,
        CAST(ACTUAL_ELAPSED_TIME as INT64) as ACTUAL_ELAPSED_TIME,
        CAST(AIR_TIME as INT64) as AIR_TIME,
        CAST(DISTANCE as INT64) as DISTANCE,
        CAST(CARRIER_DELAY as INT64) as CARRIER_DELAY,
        CAST(WEATHER_DELAY as INT64) as WEATHER_DELAY,
        CAST(NAS_DELAY as INT64) as NAS_DELAY,
        CAST(SECURITY_DELAY as INT64) as SECURITY_DELAY,
        CAST(LATE_AIRCRAFT_DELAY as INT64) as LATE_AIRCRAFT_DELAY
    from {{ source('raw_flight_data', '2017') }}
    where
      FL_DATE is not null and
      OP_CARRIER is not null and
      OP_CARRIER_FL_NUM is not null and
      ORIGIN is not null and
      DEST is not null and
      CRS_DEP_TIME is not null and
      DEP_TIME is not null and
      DEP_DELAY is not null and
      TAXI_OUT is not null and
      WHEELS_OFF is not null and
      WHEELS_ON is not null and
      TAXI_IN is not null and
      CRS_ARR_TIME is not null and
      ARR_TIME is not null and
      ARR_DELAY is not null and
      CANCELLED is not null and
      DIVERTED is not null and
      CRS_ELAPSED_TIME is not null and
      CRS_ELAPSED_TIME >= 0 and

      ACTUAL_ELAPSED_TIME is not null and
      AIR_TIME is not null and
      DISTANCE is not null

    union all

    -- Année 2018
    select
        2018 as flight_year,
        FL_DATE,
        OP_CARRIER as op_carrier,
        OP_CARRIER_FL_NUM,
        ORIGIN,
        DEST,
        CAST(CRS_DEP_TIME as INT64) as CRS_DEP_TIME,
        CAST(DEP_TIME as INT64) as DEP_TIME,
        CAST(DEP_DELAY as INT64) as DEP_DELAY,
        CAST(TAXI_OUT as INT64) as TAXI_OUT,
        CAST(WHEELS_OFF as INT64) as WHEELS_OFF,
        CAST(WHEELS_ON as INT64) as WHEELS_ON,
        CAST(TAXI_IN as INT64) as TAXI_IN,
        CAST(CRS_ARR_TIME as INT64) as CRS_ARR_TIME,
        CAST(ARR_TIME as INT64) as ARR_TIME,
        CAST(ARR_DELAY as INT64) as ARR_DELAY,
        IF(CAST(CANCELLED as INT64) = 1, true, false) as CANCELLED,
        CANCELLATION_CODE,
        IF(CAST(DIVERTED as INT64) = 1, true, false) as DIVERTED,
        CAST(CRS_ELAPSED_TIME as INT64) as CRS_ELAPSED_TIME,
        CAST(ACTUAL_ELAPSED_TIME as INT64) as ACTUAL_ELAPSED_TIME,
        CAST(AIR_TIME as INT64) as AIR_TIME,
        CAST(DISTANCE as INT64) as DISTANCE,
        CAST(CARRIER_DELAY as INT64) as CARRIER_DELAY,
        CAST(WEATHER_DELAY as INT64) as WEATHER_DELAY,
        CAST(NAS_DELAY as INT64) as NAS_DELAY,
        CAST(SECURITY_DELAY as INT64) as SECURITY_DELAY,
        CAST(LATE_AIRCRAFT_DELAY as INT64) as LATE_AIRCRAFT_DELAY
    from {{ source('raw_flight_data', '2018') }}
    where
      FL_DATE is not null and
      OP_CARRIER is not null and
      OP_CARRIER_FL_NUM is not null and
      ORIGIN is not null and
      DEST is not null and
      CRS_DEP_TIME is not null and
      DEP_TIME is not null and
      DEP_DELAY is not null and
      TAXI_OUT is not null and
      WHEELS_OFF is not null and
      WHEELS_ON is not null and
      TAXI_IN is not null and
      CRS_ARR_TIME is not null and
      ARR_TIME is not null and
      ARR_DELAY is not null and
      CANCELLED is not null and
      DIVERTED is not null and
      CRS_ELAPSED_TIME is not null and
      CRS_ELAPSED_TIME >= 0 and

      ACTUAL_ELAPSED_TIME is not null and
      AIR_TIME is not null and
      DISTANCE is not null
)

select *
from union_data