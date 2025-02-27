with bronze as (
  select * from {{ ref('bronze_flights') }}
)
select
  CAST(flight_year AS INT64) as flight_year,
  FL_DATE,
  OP_CARRIER as op_carrier,
  OP_CARRIER_FL_NUM,
  ORIGIN,
  DEST,
  -- Transformation de DEP_TIME : conversion de '2400' en '0000' et cast en INT64
  CAST(
    CASE 
      WHEN CAST(DEP_TIME as STRING) = '2400' THEN '0000'
      ELSE LPAD(CAST(DEP_TIME as STRING), 4, '0')
    END as INT64
  ) as DEP_TIME,
  CAST(DEP_DELAY as INT64) as DEP_DELAY,
  CAST(TAXI_OUT as INT64) as TAXI_OUT,
  CAST(WHEELS_OFF as INT64) as WHEELS_OFF,
  CAST(WHEELS_ON as INT64) as WHEELS_ON,
  CAST(TAXI_IN as INT64) as TAXI_IN,
  CAST(ARR_TIME as INT64) as ARR_TIME,
  CAST(ARR_DELAY as INT64) as ARR_DELAY,
  {# Calcul de CRS_DEP_TIME en fonction de l'année #}
  CASE 
    WHEN CAST(flight_year AS INT64) < 2019 THEN
         CAST(
           CASE 
             WHEN CAST(CRS_DEP_TIME as STRING) = '2400' THEN '0000'
             ELSE LPAD(CAST(CRS_DEP_TIME as STRING), 4, '0')
           END as INT64)
    ELSE
         EXTRACT(HOUR FROM TIME_SUB(
            PARSE_TIME("%H%M", 
              CASE 
                WHEN CAST(DEP_TIME as STRING) = '2400' THEN '0000'
                ELSE LPAD(CAST(DEP_TIME as STRING), 4, '0') 
              END),
            INTERVAL CAST(DEP_DELAY AS INT64) MINUTE)) * 100 +
         EXTRACT(MINUTE FROM TIME_SUB(
            PARSE_TIME("%H%M", 
              CASE 
                WHEN CAST(DEP_TIME as STRING) = '2400' THEN '0000'
                ELSE LPAD(CAST(DEP_TIME as STRING), 4, '0') 
              END),
            INTERVAL CAST(DEP_DELAY AS INT64) MINUTE))
  END as CRS_DEP_TIME,
  {# Calcul de CRS_ARR_TIME en fonction de l'année #}
  CASE 
    WHEN CAST(flight_year AS INT64) < 2019 THEN
         CAST(
           CASE 
             WHEN CAST(CRS_ARR_TIME as STRING) = '2400' THEN '0000'
             ELSE LPAD(CAST(CRS_ARR_TIME as STRING), 4, '0')
           END as INT64)
    ELSE
         EXTRACT(HOUR FROM TIME_SUB(
            PARSE_TIME("%H%M", 
              CASE 
                WHEN CAST(ARR_TIME as STRING) = '2400' THEN '0000'
                ELSE LPAD(CAST(ARR_TIME as STRING), 4, '0') 
              END),
            INTERVAL CAST(ARR_DELAY AS INT64) MINUTE)) * 100 +
         EXTRACT(MINUTE FROM TIME_SUB(
            PARSE_TIME("%H%M", 
              CASE 
                WHEN CAST(ARR_TIME as STRING) = '2400' THEN '0000'
                ELSE LPAD(CAST(ARR_TIME as STRING), 4, '0') 
              END),
            INTERVAL CAST(ARR_DELAY AS INT64) MINUTE))
  END as CRS_ARR_TIME,
  CAST(AIR_TIME as INT64) as AIR_TIME,
  CAST(DISTANCE as INT64) as DISTANCE,
  CAST(CARRIER_DELAY as INT64) as CARRIER_DELAY,
  CAST(WEATHER_DELAY as INT64) as WEATHER_DELAY,
  CAST(NAS_DELAY as INT64) as NAS_DELAY,
  CAST(SECURITY_DELAY as INT64) as SECURITY_DELAY,
  CAST(LATE_AIRCRAFT_DELAY as INT64) as LATE_AIRCRAFT_DELAY,
  {# Ajout des champs supplémentaires #}
  CASE 
    WHEN CAST(flight_year AS INT64) < 2019 THEN CAST(CANCELLED as INT64)
    ELSE 0
  END as CANCELLED,
  CASE 
    WHEN CAST(flight_year AS INT64) < 2019 THEN CANCELLATION_CODE
    ELSE CAST(null as STRING)
  END as CANCELLATION_CODE,
  CASE 
    WHEN CAST(flight_year AS INT64) < 2019 THEN CAST(DIVERTED as INT64)
    ELSE 0
  END as DIVERTED,
  CRS_ELAPSED_TIME,
  ACTUAL_ELAPSED_TIME
from bronze
where 
  FL_DATE is not null and
  OP_CARRIER is not null and
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
  DEP_TIME >= 0 and DEP_TIME <= 2400  
order by flight_year desc
