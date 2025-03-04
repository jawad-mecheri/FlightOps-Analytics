with flight as (
  select *
  from {{ ref('silver_flight_enriched') }}
)

select
  flight_id,
  FL_DATE,
  -- Génération d'une clé date au format YYYYMMDD pour la jointure avec Dim_Date
  FORMAT_DATE('%Y%m%d', FL_DATE) as date_key,
  flight_year,
  -- Clé de la dimension compagnie : ici on utilise directement le code de la compagnie
  op_carrier as carrier_code,
  OP_CARRIER_FL_NUM,
  -- Clés d'aéroports d'origine et de destination (pour jointure avec Dim_Airport)
  ORIGIN as origin_airport_code,
  DEST as destination_airport_code,
  dep_time,
  -- Calcul d'une clé temps pour l'heure de départ (en minutes depuis minuit)
  (CAST(SUBSTR(dep_time, 1, 2) AS INT64) * 60 + CAST(SUBSTR(dep_time, 4, 2) AS INT64)) as departure_time_key,
  DEP_DELAY,
  TAXI_OUT,
  wheels_off,
  wheels_on,
  TAXI_IN,
  crs_dep_time,
    -- Calcul d'une clé temps pour l'heure d'arrivée prévu
  (CAST(SUBSTR(crs_dep_time, 1, 2) AS INT64) * 60 + CAST(SUBSTR(crs_dep_time, 4, 2) AS INT64)) as crs_dep_time_key,
  crs_arr_time,
    -- Calcul d'une clé temps pour l'heure d'arrivée prévu
  (CAST(SUBSTR(crs_arr_time, 1, 2) AS INT64) * 60 + CAST(SUBSTR(crs_arr_time, 4, 2) AS INT64)) as crs_arr_time_key,
  arr_time,
  -- Calcul d'une clé temps pour l'heure d'arrivée
  (CAST(SUBSTR(arr_time, 1, 2) AS INT64) * 60 + CAST(SUBSTR(arr_time, 4, 2) AS INT64)) as arrival_time_key,
  
  ARR_DELAY,
  CANCELLED,
  CANCELLATION_CODE,
  DIVERTED,
  CRS_ELAPSED_TIME,
  ACTUAL_ELAPSED_TIME,
  AIR_TIME,
  
  distance_km,
  
  CARRIER_DELAY,
  WEATHER_DELAY,
  NAS_DELAY,
  SECURITY_DELAY,
  LATE_AIRCRAFT_DELAY

from flight
