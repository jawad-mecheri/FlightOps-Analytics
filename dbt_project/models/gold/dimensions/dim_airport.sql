
with flight_airports as (
  select ORIGIN as airport_code
  from {{ ref('silver_flight_enriched') }}
  
  union all
  
  select DEST as airport_code
  from {{ ref('silver_flight_enriched') }}
),

unique_airports as (
  -- Éliminer les doublons
  select distinct airport_code
  from flight_airports
),

airport_data as (
  select *
  from {{ ref('silver_aeroport') }}
)

select
  u.airport_code as airport_key,
  a.ident,
  a.type,
  a.name,
  a.latitude_deg,
  a.longitude_deg,
  a.elevation_ft,
  a.country_name,
  a.iso_country,
  a.region_name,
  a.local_region,
  a.municipality,
  a.gps_code,
  a.icao_code,
  a.iata_code,
  a.home_link
from unique_airports u
left join airport_data a
  on u.airport_code = a.iata_code
