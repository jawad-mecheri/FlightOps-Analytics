with source_data as (
  select
    id as id_aeroport ,
    ident,
    type,
    name,
    latitude_deg,
    longitude_deg,
    elevation_ft,
    country_name,
    iso_country,
    region_name,
    local_region,
    municipality,
    gps_code,
    icao_code,
    iata_code,
    local_code,
    home_link
  from {{ ref('stg_aeroport') }}
)

select *
from source_data
where 
  id_aeroport is not null
  and ident is not null
  and type is not null
  and name is not null
  and latitude_deg is not null
  and longitude_deg is not null
  and elevation_ft is not null
  and country_name is not null
  and iso_country is not null
  and region_name is not null
  and local_region is not null
  and municipality is not null
  and gps_code is not null
  and icao_code is not null
  and iata_code is not null
  and local_code is not null
  and home_link is not null
