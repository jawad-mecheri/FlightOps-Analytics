{{ config(materialized='table') }}

with base as (
  select
    flight_year,
    ORIGIN as source_airport,
    DEST as target_airport,
    count(*) as weight
  from {{ ref('silver_flight_enriched') }}
  group by flight_year, ORIGIN, DEST
)

select *
from base
ORDER BY flight_year desc 
