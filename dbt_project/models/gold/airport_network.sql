{{ config(materialized='table') }}

with base as (
  select
    flight_year,
    origin_airport_code as source_airport,
    destination_airport_code as target_airport,
    count(*) as weight
  from {{ ref('fact_flight_operations') }}
  group by flight_year, origin_airport_code, destination_airport_code
)

select *
from base
