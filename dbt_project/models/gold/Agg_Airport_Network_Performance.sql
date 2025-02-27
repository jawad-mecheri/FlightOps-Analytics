{{ config(materialized='table') }}

with flight_origin as (
  select 
    flight_year,
    origin_airport_code as airport_key,
    count(*) as departure_count,
    avg(TAXI_OUT) as avg_taxi_out
  from {{ ref('fact_flight_operations') }}
  group by flight_year, origin_airport_code
),

flight_destination as (
  select 
    flight_year,
    destination_airport_code as airport_key,
    count(*) as arrival_count,
    avg(TAXI_IN) as avg_taxi_in
  from {{ ref('fact_flight_operations') }}
  group by flight_year, destination_airport_code
),

combined as (
  -- Agrégation opérationnelle par année et par aéroport
  select
    coalesce(o.flight_year, d.flight_year) as flight_year,
    coalesce(o.airport_key, d.airport_key) as airport_key,
    (coalesce(departure_count, 0) + coalesce(arrival_count, 0)) as flight_volume,
    o.avg_taxi_out,
    d.avg_taxi_in,
    (coalesce(o.avg_taxi_out, 0) + coalesce(d.avg_taxi_in, 0)) as avg_ground_time
  from flight_origin o
  full outer join flight_destination d
    on o.flight_year = d.flight_year
    and o.airport_key = d.airport_key
),

graph_metrics as (
  select 
    flight_year,
    airport_key,
    cluster_class,
    betweenness_centrality,
    degree_centrality
  from `flightops-analytics.aeroport_data.airport_network_metrics`
)

select
  c.flight_year,
  c.airport_key,
  c.flight_volume,
  gm.cluster_class,
  gm.betweenness_centrality,
  gm.degree_centrality,
  c.avg_taxi_out,
  c.avg_taxi_in,
  c.avg_ground_time
from combined c
left join graph_metrics gm
  on c.flight_year = gm.flight_year
  and c.airport_key = gm.airport_key
