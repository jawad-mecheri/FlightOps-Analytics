with carriers as (
  select distinct carrier_code as carrier_code
  from {{ ref('fact_flight_operations') }}
)

select
  carrier_code as carrier_key,
  carrier_code as op_carrier_code,
  carrier_code as airline_name 
from carriers
