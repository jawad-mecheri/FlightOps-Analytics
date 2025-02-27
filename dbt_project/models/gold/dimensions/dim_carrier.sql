with carriers as (
  select distinct op_carrier as carrier_code
  from {{ ref('silver_flight_enriched') }}
)

select
  carrier_code as carrier_key,
  carrier_code as op_carrier_code,
  carrier_code as airline_name 
from carriers
