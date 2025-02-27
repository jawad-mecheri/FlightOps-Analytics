with flight_delay as (
  select
    flight_id,
    ARR_DELAY,
    
    coalesce(CARRIER_DELAY, 0) as carrier_delay_1,
    coalesce(WEATHER_DELAY, 0) as weather_delay_1,
    coalesce(NAS_DELAY, 0) as nas_delay_1,
    coalesce(SECURITY_DELAY, 0) as security_delay_1,
    coalesce(LATE_AIRCRAFT_DELAY, 0) as late_aircraft_delay_1,
    
    case 
      when ARR_DELAY <= 0 then 'On time / Early'
      when ARR_DELAY > 0 
           and (CARRIER_DELAY is null 
                and WEATHER_DELAY is null 
                and NAS_DELAY is null 
                and SECURITY_DELAY is null 
                and LATE_AIRCRAFT_DELAY is null)
           then 'Delayed (unknown breakdown)'
      when ARR_DELAY > 0 
           and (CARRIER_DELAY is not null 
                or WEATHER_DELAY is not null 
                or NAS_DELAY is not null 
                or SECURITY_DELAY is not null 
                or LATE_AIRCRAFT_DELAY is not null)
           then 'Delayed (with breakdown)'
      else 'Unknown'
    end as delay_category,
    
    coalesce(CARRIER_DELAY, 0) 
      + coalesce(WEATHER_DELAY, 0)
      + coalesce(NAS_DELAY, 0)
      + coalesce(SECURITY_DELAY, 0)
      + coalesce(LATE_AIRCRAFT_DELAY, 0) as total_breakdown_delay,
      
    ARR_DELAY - (
      coalesce(CARRIER_DELAY, 0)
      + coalesce(WEATHER_DELAY, 0)
      + coalesce(NAS_DELAY, 0)
      + coalesce(SECURITY_DELAY, 0)
      + coalesce(LATE_AIRCRAFT_DELAY, 0)
    ) as delta_delay
    
  from {{ ref('fact_flight_operations') }}
)

select *
from flight_delay
