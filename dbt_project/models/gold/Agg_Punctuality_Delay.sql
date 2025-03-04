{{ config(materialized='table') }}

with flight_data as (
    SELECT
        f.date_key,
        f.carrier_code,
        c.airline_name,
        f.origin_airport_code,
        oa.name as origin_name,
        f.destination_airport_code,
        da.name as destination_name,
        dd.holiday_flag,
        dd.day_of_week,
        dd.month,
        dd.year,
        tt.period_of_day as time_period,
        f.DEP_DELAY,
        f.ARR_DELAY,
        f.CARRIER_DELAY,
        f.WEATHER_DELAY,
        f.NAS_DELAY,
        f.SECURITY_DELAY,
        f.LATE_AIRCRAFT_DELAY,
        f.TAXI_OUT,
        f.TAXI_IN,
        f.CANCELLED
    FROM {{ ref('fact_flight_operations') }} f
    LEFT JOIN {{ ref('dim_carrier') }} c 
        ON f.carrier_code = c.carrier_key
    LEFT JOIN {{ ref('dim_airport') }} oa 
        ON f.origin_airport_code = oa.airport_key
    LEFT JOIN {{ ref('dim_airport') }} da 
        ON f.destination_airport_code = da.airport_key
    LEFT JOIN {{ ref('dim_date') }} dd 
        ON f.date_key = dd.date_key
    LEFT JOIN {{ ref('dim_time') }} tt 
        ON f.departure_time_key = tt.time_key
    -- Suppression de la condition WHERE pour voir toutes les années
)

SELECT
    date_key,
    carrier_code,
    airline_name,
    origin_airport_code,
    origin_name,
    destination_airport_code,
    destination_name,
    holiday_flag,
    day_of_week,
    month,
    year,
    time_period,
    COUNT(*) as flight_count,
    AVG(DEP_DELAY) as avg_dep_delay,
    AVG(ARR_DELAY) as avg_arr_delay,
    AVG(CARRIER_DELAY) as avg_carrier_delay,
    AVG(WEATHER_DELAY) as avg_weather_delay,
    AVG(NAS_DELAY) as avg_nas_delay,
    AVG(SECURITY_DELAY) as avg_security_delay,
    AVG(LATE_AIRCRAFT_DELAY) as avg_late_aircraft_delay,
    AVG(TAXI_OUT) as avg_taxi_out,
    AVG(TAXI_IN) as avg_taxi_in,
    SUM(CASE WHEN ARR_DELAY > 15 THEN 1 ELSE 0 END) as delayed_flights,
    SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) as cancelled_flights
FROM flight_data
GROUP BY
    date_key,
    carrier_code,
    airline_name,
    origin_airport_code,
    origin_name,
    destination_airport_code,
    destination_name,
    holiday_flag,
    day_of_week,
    month,
    year,
    time_period