

select
    FL_DATE,
    OP_CARRIER as op_carrier,
    OP_CARRIER_FL_NUM,
    ORIGIN,
    DEST,
    CRS_DEP_TIME,
    DEP_TIME,
    DEP_DELAY,
    TAXI_OUT,
    WHEELS_OFF,
    WHEELS_ON,
    TAXI_IN,
    CRS_ARR_TIME,
    ARR_TIME,
    ARR_DELAY,
    CANCELLED,
    CANCELLATION_CODE,
    DIVERTED,
    CRS_ELAPSED_TIME,
    ACTUAL_ELAPSED_TIME,
    AIR_TIME,
    DISTANCE,
    CARRIER_DELAY,
    WEATHER_DELAY,
    NAS_DELAY,
    SECURITY_DELAY,
    LATE_AIRCRAFT_DELAY
from {{ source('raw_flight_data', '2018') }}

