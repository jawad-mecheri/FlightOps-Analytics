{{ config(materialized='table') }}
{% set flight_years = ["2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019"] %}
{% set queries = [] %}

{% for flight_year in flight_years %}
    {% if flight_year < "2019" %}
        {% set query %}
            select 
                '{{ flight_year }}' as flight_year,
                FL_DATE,
                OP_CARRIER,
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
            from {{ source('raw_flight_data', flight_year) }}
        {% endset %}
    {% else %}
        {% set query %}
            select 
                '{{ flight_year }}' as flight_year,
                FL_DATE,
                OP_UNIQUE_CARRIER as OP_CARRIER,
                OP_CARRIER_FL_NUM,
                ORIGIN,
                DEST,
                null as CRS_DEP_TIME,
                DEP_TIME,
                DEP_DELAY,
                TAXI_OUT,
                WHEELS_OFF,
                WHEELS_ON,
                TAXI_IN,
                null as CRS_ARR_TIME,
                ARR_TIME,
                ARR_DELAY,
                null as CANCELLED,
                null as CANCELLATION_CODE,
                null as DIVERTED,
                null as CRS_ELAPSED_TIME,
                null as ACTUAL_ELAPSED_TIME,
                AIR_TIME,
                DISTANCE,
                CARRIER_DELAY,
                WEATHER_DELAY,
                NAS_DELAY,
                SECURITY_DELAY,
                LATE_AIRCRAFT_DELAY
            from {{ source('raw_flight_data', flight_year) }}
        {% endset %}
    {% endif %}
    {% do queries.append(query) %}
{% endfor %}

{{ queries | join(" union all ") }}
