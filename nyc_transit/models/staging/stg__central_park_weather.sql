-- Create a list of staging commands to translate SQL commandes that modifies data type and column name 

with source as (
    select * from {{ source('main','central_park_weather') }}

),
renamed as (
    select 
        station,
        name,
        date::date as date,
        awnd::double as average_wind_speed,
        prcp::double as precipitation,
        snow::double as snow,
        snwd::double as snow_depth,
        tmax::integer as temperature_max,
        tmin::integer as temperature_min,
        filename
    from source
)

select * from renamed
