-- Create a list of staging commands to translate SQL commandes that modifies data type and column name 

with source as (
    select * from {{ source('main','central_park_weather') }}

),
renamed as (
    select 
        station,
        name,
        cast(date as date) as date,
        cast(awnd as double) as awnd,
        cast(prcp as double) as precipitation,
        cast(snow as double) as snow,
        cast(snwd as double) as snwd,
        cast(tmax as int) as temperature_max,
        cast(tmin as int) as temperature_min,
        filename
    from source
)

select * from renamed
