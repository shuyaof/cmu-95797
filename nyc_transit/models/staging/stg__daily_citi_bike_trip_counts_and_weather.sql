-- Create a list of staging commands to translate SQL commandes that modifies data type and column name 

with source as (
    select * from {{ source('main','daily_citi_bike_trip_counts_and_weather') }}

),
renamed as (
    select 
        date::date as date,
        try_cast(trips as integer) as trips,
        try_cast(precipitation as double) as precipitation,
        try_cast(snow_depth as double) as snow_depth,
        try_cast(snowfall as double) as snowfall,
        try_cast(max_temperature as double) as max_temperature,
        try_cast(min_temperature as double) as min_temperature,
        try_cast(average_wind_speed as double) as average_wind_speed,
        try_cast(dow as integer) as dow,
        try_cast(year as integer) as year,
        try_cast(month as integer) as month,
        holiday::boolean as holiday,
        try_cast(stations_in_service as integer) as stations_in_service,
        weekday::boolean as weekday,
        weekday_non_holiday::boolean as weekday_non_holiday,
        filename
    from source
)

select * from renamed
