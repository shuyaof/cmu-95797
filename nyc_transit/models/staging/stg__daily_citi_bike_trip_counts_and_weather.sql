-- Create a list of staging commands to translate SQL commandes that modifies data type and column name 

with source as (
    select * from {{ source('main','daily_citi_bike_trip_counts_and_weather') }}

),
renamed as (
    select 
        cast(date as date) as date,
        cast(trips as int) as trips,
        cast(precipitation as double) as precipitation,
        cast(snow_depth as double) as snow_depth,
        cast(snowfall as double) as snowfall,
        cast(max_temperature as double) as max_temperature,
        cast(min_temperature as double) as min_temperature,
        cast(average_wind_speed as double) as average_wind_speed,
        cast(dow as int) as dow,
        cast(year as int) as year,
        cast(month as int) as month,
        cast(holiday as boolean) as holiday,
        cast(stations_in_service as int) as stations_in_service,
        cast(weekday as boolean) as weekday,
        cast(weekday_non_holiday as boolean) as weekday_non_holiday,
        filename
    from source
)

select * from renamed
