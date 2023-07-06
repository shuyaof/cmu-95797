-- Create a list of staging commands to translate SQL commandes that modifies data type and column name 

with source as (
    select * from {{ source('main','bike_data') }}

),
renamed as (
    select
        cast(coalesce(bikeid, ride_id) as int) as bike_id,
        rideable_type as ride_type,
        usertype as user_type,
        cast(gender as int) as gender,
        member_casual,
        cast('birth year' as int) as birth_year,
        cast(tripduration as int) as trip_duration,
        cast(coalesce(starttime, started_at) as timestamp) as started_at_ts,
        cast(coalesce(stoptime, ended_at) as timestamp) as end_at_ts,
        cast(coalesce('start station id', start_station_id) as int) as start_station_id,
        coalesce('start station name', start_station_name) as start_station_name,
        cast(coalesce('start station latitude', start_lat) as int) as start_station_lat,
        cast(coalesce('start station longitude', start_lng) as int) as start_station_lng,
        cast(coalesce('end station id', end_station_id) as int) as end_station_id,
        coalesce('end station name', end_station_name) as end_station_name,
        cast(coalesce('end station latitude', end_lat) as int) as end_station_lat,
        cast(coalesce('end station longitude', end_lng) as int) as end_station_lng,
        filename
    from source
)

select * from renamed
