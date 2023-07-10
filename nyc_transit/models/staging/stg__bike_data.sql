-- Create a list of staging commands to translate SQL commandes that modifies data type and column name 

with source as (
    select * from {{ source('main','bike_data') }}

),

renamed as (
    select
        bikeid::integer as bike_id, 
        ride_id,
        rideable_type as ride_type,
        usertype as user_type,
        gender::integer as gender,
        member_casual,
        "birth year"::integer as birth_year,
        coalesce(tripduration,0)::integer as trip_duration,
        coalesce(starttime, started_at) ::timestamp as started_at_ts,
        coalesce(stoptime, ended_at)::timestamp as end_at_ts,
        coalesce("start station id", start_station_id) as start_station_id,
        coalesce("start station name", start_station_name) as start_station_name,
        coalesce("start station latitude", start_lat)::double as start_station_lat,
        coalesce("start station longitude", start_lng)::double as start_station_lng,
        coalesce("end station id", end_station_id) as end_station_id,
        coalesce("end station name", end_station_name) as end_station_name,
        coalesce("end station latitude", end_lat)::double as end_station_lat,
        coalesce("end station longitude", end_lng)::double as end_station_lng,
        filename
    from source
)

select * from renamed