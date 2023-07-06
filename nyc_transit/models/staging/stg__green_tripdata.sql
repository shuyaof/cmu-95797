-- Create a list of staging commands to translate SQL commandes that modifies data type and column name 

with source as (
    select * from {{ source('main','green_tripdata') }}

),
renamed as (
    select 
        VendorID as vendor_id,
        lpep_pickup_datetime as meter_engaged_dt,
        lpep_dropoff_datetime as meter_disengaged_dt,
        cast(store_and_fwd_flag as boolean) as store_fwd_flag,
        cast(RatecodeID as int) as rate_code_id,
        PUlocationID as pick_up_loc_id,
        DOlocationID as drop_off_loc_id,
        Passenger_count as passenger_count,
        trip_distance,
        fare_amount,
        extra as extra_amount,
        mta_tax,
        tip_amount,
        tolls_amount,
        --ehail_fee drop null column
        improvement_surcharge,
        total_amount,
        cast(payment_type as int) as payment_type,
        cast(trip_type as int) as trip_type,
        congestion_surcharge,
        filename
    from source
)

select * from renamed
