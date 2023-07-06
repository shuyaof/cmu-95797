-- Create a list of staging commands to translate SQL commandes that modifies data type and column name 

with source as (
    select * from {{ source('main','fhv_tripdata') }}

),
renamed as (
    select 
        dispatching_base_num as dispatch_base_num,
        pickup_datetime as pick_up_dt,
        dropOff_datetime as drop_off_dt,
        PUlocationID as pick_up_loc_id,
        DOlocationID as drop_off_loc_id,
        --SR_Flag drop null column
        Affiliated_base_number as af_base_num,
        filename
    from source
)

select * from renamed
