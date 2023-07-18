with source as (

    select * from {{ source('main', 'fhv_tripdata') }}

),

renamed as (

    select
        trim(upper(dispatching_base_num)) as dispatching_base_num, --capitalize dispatching_base_num and remove the leading and trailing characters of a string in that column
        pickup_datetime,
        dropoff_datetime,
        pulocationid,
        dolocationid,
        --sr_flag, always null so chuck it
        affiliated_base_number,
        filename

    from source

)

select * from renamed


