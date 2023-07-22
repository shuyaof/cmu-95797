with trips_renamed as 
(
    --union all will use the column name from the first select, and make all column names consistent 
    select 'fhv' as type, pickup_datetime, dropoff_datetime, pulocationid, dolocationid
    from {{ ref('stg__fhv_tripdata') }}
        union all 
    select 'fhvhv' as type, pickup_datetime, dropoff_datetime, pulocationid, dolocationid
    from {{ ref('stg__fhvhv_tripdata') }}
        union all 
    select 'green' as type, lpep_pickup_datetime as pickup_datetime , lpep_dropoff_datetime as dropoff_datetime, pulocationid, dolocationid
    from {{ ref('stg__green_tripdata') }}
        union all 
    select 'yellow' as type, tpep_pickup_datetime as pickup_datetime, tpep_dropoff_datetime as dropoff_datetime, pulocationid, dolocationid
    FROM {{ ref('stg__yellow_tripdata') }}
)

select 
    type, 
    pickup_datetime,
    dropoff_datetime,
    datediff('minute', pickup_datetime, dropoff_datetime) as duration_min,
    datediff('second', pickup_datetime, dropoff_datetime) as duration_sec,
    pulocationid,
    dolocationid
from trips_renamed