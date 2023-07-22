with all_trips as 
( 
    --union all will use the column and make column names consistent
    select 'bike' as type, started_at_ts, ended_at_ts from {{ ref('stg__bike_data') }}
        union all
    select 'fhv' as type, pickup_datetime as started_at_ts, dropoff_datetime as ended_at_ts from {{ ref('stg__fhv_tripdata') }}
        union all
    select 'fhvhv' as type, pickup_datetime as started_at_ts, dropoff_datetime as ended_at_ts from {{ ref('stg__fhvhv_tripdata') }}
        union all
    select 'green' as type, lpep_pickup_datetime as started_at_ts, lpep_dropoff_datetime as ended_at_ts from {{ ref('stg__green_tripdata') }}
        union all
    select 'yellow' as type, tpep_pickup_datetime as started_at_ts, tpep_dropoff_datetime as ended_at_ts from {{ ref('stg__yellow_tripdata') }}
)

select 
    type, 
    started_at_ts,
    ended_at_ts,
    datediff('minute', started_at_ts, ended_at_ts) as duration_min,
    datediff('second', started_at_ts, ended_at_ts) as duration_sec
from all_trips