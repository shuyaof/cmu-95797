
-- Find taxi trips which dont have a pick up location_id in the locaton table. 

select *
from {{ ref('mart__fact_all_taxi_trips') }}
where pulocationid is null
order by pickup_datetime