
--Calculate the number of trips and average duration by borough and zone

select borough,
       zone,
       count(pulocationid) as taxi_trips,
       avg(duration_min) as average_duration_by_min,
       avg(duration_sec) as average_duration_by_sec
from {{ ref('mart__fact_trips_by_borough')}}
-- where zone = 'Midtown East' insanity check
group by borough,zone
order by borough,zone