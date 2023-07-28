--Find the average time between taxi pick ups per zone
--● Use lead or lag to find the next trip per zone for each record
--● then find the time difference between the pick up time for the current record and the next
--● then use this result to calculate the average time between pick ups per zone.

with pickup_zone as (select 
       zone,
       datediff('second',pickup_datetime,lead(pickup_datetime) 
       over (partition by zone order by pickup_datetime)) as difference
from {{ ref('mart__fact_all_taxi_trips') }} t
join {{ ref('mart__dim_locations') }} l on l.locationid = t.pulocationid
-- where zone = 'Westerleigh'
)

select pickup_zone.zone as zone,
       avg(pickup_zone.difference) as average_time_between_pickups
from pickup_zone
group by zone
 