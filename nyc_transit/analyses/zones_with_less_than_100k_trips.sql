--Find all the Zones where there are less than 100000 trips.
 select zone,
        sum(taxi_trips) as taxi_trips
 from {{ ref('mart__fact_trips_by_borough')}}
 group by zone
 having sum(taxi_trips) < 100000
