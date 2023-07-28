-- Write a query to calculate the 7 day moving average precipitation for every day in the weather data.

select date,
       prcp,
       AVG(prcp) OVER (ORDER BY date ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) 
       AS moving_avg_prcp
from {{ ref('stg__central_park_weather') }}
order by date
offset 3


