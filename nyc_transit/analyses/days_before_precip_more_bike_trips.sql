-- determine if days immediately preceding precipitation or snow had more bike trips on average than the days with precipitation or snow.

with bike_weather as 
      (
       SELECT date, sum(trips) as trips,precipitation, snowfall, 
       CASE WHEN (precipitation > 0 OR snowfall > 0) THEN 1 ELSE 0 END AS is_bad_weather
FROM {{ ref('stg__daily_citi_bike_trip_counts_and_weather') }}
group by all),

lagged_data AS (
    SELECT date, trips, is_bad_weather, 
        lead(is_bad_weather) OVER (ORDER BY date) AS next_day_weather
    FROM bike_weather)

SELECT AVG(CASE WHEN next_day_weather = 1 THEN trips ELSE NULL END) AS avg_trips_on_previous_bad_weather_days,
       AVG(CASE WHEN is_bad_weather = 1 THEN trips ELSE NULL END) AS avg_trips_on_bad_weather_days
FROM lagged_data
