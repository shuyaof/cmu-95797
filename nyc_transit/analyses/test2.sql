-- with bike_weather as 
--       (
--        SELECT date, sum(trips) as trips,precipitation, snowfall, 
--        CASE WHEN (precipitation > 0 OR snowfall > 0) THEN 1 ELSE 0 END AS is_bad_weather
-- FROM {{ ref('stg__daily_citi_bike_trip_counts_and_weather') }}
-- group by all),

-- lagged_data AS (
--     SELECT date, trips, is_bad_weather, 
--         lead(is_bad_weather,1) OVER (ORDER BY date) AS previous_day_weather
--     FROM bike_weather)

-- select * from lagged_data
-- limit 10


WITH bike_trips AS (
    SELECT at.date, SUM(at.trips) as trips
    FROM {{ ref('mart__fact_all_trips_daily') }} at
    WHERE at.type = 'bike'
    GROUP BY at.date
),

weather AS (
    SELECT w.date, w.prcp, w.snow, 
        CASE WHEN (w.prcp > 0 OR w.snow > 0) THEN 1 ELSE 0 END AS is_bad_weather
    FROM {{ ref('stg__central_park_weather') }} w
),

weather_and_bike_trips AS (
    SELECT w.date, w.is_bad_weather, COALESCE(b.trips, 0) as trips
    FROM weather w
    LEFT JOIN bike_trips b ON w.date = b.date
),

lagged_data AS (
    SELECT date, trips, is_bad_weather, 
        lead(is_bad_weather) OVER (ORDER BY date) AS previous_day_weather
    FROM weather_and_bike_trips
)

SELECT AVG(CASE WHEN previous_day_weather = 1 THEN trips ELSE NULL END) AS avg_trips_on_previous_bad_weather_days,
       AVG(CASE WHEN is_bad_weather = 1 THEN trips ELSE NULL END) AS avg_trips_on_bad_weather_days
FROM lagged_data;