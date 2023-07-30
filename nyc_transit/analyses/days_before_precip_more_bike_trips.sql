-- determine if days immediately preceding precipitation or snow had more bike trips on average than the days with precipitation or snow.

WITH bike_trips AS (
    SELECT 
    date_trunc('day', started_at_ts)::date as date,
	count(*) as trips
	from {{ ref('mart__fact_all_bike_trips') }}
group by all
),

weather AS (
    SELECT w.date, w.prcp, w.snow, 
        CASE WHEN (w.prcp > 0 OR w.snow > 0) THEN 1 ELSE 0 END AS is_bad_weather
    FROM {{ ref('stg__central_park_weather') }} w
),

weather_and_bike_trips AS (
    SELECT w.date, w.is_bad_weather, COALESCE(b.trips, 0) as trips
    FROM weather w
    inner JOIN bike_trips b ON w.date = b.date
),

lead_data AS (
    SELECT date, trips, is_bad_weather, 
        lead(is_bad_weather) OVER (ORDER BY date) AS lead_day_weather
    FROM weather_and_bike_trips
)


SELECT AVG(CASE WHEN lead_day_weather = 1 THEN trips ELSE NULL END) AS avg_trips_on_previous_bad_weather_days,
       AVG(CASE WHEN is_bad_weather = 1 THEN trips ELSE NULL END) AS avg_trips_on_bad_weather_days
FROM lead_data