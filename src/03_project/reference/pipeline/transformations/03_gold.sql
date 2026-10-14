-- =====================================================================
-- GOLD: analysis-ready tables for the dashboard (same as project step 4)
-- =====================================================================

CREATE OR REFRESH MATERIALIZED VIEW daily_traffic
COMMENT "One row per day: bikes, pedestrians, vehicles (totals and per station/site) and weather"
AS WITH weather AS (
  SELECT to_date(ts_hour) AS date,
         ROUND(AVG(temperature_c), 1) AS temp_mean_c, MAX(temperature_c) AS temp_max_c,
         ROUND(SUM(precipitation_mm), 1) AS precipitation_mm, ROUND(SUM(snowfall_cm), 1) AS snowfall_cm
  FROM weather_hourly GROUP BY 1
),
bikes AS (
  SELECT date, SUM(bikes) AS bikes, COUNT(DISTINCT station_id) AS bike_stations
  FROM bike_counts_hourly WHERE has_bike_data GROUP BY date
),
peds AS (
  SELECT date, SUM(pedestrians) AS pedestrians, COUNT(DISTINCT station_id) AS pedestrian_stations
  FROM bike_counts_hourly WHERE has_pedestrian_data GROUP BY date
),
cars AS (
  SELECT date, SUM(vehicles) AS vehicles, COUNT(DISTINCT site_id) AS car_sites
  FROM car_counts_hourly GROUP BY date
)
SELECT w.date, year(w.date) AS year, month(w.date) AS month,
       date_format(w.date, 'EEEE') AS weekday, dayofweek(w.date) IN (1, 7) AS is_weekend,
       b.bikes, b.bike_stations, ROUND(b.bikes / b.bike_stations) AS bikes_per_station,
       p.pedestrians, p.pedestrian_stations, ROUND(p.pedestrians / p.pedestrian_stations) AS pedestrians_per_station,
       c.vehicles, c.car_sites, ROUND(c.vehicles / c.car_sites) AS vehicles_per_site,
       w.temp_mean_c, w.temp_max_c, w.precipitation_mm, w.snowfall_cm,
       w.precipitation_mm >= 1.0 AS is_rainy_day
FROM weather w
LEFT JOIN bikes b USING (date)
LEFT JOIN peds  p USING (date)
LEFT JOIN cars  c USING (date);

CREATE OR REFRESH MATERIALIZED VIEW yearly_trend
COMMENT "Average daily traffic per station/site, complete years only"
AS SELECT year,
          ROUND(AVG(bikes_per_station))       AS avg_daily_bikes_per_station,
          ROUND(AVG(pedestrians_per_station)) AS avg_daily_pedestrians_per_station,
          ROUND(AVG(vehicles_per_site))       AS avg_daily_vehicles_per_site,
          ROUND(AVG(bike_stations))           AS avg_active_bike_stations,
          ROUND(AVG(car_sites))               AS avg_active_car_sites,
          SUM(bikes)                          AS bikes_total,
          SUM(vehicles)                       AS vehicles_total
FROM daily_traffic
WHERE year BETWEEN 2011 AND 2025
GROUP BY year;

CREATE OR REFRESH MATERIALIZED VIEW location_map
COMMENT "Average daily volume per counting location in 2025 (bikes and cars)"
AS SELECT 'Bike' AS mode, s.station_name AS location_name, s.lat, s.lon,
          ROUND(SUM(h.bikes) / COUNT(DISTINCT h.date)) AS avg_daily_count
   FROM bike_counts_hourly h JOIN bike_stations s USING (station_id)
   WHERE h.has_bike_data AND h.year = 2025
   GROUP BY ALL
   UNION ALL
   SELECT 'Car', c.site_name, c.lat, c.lon,
          ROUND(SUM(h.vehicles) / COUNT(DISTINCT h.date))
   FROM car_counts_hourly h JOIN car_sites c USING (site_id)
   WHERE h.year = 2025
   GROUP BY ALL;

CREATE OR REFRESH MATERIALIZED VIEW hourly_profile
COMMENT "Average count per station/site and hour in 2025, weekday vs weekend"
AS SELECT 'Bike' AS mode, IF(is_weekend, 'Weekend', 'Weekday') AS day_type, hour,
          ROUND(AVG(bikes), 1) AS avg_per_location
   FROM bike_counts_hourly WHERE has_bike_data AND year = 2025
   GROUP BY ALL
   UNION ALL
   SELECT 'Car', IF(is_weekend, 'Weekend', 'Weekday'), hour, ROUND(AVG(vehicles), 1)
   FROM car_counts_hourly WHERE year = 2025
   GROUP BY ALL;
