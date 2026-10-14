-- =====================================================================
-- SILVER: typed, harmonized to one grain (hour), validated.
-- Expectations (CONSTRAINT ... EXPECT) are data quality rules; their
-- statistics are shown in the pipeline UI.
-- =====================================================================

CREATE OR REFRESH MATERIALIZED VIEW bike_stations (
  CONSTRAINT valid_station_id EXPECT (station_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Bike & pedestrian counting stations with WGS84 coordinates"
AS SELECT CAST(station_id AS INT) AS station_id,
          bezeichnung             AS station_name,
          velo = '1'              AS counts_bikes,
          fuss = '1'              AS counts_pedestrians,
          richtung_in             AS direction_in,
          richtung_out            AS direction_out,
          CAST(lon AS DOUBLE)     AS lon,
          CAST(lat AS DOUBLE)     AS lat
FROM bike_stations_raw;

CREATE OR REFRESH MATERIALIZED VIEW car_sites
COMMENT "Motorized traffic counting sites, LV95 converted to WGS84 (swisstopo approximation)"
AS WITH s AS (
  SELECT ZSID AS site_id, MAX(ZSName) AS site_name, MAX(Achse) AS street,
         (AVG(EKoord) - 2600000) / 1000000 AS y,
         (AVG(NKoord) - 1200000) / 1000000 AS x
  FROM car_counts
  GROUP BY ZSID
)
SELECT site_id, site_name, street,
       (2.6779094 + 4.728982 * y + 0.791484 * y * x + 0.1306 * y * x * x - 0.0436 * y * y * y) * 100 / 36 AS lon,
       (16.9023892 + 3.238272 * x - 0.270978 * y * y - 0.002528 * x * x
        - 0.0447 * y * y * x - 0.0140 * x * x * x) * 100 / 36                                           AS lat
FROM s;

CREATE OR REFRESH MATERIALIZED VIEW weather_hourly (
  CONSTRAINT valid_timestamp EXPECT (ts_hour IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Hourly weather for Zurich, typed"
AS SELECT to_timestamp(time, "yyyy-MM-dd'T'HH:mm") AS ts_hour,
          CAST(temperature_c    AS DOUBLE)          AS temperature_c,
          CAST(precipitation_mm AS DOUBLE)          AS precipitation_mm,
          CAST(snowfall_cm      AS DOUBLE)          AS snowfall_cm,
          CAST(wind_speed_kmh   AS DOUBLE)          AS wind_speed_kmh,
          CAST(cloud_cover_pct  AS INT)             AS cloud_cover_pct
FROM weather_raw;

CREATE OR REFRESH MATERIALIZED VIEW bike_counts_hourly (
  CONSTRAINT valid_hour    EXPECT (ts_hour IS NOT NULL)    ON VIOLATION DROP ROW,
  CONSTRAINT valid_station EXPECT (station_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Bike & pedestrian counts per station and hour"
AS WITH typed AS (
  SELECT CAST(FK_STANDORT AS INT)                                               AS station_id,
         date_trunc('HOUR', to_timestamp(DATUM, "yyyy-MM-dd'T'HH:mm"))          AS ts_hour,
         coalesce(VELO_IN, 0)                                                   AS bikes_in,
         coalesce(VELO_OUT, 0)                                                  AS bikes_out,
         coalesce(FUSS_IN, 0) + coalesce(FUSS_OUT, 0)                           AS pedestrians,
         VELO_IN IS NOT NULL OR VELO_OUT IS NOT NULL                            AS has_bike_data,
         FUSS_IN IS NOT NULL OR FUSS_OUT IS NOT NULL                            AS has_pedestrian_data
  FROM bike_counts
)
SELECT station_id, ts_hour,
       SUM(bikes_in) AS bikes_in, SUM(bikes_out) AS bikes_out, SUM(pedestrians) AS pedestrians,
       MAX(has_bike_data) AS has_bike_data, MAX(has_pedestrian_data) AS has_pedestrian_data,
       SUM(bikes_in) + SUM(bikes_out) AS bikes,
       to_date(ts_hour) AS date, year(ts_hour) AS year, hour(ts_hour) AS hour,
       dayofweek(ts_hour) IN (1, 7) AS is_weekend
FROM typed
GROUP BY station_id, ts_hour;

CREATE OR REFRESH MATERIALIZED VIEW car_counts_hourly (
  CONSTRAINT valid_hour EXPECT (ts_hour IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Motorized traffic per counting site and hour (measured or imputed values only)"
AS WITH typed AS (
  SELECT ZSID AS site_id,
         to_timestamp(substring(MessungDatZeit, 1, 19), "yyyy-MM-dd'T'HH:mm:ss") AS ts_hour,
         AnzFahrzeuge AS vehicles
  FROM car_counts
  WHERE AnzFahrzeugeStatus IN ('Gemessen', 'Imputiert') AND AnzFahrzeuge IS NOT NULL
)
SELECT site_id, ts_hour, SUM(vehicles) AS vehicles,
       to_date(ts_hour) AS date, year(ts_hour) AS year, hour(ts_hour) AS hour,
       dayofweek(ts_hour) IN (1, 7) AS is_weekend
FROM typed
GROUP BY site_id, ts_hour;
