-- =====================================================================
-- BRONZE: load the raw files from the volume 1:1.
-- Streaming tables pick up only NEW files on each run (incremental load).
-- ${landing_path} is set in resources/solution_pipeline.yml (configuration).
-- =====================================================================

CREATE OR REFRESH STREAMING TABLE bike_counts
COMMENT "Raw 15-minute bike and pedestrian counts, City of Zurich (Open Data, CC0)"
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${landing_path}/mobility/bike_pedestrian_counts/', format => 'parquet');

CREATE OR REFRESH STREAMING TABLE car_counts
COMMENT "Raw hourly motorized traffic counts, City of Zurich (Open Data, CC0)"
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${landing_path}/mobility/motorized_traffic_counts/', format => 'parquet');

CREATE OR REFRESH STREAMING TABLE bike_stations_raw
COMMENT "Raw bike & pedestrian counting stations (Open Data Zurich, CC0)"
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${landing_path}/mobility/bike_pedestrian_stations/',
                       format => 'csv', header => true, inferColumnTypes => false);

CREATE OR REFRESH STREAMING TABLE weather_raw
COMMENT "Raw hourly weather for Zurich (Open-Meteo, CC BY 4.0)"
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${landing_path}/mobility/weather/',
                       format => 'csv', header => true, inferColumnTypes => false);
