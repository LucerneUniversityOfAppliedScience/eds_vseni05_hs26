# Workshop data

The data files are **not stored in Git** (≈ 260 MB). They are published as `workshop_data.zip` in the
GitHub release [`data-v1`](https://github.com/LucerneUniversityOfAppliedScience/eds_vseni05_hs26/releases/tag/data-v1).
The setup job (`databricks bundle run setup_data`) downloads the ZIP and extracts it into the volume
`/Volumes/workspace/mobility_raw/landing/`. You do not need to download anything yourself.

| Folder | Content | Rows / size | Source & license |
|---|---|---|---|
| `mobility/bike_pedestrian_counts/` | Automatic bike & pedestrian counts, 15-minute values, 2009–2026 | 15.8 M rows, 61 MB Parquet | [Open Data Zurich](https://data.stadt-zuerich.ch/dataset/ted_taz_verkehrszaehlungen_werte_fussgaenger_velo), CC0 |
| `mobility/bike_pedestrian_stations/` | Bike & pedestrian counting stations (name, directions, WGS84 coordinates) | 175 stations | [Open Data Zurich](https://data.stadt-zuerich.ch/dataset/geo_standorte_der_automatischen_fuss__und_velozaehlungen), CC0 |
| `mobility/motorized_traffic_counts/` | Motorized traffic counts (cars, trucks, …), hourly values, 2012–2026 | 23.4 M rows, 73 MB Parquet | [Open Data Zurich](https://data.stadt-zuerich.ch/dataset/sid_dav_verkehrszaehlung_miv_od2031), CC0 |
| `mobility/weather/` | Hourly weather for Zurich (temperature, precipitation, snow, wind, clouds), 2009–2026 | 156 k rows | [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api), CC BY 4.0 |
| `sample_data/` | Sample files for the basic exercises (CSV, JSON, XML, XLSX, YAML, Parquet, PDF, TXT) | small | Open Data Basel/Zurich, Kaggle-style teaching datasets |
| `adventureworks/` | AdventureWorks sample database (11 tables) as CSV | ≈ 230 k rows | Microsoft sample database |

## For instructors: rebuild the ZIP

```bash
./data/build_data_zip.sh
gh release create data-v1 dist/workshop_data.zip --title "Workshop data v1" --notes "Open data for the workshop"
```
