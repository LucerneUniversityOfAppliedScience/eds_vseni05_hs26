# Ported from the Databricks recipe (src_weather/01_silver_weather_at_flights.py) – the pure pandas logic, without the Spark/pipeline part.
# Settings come from enrichment/config.py (CONF) instead of the pipeline configuration.
# Silver: weather where the aircraft are – at their altitude. Part of the flight pipeline, but with its own rhythm:
# `pipelines.trigger.interval` refreshes this table only every weather.interval (default 15 minutes), while the
# positions keep streaming every few seconds. (A separate pipeline is not possible in Free Edition: only one active
# pipeline per pipeline type.)
#
# 1. take the current aircraft positions from the flight pipeline (gold.current_positions)
# 2. snap each position to a grid cell (weather.grid_degrees, default 1°) and its altitude to the nearest pressure
#    level – weather models describe the upper air on pressure levels: 850 hPa ≈ 5,000 ft … 200 hPa ≈ 39,000 ft
# 3. one Open-Meteo call for all occupied cells (several locations per request), wind, temperature and relative
#    humidity on six pressure levels plus the weather at the ground (humidity feeds the contrail model in 03)
# Open-Meteo is free for non-commercial use (10,000 calls/day, 5,000/hour, 600/minute; attribution: "Weather data by
# Open-Meteo.com", CC BY 4.0). Requests with more than 10 variables count fractionally as several calls: we ask for
# 31 variables (7 at the ground + 4 x 6 levels) = 3.1 calls per location (before humidity: 24 = 2.4). Budget:
# cells x 3.1 x refreshes/day, e.g. 15 cells (Switzerland) x 3.1 x 96 (every 15 min) = ~4,500 calls/day – but
# weather.max_cells = 100 would be ~30,000/day, so for big regions raise weather.interval (e.g. 60 minutes = ~7,400).
import requests
import pandas as pd
from enrichment.config import CONF

CATALOG = CONF.get("flights.catalog", "opensky")
SILVER = CONF.get("flights.silver_schema", "silver")
GOLD = CONF.get("flights.gold_schema", "gold")
# focus region (flights.focus_region, empty = flights.region): the heavy analyses only run inside it, while the
# pipeline may collect the whole world (region = world) – e.g. collect world, analyse europe or middle_east
FOCUS_REGIONS = {"switzerland": (45.6, 48.0, 5.8, 10.7), "alps": (43.5, 49.0, 5.0, 16.5), "east": (36.0, 48.0, 26.0, 50.0),
                 "europe": (35.0, 72.0, -25.0, 45.0), "middle_east": (12.0, 42.0, 25.0, 63.0),
                 "north_america": (14.0, 72.0, -170.0, -50.0), "asia": (-10.0, 60.0, 60.0, 150.0),
                 "world": (-90.0, 90.0, -180.0, 180.0)}
FOCUS = (CONF.get("flights.focus_region", "") or CONF.get("flights.region", "switzerland")).lower()
FOCUS_BBOX = FOCUS_REGIONS.get(FOCUS) or tuple(
    float(x) for x in (CONF.get("flights.focus_bbox", "") or CONF.get("flights.bbox", "45.6,48.0,5.8,10.7")).split(","))
FOCUS_SQL = (f"latitude BETWEEN {FOCUS_BBOX[0]} AND {FOCUS_BBOX[1]} "
             f"AND longitude BETWEEN {FOCUS_BBOX[2]} AND {FOCUS_BBOX[3]}")
GRID = float(CONF.get("weather.grid_degrees", "1.0"))
WORLD = CONF.get("flights.region", "switzerland").lower() == "world"
# Open-Meteo free tier: 10,000 calls/day, 31 variables = 3.1 calls per cell. world: 120 cells once an hour ≈ 8,900/day
# (deploy world with --var weather_grid_degrees=3 so that one cell covers more aircraft)
MAX_CELLS = int(CONF.get("weather.max_cells" + ("_world" if WORLD else ""), "120" if WORLD else "100"))
LEVELS = [850, 700, 500, 300, 250, 200]                      # hPa
SURFACE = ["temperature_2m", "wind_speed_10m", "wind_direction_10m", "cloud_cover", "visibility", "weather_code"]
UPPER = [f"{v}_{lvl}hPa" for lvl in LEVELS
         for v in ("wind_speed", "wind_direction", "temperature", "relative_humidity")]
HUMIDITY_2M = "relative_humidity_2m"                         # surface row's level_rh_pct (not a column of its own)
URL = "https://api.open-meteo.com/v1/forecast"
# One fixed model with a documented humidity definition: DWD ICON, relative humidity with respect to liquid water
# (DWD ICON Database Reference v2.5.6). Open-Meteo's default ("best match") mixes models per location – ECMWF IFS,
# for example, defines it over ice below -23 °C – and the contrail module (contrails.rh_reference = water) needs
# to know which one it gets. ICON-EU covers Europe; the world feed needs ICON global.
MODEL = CONF.get("weather.model", "icon_global" if WORLD else "icon_eu")
HEADERS = {"User-Agent": "databricks-cookbook-flight-tracking/1.0 (teaching demo)"}
COLUMNS = ["cell_lat", "cell_lon", "observed_at"] + SURFACE + [
    "level_hpa", "level_wind_kmh", "level_wind_dir", "level_temp_c", "level_rh_pct"]   # = order of the table schema


def fetch(cells):
    """Current weather for a list of (lat, lon) grid cells – Open-Meteo accepts comma-separated coordinates.
    One row per cell and level (level_hpa 0 = ground). A failed request is skipped (printed), never raised."""
    rows = []
    for i in range(0, len(cells), 50):                       # 50 locations per request
        chunk = cells[i:i + 50]
        try:
            r = requests.get(URL, timeout=30, headers=HEADERS, params={
                "latitude": ",".join(str(c[0]) for c in chunk), "longitude": ",".join(str(c[1]) for c in chunk),
                "current": ",".join(SURFACE + [HUMIDITY_2M] + UPPER), "wind_speed_unit": "kmh", "timezone": "UTC",
                "models": MODEL})
            r.raise_for_status()
            data = r.json()
        except Exception as e:                               # the pipeline must not fail because of the weather API
            print(f"Open-Meteo request for {len(chunk)} cells failed: {e}")
            continue
        for (lat, lon), loc in zip(chunk, data if isinstance(data, list) else [data]):
            cur = loc["current"]
            base = {"cell_lat": lat, "cell_lon": lon, "observed_at": pd.Timestamp(cur["time"], tz="UTC")}
            base.update({k: cur.get(k) for k in SURFACE})
            rows.append({**base, "level_hpa": 0, "level_wind_kmh": cur.get("wind_speed_10m"),
                         "level_wind_dir": cur.get("wind_direction_10m"), "level_temp_c": cur.get("temperature_2m"),
                         "level_rh_pct": cur.get(HUMIDITY_2M)})
            for lvl in LEVELS:
                rows.append({**base, "level_hpa": lvl, "level_wind_kmh": cur.get(f"wind_speed_{lvl}hPa"),
                             "level_wind_dir": cur.get(f"wind_direction_{lvl}hPa"),
                             "level_temp_c": cur.get(f"temperature_{lvl}hPa"),
                             "level_rh_pct": cur.get(f"relative_humidity_{lvl}hPa")})
    return pd.DataFrame(rows, columns=COLUMNS)


def busiest_cells(positions, grid=GRID, max_cells=MAX_CELLS):
    """Grid cells to fetch: cells with many aircraft above 20,000 ft first (cruise winds, contrails), then all
    others by aircraft count. Big regions (world: thousands of cells) only get the top max_cells."""
    df = positions.dropna(subset=["latitude", "longitude"])
    cell = pd.Series([(round(round(la / grid) * grid, 2), round(round(lo / grid) * grid, 2))
                      for la, lo in zip(df["latitude"], df["longitude"])], index=df.index, dtype=object)
    high = pd.to_numeric(df.get("altitude_ft"), errors="coerce").fillna(0) >= 20000 if "altitude_ft" in df else False
    score = pd.DataFrame({"cell": cell, "high": high}).groupby("cell")["high"].agg(["sum", "size"])
    return score.sort_values(["sum", "size"], ascending=False).index[:max_cells].tolist()
