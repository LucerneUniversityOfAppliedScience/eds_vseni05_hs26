"""The analyses as jobs against flights.duckdb: read inputs as pandas, run the ported function, write the result table
with a fixed schema. `run_due(con)` runs every job whose interval has passed – the app calls it in the background.

    import flightlab as fl
    from enrichment import jobs
    con = fl.connect()
    jobs.run_all(con)                 # everything once (reference data first)
    jobs.run_job(con, "fuel_burn")    # one job
"""
import re
import time
import traceback
from pathlib import Path

import pandas as pd

import flightlab as fl
from enrichment import (aircraft_types, contrails, countries, fir_traffic, firs, flights_in_sigmet, fuel_co2,
                        go_arounds, holdings, metar, navaid_passages, navaids, overflights, photos, pireps,
                        restricted_airspace, route_efficiency, routes, runway_in_use, runways, sigmets, squawk, taf,
                        weather)
from enrichment.config import CONF

CACHE = Path(__file__).resolve().parent.parent / "cache"
TYPES = {"STRING": "VARCHAR", "INT": "INTEGER", "BIGINT": "BIGINT", "DOUBLE": "DOUBLE", "BOOLEAN": "BOOLEAN",
         "TIMESTAMP": "TIMESTAMP", "DATE": "DATE", "FLOAT": "DOUBLE"}


def _columns(schema):
    """'a STRING, b DOUBLE' -> [('a', 'VARCHAR'), ('b', 'DOUBLE')] (Spark DDL of the ported modules)."""
    out = []
    for part in re.split(r",\s*(?![^<]*>)", schema.strip()):
        name, typ = part.split(None, 1)
        out.append((name, TYPES.get(typ.upper().split("<")[0], "VARCHAR")))
    return out


def write(con, table, df, schema):
    """Replace `table` with df, typed by the module's schema (missing columns -> NULL, extra columns dropped)."""
    cols = _columns(schema)
    df = (df if df is not None else pd.DataFrame()).reset_index(drop=True)
    data = pd.DataFrame({name: (df[name] if name in df else pd.Series([None] * len(df), dtype=object))
                         for name, _ in cols})
    for name, typ in cols:                                # pandas NA / mixed objects -> None for DuckDB
        if typ == "TIMESTAMP":                            # mixed naive / tz-aware values -> naive UTC
            data[name] = pd.to_datetime(data[name], utc=True, errors="coerce").dt.tz_localize(None)
        if typ in ("VARCHAR",):
            data[name] = data[name].map(lambda v: None if v is None or (isinstance(v, float) and v != v) else str(v))
    select = ", ".join(f'TRY_CAST("{n}" AS {t}) AS "{n}"' for n, t in cols)
    con.register("_job_df", data)
    try:
        con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT {select} FROM _job_df")
    finally:
        con.unregister("_job_df")
    return len(data)


def q(con, sql):
    return con.execute(sql).df()


def recent(con, where="TRUE", minutes=60, columns="*"):
    """Rows of silver.flights from the last `minutes` before the newest snapshot."""
    return q(con, f"""SELECT {columns} FROM silver.flights
                      WHERE {where} AND snapshot_time >= (SELECT MAX(snapshot_time) FROM silver.flights)
                            - INTERVAL {int(minutes)} MINUTE""")


def _region_bbox():
    return fl.REGIONS.get(CONF["flights.region"]) or fl.REGIONS["switzerland"]


# ------------------------------------------------------------------------------------------------ reference data
def job_aircraft_types(con):
    """OpenSky aircraft database (~600,000 airframes) – downloaded once a month into cache/ (parsed as parquet)."""
    CACHE.mkdir(exist_ok=True)
    parquet = CACHE / "aircraft_types.parquet"
    if not parquet.exists() or time.time() - parquet.stat().st_mtime > 30 * 86400:
        content, name = aircraft_types.download_aircraft_database()
        aircraft_types.parse_aircraft_database(content, name).to_parquet(parquet)
    con.execute(f"CREATE OR REPLACE TABLE silver.aircraft_types AS SELECT * FROM read_parquet('{parquet}')")
    return con.execute("SELECT COUNT(*) FROM silver.aircraft_types").fetchone()[0]


def job_runways(con):
    raw = pd.read_csv(runways.RUNWAYS_URL, keep_default_na=False, na_values=[""],
                      storage_options={"User-Agent": "live-flight-tracking-local (teaching)"})
    return write(con, "silver.runways", runways.build_runways(raw, q(con, "SELECT airport_icao FROM silver.airports")),
                 runways.SCHEMA)


def job_navaids(con):
    return write(con, "silver.navaids", navaids.build_navaids(navaids.fetch_navaids_csv(), navaids.FOCUS_BBOX),
                 navaids.SCHEMA)


def job_firs(con):
    return write(con, "silver.firs", firs.build_firs(firs.fetch_boundaries(), _region_bbox(), firs.fetch_names()),
                 firs.SCHEMA)


def job_countries(con):
    return write(con, "silver.countries", countries.build_countries(countries.fetch_countries(), _region_bbox()),
                 countries.SCHEMA)


def job_restricted_airspace(con):
    f = q(con, "SELECT fir_icao FROM silver.firs")
    return write(con, "silver.restricted_airspace",
                 restricted_airspace.build_restricted_airspace(pdf_firs=f if len(f) else None),
                 restricted_airspace.SCHEMA)


def job_restricted_fir_shapes(con):
    return write(con, "gold.restricted_fir_shapes",
                 fir_traffic.restricted_fir_shapes(q(con, "SELECT * FROM silver.restricted_airspace"),
                                                   q(con, "SELECT * FROM silver.firs")), fir_traffic.SHAPES_SCHEMA)


# ------------------------------------------------------------------------------------------------ weather & airports
def job_metar(con):
    ids = metar.stations_in_bbox(q(con, "SELECT airport_icao, latitude, longitude FROM silver.airports"),
                                 metar.FOCUS_BBOX)[:600]
    return write(con, "silver.metar", metar.parse_metars(metar.fetch_metars(ids, metar.FOCUS_BBOX)), metar.SCHEMA)


def job_taf(con):
    ids = taf.stations_in_bbox(q(con, "SELECT airport_icao, latitude, longitude, airport_type FROM silver.airports"),
                               taf.FOCUS_BBOX)[:600]
    return write(con, "silver.taf", taf.parse_tafs(taf.fetch_tafs(ids)), taf.SCHEMA)


def job_pireps(con):
    return write(con, "silver.pireps", pireps.fetch_and_parse_pireps(pireps.FOCUS_BBOX, "auto", 6.0, 5), pireps.SCHEMA)


def job_sigmets(con):
    return write(con, "silver.sigmets", sigmets.parse_sigmets(sigmets.fetch_isigmets(), _region_bbox()),
                 sigmets.SCHEMA)


def job_weather(con):
    pos = q(con, f"SELECT latitude, longitude, altitude_ft FROM gold.current_positions WHERE {weather.FOCUS_SQL}")
    schema = ("cell_lat DOUBLE, cell_lon DOUBLE, observed_at TIMESTAMP, temperature_2m DOUBLE, wind_speed_10m DOUBLE, "
              "wind_direction_10m DOUBLE, cloud_cover DOUBLE, visibility DOUBLE, weather_code INT, level_hpa INT, "
              "level_wind_kmh DOUBLE, level_wind_dir DOUBLE, level_temp_c DOUBLE, level_rh_pct DOUBLE")
    return write(con, "silver.weather_at_flights", weather.fetch(weather.busiest_cells(pos, weather.GRID, weather.MAX_CELLS)),
                 schema)


def job_contrails(con):
    pos = q(con, f"""SELECT snapshot_time, icao24, callsign, airline_name, latitude, longitude, altitude_ft, altitude_m,
                            flight_phase, on_ground FROM gold.current_positions WHERE {contrails.FOCUS_SQL}""")
    wx = q(con, "SELECT cell_lat, cell_lon, level_hpa, level_temp_c, level_rh_pct, observed_at FROM silver.weather_at_flights")
    c = contrails.compute_contrails(pos, wx, contrails.GRID, contrails.ETA, contrails.MIN_ALT_FT, contrails.RH_REFERENCE)
    n = write(con, "gold.contrails", c, contrails.CONTRAILS_SCHEMA)
    write(con, "gold.contrails_summary", contrails.summarize_contrails(q(con, "SELECT * FROM gold.contrails"),
                                                                      contrails.MIN_ALT_FT), contrails.SUMMARY_SCHEMA)
    return n


def job_runway_in_use(con):
    fl_ = recent(con, f"{runway_in_use.FOCUS_SQL} AND NOT on_ground AND altitude_ft < 12000", runway_in_use.LOOKBACK,
                 "snapshot_time, icao24, callsign, latitude, longitude, on_ground, altitude_ft, heading, vertical_rate")
    if fl_.empty:
        return write(con, "gold.runway_in_use", None, runway_in_use.SCHEMA)
    end = pd.to_datetime(fl_["snapshot_time"]).max()
    mt = q(con, "SELECT * FROM silver.metar")
    matches = runway_in_use.match_runway_movements(fl_, q(con, "SELECT * FROM silver.airports"),
                                                   q(con, "SELECT * FROM silver.runways"), mt)
    return write(con, "gold.runway_in_use", runway_in_use.build_runway_in_use(
        matches, mt, end - pd.Timedelta(minutes=runway_in_use.LOOKBACK), end), runway_in_use.SCHEMA)


def job_go_arounds(con):
    fl_ = recent(con, f"{go_arounds.FOCUS_SQL} AND (on_ground OR altitude_ft < 10000)", go_arounds.LOOKBACK,
                 "snapshot_time, icao24, callsign, latitude, longitude, on_ground, altitude_ft, heading, vertical_rate, "
                 "category")
    return write(con, "gold.go_arounds", go_arounds.detect_go_arounds(fl_, q(con, "SELECT * FROM silver.airports"),
                                                                     q(con, "SELECT * FROM silver.metar")),
                 go_arounds.SCHEMA)


def job_holdings(con):
    fl_ = recent(con, f"{holdings.FOCUS_SQL} AND NOT on_ground AND altitude_ft BETWEEN 1000 AND 30000",
                 holdings.LOOKBACK, "snapshot_time, icao24, callsign, latitude, longitude, on_ground, altitude_ft, "
                 "speed_kmh, heading, category")
    return write(con, "gold.holding_patterns", holdings.detect_holdings(fl_, q(con, "SELECT * FROM silver.airports")),
                 holdings.SCHEMA)


def job_navaid_passages(con):
    nav = q(con, "SELECT * FROM silver.navaids")
    pos = recent(con, f"{navaid_passages.FOCUS_SQL} AND NOT on_ground", navaid_passages.LOOKBACK,
                 "snapshot_time, icao24, callsign, latitude, longitude, altitude_ft, heading, speed_kmh")
    p = (navaid_passages.navaid_passages(pos, nav, navaid_passages.RADIUS_KM,
                                         latest_time=pd.to_datetime(pos["snapshot_time"]).max())
         if len(nav) and len(pos) else None)
    n = write(con, "gold.navaid_passages", p, navaid_passages.PASSAGES_SCHEMA)
    write(con, "gold.navaid_traffic", navaid_passages.navaid_traffic(q(con, "SELECT * FROM gold.navaid_passages"),
                                                                     nav, navaid_passages.LOOKBACK)
          if len(nav) else None, navaid_passages.TRAFFIC_SCHEMA)
    return n


# ------------------------------------------------------------------------------------------------ aircraft
def job_fuel_burn(con):
    df = q(con, f"""SELECT f.snapshot_time, f.icao24, f.callsign, f.airline_icao, f.on_ground, f.altitude_ft,
                           f.speed_kmh, f.vertical_rate, f.flight_phase, t.typecode
                    FROM silver.flights f LEFT JOIN silver.aircraft_types t USING (icao24)
                    WHERE f.snapshot_time >= (SELECT MAX(snapshot_time) FROM silver.flights)
                          - INTERVAL {fuel_co2.WINDOW_MINUTES} MINUTE""")
    burn = fuel_co2.estimate_fuel_burn(df.drop(columns="typecode"), df[["icao24", "typecode"]].drop_duplicates("icao24"))
    n = write(con, "gold.fuel_burn", burn, fuel_co2.BURN_SCHEMA)
    write(con, "gold.co2_per_minute", fuel_co2.co2_per_minute(q(con, "SELECT * FROM gold.fuel_burn"),
                                                             q(con, "SELECT airline_icao, airline_name FROM silver.airlines")),
          fuel_co2.MINUTE_SCHEMA)
    return n


def job_flight_routes(con, keep_hours=24, max_new=200):
    """adsbdb routes: callsigns over Switzerland first, then a sample of the rest; only callsigns not looked up in the
    last `keep_hours` are asked (the table grows over time – and adsbdb gets far fewer requests)."""
    pos = q(con, "SELECT callsign, on_ground, latitude, longitude FROM gold.current_positions")
    lat0, lat1, lon0, lon1 = fl.REGIONS["switzerland"]
    swiss = pos[pos.latitude.between(lat0, lat1) & pos.longitude.between(lon0, lon1)]
    wanted = routes.airline_callsigns(swiss, 400) + routes.airline_callsigns(pos, 300)
    try:
        known = q(con, f"SELECT * FROM silver.flight_routes "
                       f"WHERE looked_up_at >= current_timestamp - INTERVAL {int(keep_hours)} HOUR")
    except Exception:                                    # first run: no table yet
        known = pd.DataFrame(columns=[c for c, _ in _columns(routes.ROUTE_SCHEMA)])
    done = set(known["callsign"])
    new = [c for c in dict.fromkeys(wanted) if c not in done][:max_new]
    fresh = routes.lookup_routes(new) if new else None
    both = pd.concat([d for d in (known, fresh) if d is not None and len(d)], ignore_index=True) \
        if (len(known) or (fresh is not None and len(fresh))) else None
    if both is not None:
        both = both.drop_duplicates("callsign", keep="last")
    return write(con, "silver.flight_routes", both, routes.ROUTE_SCHEMA)


def job_flights_with_routes(con):
    return write(con, "gold.flights_with_routes",
                 routes.build_flights_with_routes(q(con, "SELECT * FROM gold.current_positions"),
                                                  q(con, "SELECT * FROM silver.flight_routes"), _region_bbox()),
                 routes.GOLD_SCHEMA)


def job_aircraft_photos(con):
    if not photos.CONTACT:
        return write(con, "silver.aircraft_photos", None, photos.SCHEMA)     # planespotters needs a contact
    appr = q(con, "SELECT icao24, callsign, airport_icao, distance_km FROM gold.airport_approaches")
    return write(con, "silver.aircraft_photos", photos.lookup_photos(photos.photo_candidates(appr), photos.CONTACT),
                 photos.SCHEMA)


def job_route_efficiency(con):
    lookback = route_efficiency.LOOKBACK_MIN
    fl_ = q(con, f"""
        SELECT icao24, trim(callsign) AS callsign, arg_max(snapshot_time, snapshot_time) AS snapshot_time,
               arg_max(airline_icao, snapshot_time) AS airline_icao, arg_max(latitude, snapshot_time) AS latitude,
               arg_max(longitude, snapshot_time) AS longitude, arg_max(altitude_ft, snapshot_time) AS altitude_ft,
               arg_max(vertical_rate, snapshot_time) AS vertical_rate, arg_max(on_ground, snapshot_time) AS on_ground
        FROM silver.flights
        WHERE NOT on_ground AND snapshot_time >= (SELECT MAX(snapshot_time) FROM silver.flights) - INTERVAL {lookback} MINUTE
          AND trim(callsign) IN (SELECT callsign FROM silver.flight_routes)
        GROUP BY icao24, trim(callsign), date_trunc('minute', snapshot_time)""")
    df = route_efficiency.build_route_efficiency(
        fl_, q(con, "SELECT * FROM silver.flight_routes"),
        q(con, "SELECT icao24, flight_phase, fuel_kg_per_h, speed_kt FROM gold.fuel_burn"),
        q(con, "SELECT icao24, typecode FROM silver.aircraft_types"),
        q(con, "SELECT airline_icao, airline_name FROM silver.airlines"),
        q(con, "SELECT airport_icao, latitude, longitude, airport_type FROM silver.airports"))
    n = write(con, "gold.route_efficiency", df, route_efficiency.SCHEMA)
    tracks = q(con, f"""
        SELECT icao24, trim(callsign) AS callsign, arg_max(snapshot_time, snapshot_time) AS snapshot_time,
               arg_max(latitude, snapshot_time) AS latitude, arg_max(longitude, snapshot_time) AS longitude,
               arg_max(origin_country, snapshot_time) AS origin_country
        FROM silver.flights
        WHERE NOT on_ground AND snapshot_time >= (SELECT MAX(snapshot_time) FROM silver.flights) - INTERVAL {lookback} MINUTE
          AND (icao24, trim(callsign)) IN (SELECT icao24, callsign FROM gold.route_efficiency)
        GROUP BY icao24, trim(callsign), floor(epoch(snapshot_time) / {60 * fir_traffic.TRACK_SAMPLE_MIN})""")
    re_, shapes = q(con, "SELECT * FROM gold.route_efficiency"), q(con, "SELECT * FROM gold.restricted_fir_shapes")
    write(con, "gold.route_restrictions", fir_traffic.route_restrictions(re_, shapes, tracks)
          if len(re_) and len(shapes) else None, fir_traffic.ROUTE_SCHEMA)
    return n


# ------------------------------------------------------------------------------------------------ geo & alerts
def job_squawk(con):
    s = recent(con, "squawk IN (" + ", ".join(f"'{c}'" for c in squawk.SQUAWKS) + ")", 60 * squawk.LOOKBACK_HOURS,
               "snapshot_time, icao24, callsign, squawk, latitude, longitude, altitude_ft, on_ground, origin_country")
    return write(con, "gold.squawk_alerts", squawk.squawk_alerts(s), squawk.SCHEMA)


def job_flights_in_sigmet(con):
    act = q(con, "SELECT * FROM silver.sigmets WHERE current_timestamp BETWEEN valid_from AND valid_to "
                 "AND polygon_json IS NOT NULL")
    return write(con, "gold.flights_in_sigmet",
                 flights_in_sigmet.flights_in_sigmet(q(con, "SELECT * FROM gold.current_positions"), act),
                 flights_in_sigmet.SCHEMA)


def job_overflights(con):
    ctry = q(con, "SELECT * FROM silver.countries")
    n = write(con, "gold.current_overflights",
              overflights.current_overflights(q(con, "SELECT * FROM gold.current_positions"), ctry),
              overflights.CURRENT_SCHEMA)
    minutes = q(con, f"""
        SELECT icao24, date_trunc('minute', snapshot_time) AS minute, arg_max(snapshot_time, snapshot_time) AS snapshot_time,
               arg_max(callsign, snapshot_time) AS callsign, arg_max(latitude, snapshot_time) AS latitude,
               arg_max(longitude, snapshot_time) AS longitude, arg_max(altitude_ft, snapshot_time) AS altitude_ft,
               arg_max(on_ground, snapshot_time) AS on_ground
        FROM silver.flights
        WHERE snapshot_time >= (SELECT MAX(snapshot_time) FROM silver.flights) - INTERVAL {overflights.LOOKBACK_MINUTES} MINUTE
        GROUP BY ALL""")
    write(con, "silver.flights_country_per_minute", overflights.flights_with_country(minutes, ctry),
          overflights.PER_MINUTE_SCHEMA)
    per_min = q(con, "SELECT * FROM silver.flights_country_per_minute")
    write(con, "gold.country_traffic_per_minute", overflights.country_traffic_per_minute(per_min),
          overflights.TRAFFIC_SCHEMA)
    write(con, "gold.border_crossings", overflights.border_crossings(per_min), overflights.CROSSINGS_SCHEMA)
    return n


def job_fir_traffic(con):
    cur = fir_traffic.current_firs(q(con, "SELECT * FROM gold.current_positions"), q(con, "SELECT * FROM silver.firs"),
                                   q(con, "SELECT * FROM gold.restricted_fir_shapes"))
    n = write(con, "gold.current_firs", cur, fir_traffic.CURRENT_SCHEMA)
    cur = q(con, "SELECT * FROM gold.current_firs")
    write(con, "gold.flights_per_fir", fir_traffic.flights_per_fir(
        cur, q(con, "SELECT fir_icao, in_region, label_lat, label_lon FROM silver.firs"),
        q(con, "SELECT fir_icao, fir_name, country, status, severity FROM gold.restricted_fir_shapes")),
        fir_traffic.PER_FIR_SCHEMA)
    write(con, "gold.restricted_fir_traffic", fir_traffic.restricted_fir_traffic(cur),
          fir_traffic.RESTRICTED_TRAFFIC_SCHEMA)
    return n


def build_views(con):
    """The two SQL views of the Databricks recipe (aircraft_now, flights_with_weather) – after the tables exist."""
    con.execute("""
        CREATE OR REPLACE VIEW gold.aircraft_now AS
        SELECT p.snapshot_time, p.icao24, p.callsign, p.airline_icao, p.airline_name, p.origin_country, p.latitude,
               p.longitude, p.altitude_ft, p.speed_kmh, p.heading, p.flight_phase, p.on_ground,
               t.icao24 IS NOT NULL AS has_metadata, t.registration, t.typecode, t.manufacturer, t.model,
               t.icao_aircraft_class,
               COALESCE(t.operator, CASE WHEN p.airline_icao IS NOT NULL AND p.airline_name <> 'unknown / private'
                                         THEN p.airline_name END, t.owner) AS operator,
               COALESCE(t.operator_icao, p.airline_icao) AS operator_icao, t.built_year,
               year(current_date) - t.built_year AS age_years
        FROM gold.current_positions p LEFT JOIN silver.aircraft_types t USING (icao24)""")
    grid = float(weather.GRID)
    con.execute(f"""
        CREATE OR REPLACE VIEW gold.flights_with_weather AS
        WITH positioned AS (
            SELECT p.*, ROUND(ROUND(p.latitude / {grid}) * {grid}, 2) AS cell_lat,
                   ROUND(ROUND(p.longitude / {grid}) * {grid}, 2) AS cell_lon,
                   CASE WHEN p.on_ground OR p.altitude_ft < 3000 THEN 0 WHEN p.altitude_ft < 7500 THEN 850
                        WHEN p.altitude_ft < 14000 THEN 700 WHEN p.altitude_ft < 24000 THEN 500
                        WHEN p.altitude_ft < 32000 THEN 300 WHEN p.altitude_ft < 37000 THEN 250 ELSE 200 END AS level_hpa
            FROM gold.current_positions p)
        SELECT f.snapshot_time, f.icao24, f.callsign, f.airline_name, f.latitude, f.longitude, f.altitude_ft,
               f.speed_kmh, f.heading, f.flight_phase, f.level_hpa, w.level_wind_kmh AS wind_kmh,
               w.level_wind_dir AS wind_from_deg, w.level_temp_c AS outside_temp_c,
               ROUND(w.level_wind_kmh * COS(RADIANS(w.level_wind_dir - f.heading))) AS headwind_kmh,
               CASE WHEN w.level_wind_kmh * COS(RADIANS(w.level_wind_dir - f.heading)) > 10 THEN 'headwind'
                    WHEN w.level_wind_kmh * COS(RADIANS(w.level_wind_dir - f.heading)) < -10 THEN 'tailwind'
                    ELSE 'crosswind / calm' END AS wind_effect
        FROM positioned f LEFT JOIN silver.weather_at_flights w
          ON f.cell_lat = w.cell_lat AND f.cell_lon = w.cell_lon AND f.level_hpa = w.level_hpa""")


# name -> (function, interval in seconds). Order = dependency order.
JOBS = {
    "aircraft_types": (job_aircraft_types, 86400), "runways": (job_runways, 86400), "navaids": (job_navaids, 86400),
    "firs": (job_firs, 86400), "countries": (job_countries, 86400),
    "restricted_airspace": (job_restricted_airspace, 86400), "restricted_fir_shapes": (job_restricted_fir_shapes, 86400),
    "metar": (job_metar, 900), "taf": (job_taf, 1800), "pireps": (job_pireps, 1800), "sigmets": (job_sigmets, 900),
    "weather": (job_weather, 900), "flight_routes": (job_flight_routes, 600),
    "flights_with_routes": (job_flights_with_routes, 60), "fuel_burn": (job_fuel_burn, 120),
    "route_efficiency": (job_route_efficiency, 600), "aircraft_photos": (job_aircraft_photos, 1800),
    "contrails": (job_contrails, 300), "runway_in_use": (job_runway_in_use, 60), "go_arounds": (job_go_arounds, 120),
    "holdings": (job_holdings, 120), "navaid_passages": (job_navaid_passages, 120), "squawk": (job_squawk, 60),
    "flights_in_sigmet": (job_flights_in_sigmet, 60), "overflights": (job_overflights, 120),
    "fir_traffic": (job_fir_traffic, 60),
}
_last_run = {}


def run_job(con, name, log=print):
    fn, _ = JOBS[name]
    t0 = time.time()
    try:
        n = fn(con)
        log(f"{name}: {n} rows in {time.time() - t0:.1f} s")
        return True
    except Exception as e:                                # an enrichment never stops the others
        log(f"{name}: FAILED {type(e).__name__}: {str(e)[:200]}")
        log(traceback.format_exc(limit=3))
        return False
    finally:
        _last_run[name] = time.time()


def run_due(con, log=print):
    """Run every job whose interval has passed (first call: all). Call regularly, e.g. after each snapshot."""
    ran = False
    for name, (_, interval) in JOBS.items():
        if time.time() - _last_run.get(name, 0) >= interval:
            run_job(con, name, log)
            ran = True
    if ran:
        build_views(con)


def run_all(con, log=print):
    _last_run.clear()
    run_due(con, log)
