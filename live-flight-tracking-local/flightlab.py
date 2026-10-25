"""flightlab – the finished building blocks of notebooks 01–06, so that later notebooks, collector.py and the app can
use them (a bug in your own step 2 should not block you in step 7).

    import flightlab as fl
    con = fl.connect()                              # flights.duckdb next to this file
    data, credits = fl.fetch_states(fl.REGIONS["switzerland"])
    fl.store_snapshot(con, data, "switzerland", source="api", credits_remaining=credits)
    fl.build_silver(con); fl.build_gold(con)

Layers (DuckDB schemas in one file):
    bronze.snapshots  one row per OpenSky answer (raw JSON), key (region, time) – the merge key for live + backfill
    bronze.states     one row per aircraft and snapshot, exactly as OpenSky sends it
    silver.flights    cleaned positions in feet / km/h, airline code + name, aircraft category, flight phase
                      – key (icao24, snapshot_time)
    silver.airlines, silver.aircraft_categories   lookups joined into silver.flights (OpenFlights, OpenSky codes)
    silver.airports   reference data for the gold views (OurAirports)
    gold.*            views for the map and the analyses
"""
import gzip
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

HERE = Path(__file__).resolve().parent
DB_PATH = HERE / "flights.duckdb"
CACHE = HERE / "cache"

STATES_URL = "https://opensky-network.org/api/states/all"
TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

# bounding boxes (lamin, lamax, lomin, lomax) – the bigger the box, the more credits per call
REGIONS = {
    "switzerland": (45.6, 48.0, 5.8, 10.7),     # 1 credit (< 25 square degrees)
    "alps": (43.5, 49.0, 5.0, 16.5),            # 2 credits
    "europe": (35.0, 72.0, -25.0, 45.0),        # 4 credits
    "world": None,                              # 4 credits, ~13,000 aircraft
}

# the 18 fields of one OpenSky state vector (with extended=1), in this order
STATE_FIELDS = ["icao24", "callsign", "origin_country", "time_position", "last_contact", "longitude", "latitude",
                "baro_altitude", "on_ground", "velocity", "true_track", "vertical_rate", "sensors", "geo_altitude",
                "squawk", "spi", "position_source", "category"]


# ------------------------------------------------------------------------------------------------ 01 first message
def load_env(path=HERE / ".env"):
    """KEY=value lines of .env (if it exists) – environment variables win."""
    env = {}
    if Path(path).exists():
        for line in Path(path).read_text().splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    env.update({k: v for k, v in os.environ.items() if k.startswith(("OPENSKY_", "R2_"))})
    return env


def get_token(client_id, client_secret):
    """OAuth2 client-credentials token of an OpenSky account (valid ~30 minutes)."""
    r = requests.post(TOKEN_URL, data={"grant_type": "client_credentials", "client_id": client_id,
                                       "client_secret": client_secret}, timeout=15)
    r.raise_for_status()
    return r.json()["access_token"]


def fetch_states(bbox=None, token=None, timeout=20):
    """One call of GET /api/states/all. Returns (answer as dict, remaining credits or None)."""
    params = {"extended": 1}
    if bbox:
        params.update(dict(zip(["lamin", "lamax", "lomin", "lomax"], bbox)))
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    r = requests.get(STATES_URL, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    remaining = r.headers.get("X-Rate-Limit-Remaining")
    return r.json(), int(remaining) if remaining is not None else None


def parse_state(vector):
    """One state vector (a list) -> dict with names, plus units for humans."""
    s = dict(zip(STATE_FIELDS, vector))
    s["callsign"] = (s["callsign"] or "").strip() or None
    alt, vel = s["baro_altitude"], s["velocity"]
    s["altitude_ft"] = round(alt * 3.28084) if alt is not None else None
    s["speed_kmh"] = round(vel * 3.6) if vel is not None else None
    return s


def flight_phase(on_ground, vertical_rate):
    if on_ground:
        return "on ground"
    if vertical_rate is not None and vertical_rate > 2.5:
        return "climbing"
    if vertical_rate is not None and vertical_rate < -2.5:
        return "descending"
    return "cruise"


# ------------------------------------------------------------------------------------------------ 02 bronze in DuckDB
def connect(path=DB_PATH, read_only=False):
    """DuckDB connection with the three schemas. Only ONE process may write – notebooks that only look at the data
    while collector.py or the app is running open the file with read_only=True."""
    import duckdb
    con = duckdb.connect(str(path), read_only=read_only)
    con.execute("SET TimeZone = 'UTC'")              # all timestamps in UTC, independent of the laptop's time zone
    if not read_only:
        init_db(con)
    return con


class NoFlightData(RuntimeError):
    """flights.duckdb is missing or has no silver.flights rows yet."""


def snapshot(path=DB_PATH):
    """A copy of flights.duckdb to analyse while collector.py or the app is writing (DuckDB locks the file for every
    other process – even for reading). The copy is yours: you may also create tables in it."""
    import shutil

    import duckdb
    CACHE.mkdir(exist_ok=True)
    for old in CACHE.glob("snapshot_*.duckdb"):           # copies of notebooks that are no longer running
        try:
            os.kill(int(old.stem.split("_")[1]), 0)
        except (ValueError, ProcessLookupError):
            for f in (old, Path(f"{old}.wal")):
                f.unlink(missing_ok=True)
        except PermissionError:
            pass
    # one copy per notebook process – several notebooks can be open at the same time
    dst = CACHE / os.getenv("FLIGHTLAB_SNAPSHOT", f"snapshot_{os.getpid()}.duckdb")

    for suffix in ("", ".wal"):
        src, out = Path(f"{path}{suffix}"), Path(f"{dst}{suffix}")
        out.unlink(missing_ok=True)
        if src.exists():
            shutil.copy2(src, out)
    con = duckdb.connect(str(dst))
    con.execute("SET TimeZone = 'UTC'")
    init_db(con)
    try:
        flights = con.execute("SELECT COUNT(*) FROM silver.flights").fetchone()[0]
    except duckdb.CatalogException:
        flights = 0
    if not flights:
        con.close()
        raise NoFlightData(
            f"No flight data in {Path(path).name} yet. Notebooks 07–20 analyse the real data – load it once, then "
            f"rerun this notebook:\n"
            f"  • tools/88_start_everything.py → Start (history + live map), or\n"
            f"  • in a terminal: uv run collector.py --backfill-hours 24 --no-live")
    return con


# tables the app's background jobs (enrichment/jobs.py) write – and what each one needs first
ENRICHMENT = {
    "aircraft_types": ("silver.aircraft_types", []),
    "runways": ("silver.runways", []),
    "metar": ("silver.metar", []),
    "flight_routes": ("silver.flight_routes", []),
    "flights_with_routes": ("gold.flights_with_routes", ["flight_routes"]),
    "fuel_burn": ("gold.fuel_burn", ["aircraft_types"]),
    "route_efficiency": ("gold.route_efficiency", ["flight_routes", "aircraft_types", "fuel_burn"]),
}


def ensure(con, *names):
    """Notebooks 07–20 use tables the app computes in the background. Data loaded with collector.py only has
    bronze/silver/gold – then compute the missing ones here, in the notebook's copy (prerequisites first)."""
    from enrichment import jobs

    def exists(table):
        schema, name = table.split(".")
        return con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
                           [schema, name]).fetchone()[0] > 0

    done = set()

    def run(name):
        table, needs = ENRICHMENT[name]
        for need in needs:
            run(need)
        if name not in done and not exists(table):
            print(f"{table} is not there (the app computes it in the background) – computing it now ...")
            jobs.run_job(con, name, log=lambda *_: None)
        done.add(name)

    for name in names:
        run(name)


def practice(step):
    """The playground of notebooks 02–06: one file per notebook (practice_02.duckdb, practice_03.duckdb, …), so they
    can be open at the same time – DuckDB lets only one process write a file. When a notebook opens, it takes over
    the newest earlier step if that changed after its own file: 03 continues with what you stored in 02, and so on."""
    import shutil

    import duckdb

    def changed(path):
        return max((q.stat().st_mtime for q in (path, Path(f"{path}.wal")) if q.exists()), default=0)

    dst = HERE / f"practice_{step:02d}.duckdb"
    src = next((q for q in (HERE / f"practice_{k:02d}.duckdb" for k in range(step - 1, 1, -1)) if q.exists()), None)
    if src is not None and changed(src) > changed(dst):
        for suffix in ("", ".wal"):
            Path(f"{dst}{suffix}").unlink(missing_ok=True)
            if Path(f"{src}{suffix}").exists():
                shutil.copy2(f"{src}{suffix}", f"{dst}{suffix}")
    con = duckdb.connect(str(dst))
    con.execute("SET TimeZone = 'UTC'")
    return con


def init_db(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.snapshots (
            region VARCHAR NOT NULL,             -- bounding box name: switzerland, world, ...
            time BIGINT NOT NULL,                -- OpenSky time of the snapshot (unix seconds)
            fetched_at TIMESTAMP,                -- when we got it
            source VARCHAR,                      -- api | r2
            credits_remaining INTEGER,
            aircraft INTEGER,
            payload JSON,                        -- the raw answer – everything can be rebuilt from here
            PRIMARY KEY (region, time))""")
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.states (
            region VARCHAR NOT NULL, time BIGINT NOT NULL, icao24 VARCHAR NOT NULL,
            callsign VARCHAR, origin_country VARCHAR, time_position BIGINT, last_contact BIGINT,
            longitude DOUBLE, latitude DOUBLE, baro_altitude DOUBLE, on_ground BOOLEAN, velocity DOUBLE,
            true_track DOUBLE, vertical_rate DOUBLE, geo_altitude DOUBLE, squawk VARCHAR, category INTEGER,
            PRIMARY KEY (region, time, icao24))""")


STATE_COLUMNS = ["icao24", "callsign", "origin_country", "time_position", "last_contact", "longitude", "latitude",
                 "baro_altitude", "on_ground", "velocity", "true_track", "vertical_rate", "geo_altitude", "squawk",
                 "category"]


def store_snapshot(con, data, region, source="api", credits_remaining=None):
    """One OpenSky answer -> bronze. Idempotent: a snapshot (region, time) that is already there is skipped, so live
    collection and backfill can overlap. Returns True if it was new."""
    states = data.get("states") or []
    inserted = con.execute("""
        INSERT INTO bronze.snapshots VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING RETURNING time""",
        [region, int(data["time"]), datetime.now(timezone.utc).replace(tzinfo=None), source, credits_remaining,
         len(states), json.dumps(data)]).fetchall()
    if not inserted:
        return False
    rows = []
    for v in states:
        s = dict(zip(STATE_FIELDS, v))
        rows.append([region, int(data["time"])] + [s.get(c) for c in STATE_COLUMNS])
    if rows:
        df = pd.DataFrame(rows, columns=["region", "time"] + STATE_COLUMNS)
        df["callsign"] = df["callsign"].str.strip()
        con.execute("INSERT INTO bronze.states SELECT * FROM df ON CONFLICT DO NOTHING")
    return True


# ------------------------------------------------------------------------------------------------ 03 collector
def retry_after(err):
    """Seconds OpenSky asks us to wait after a 429 (header X-Rate-Limit-Retry-After-Seconds), or None if missing."""
    response = getattr(err, "response", None)
    value = response.headers.get("X-Rate-Limit-Retry-After-Seconds") if response is not None else None
    try:
        return max(0.0, float(value)) if value is not None else None
    except ValueError:
        return None


def collect(con, region="switzerland", interval=35, minutes=10, min_credits=20, token=None, log=print):
    """Poll OpenSky every `interval` seconds for `minutes` (0 = forever). Stops when the credits run low, waits and
    retries on 429 (too many requests) and network errors. After a 429 it waits as long as OpenSky says in
    X-Rate-Limit-Retry-After-Seconds; without that header it doubles the wait (exponential backoff, max 600 s).
    Returns the number of new snapshots."""
    bbox, new, end = REGIONS.get(region), 0, time.time() + minutes * 60
    backoff = interval
    while minutes == 0 or time.time() < end:
        t0 = time.time()
        try:
            data, credits = fetch_states(bbox, token)
            if store_snapshot(con, data, region, "api", credits):
                new += 1
                log(f"{datetime.now():%H:%M:%S} {len(data.get('states') or [])} aircraft, credits left {credits}")
            backoff = interval
            if credits is not None and credits < min_credits:
                log(f"only {credits} credits left – stopping")
                break
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                wait = retry_after(e)
                if wait is None:
                    backoff = min(backoff * 2, 600)
                    wait = backoff
                if minutes and time.time() + wait > end:
                    log(f"429 too many requests – waiting {wait:.0f} s would outlast the run – stopping")
                    break
                log(f"429 too many requests – waiting {wait:.0f} s")
                time.sleep(wait)
                continue
            raise
        except (requests.ConnectionError, requests.Timeout) as e:
            log(f"network problem ({type(e).__name__}) – retry in {interval} s")
        time.sleep(max(0.0, interval - (time.time() - t0)))
    return new


# ------------------------------------------------------------------------------------------------ 04 backfill from R2
def r2_client(env=None):
    """boto3 S3 client for the class bucket + bucket name, from R2_ENDPOINT / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY."""
    import boto3
    env = env or load_env()
    endpoint, bucket = env["R2_ENDPOINT"].rstrip("/").rsplit("/", 1)
    s3 = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=env["R2_ACCESS_KEY_ID"],
                      aws_secret_access_key=env["R2_SECRET_ACCESS_KEY"], region_name="auto")
    return s3, bucket


def hour_prefixes(region, since, until):
    """The partition folders region/year=/month=/day=/hour=/ between two datetimes (UTC) – only these are listed."""
    t = since.replace(minute=0, second=0, microsecond=0)
    out = []
    while t <= until:
        out.append(f"{region}/year={t:%Y}/month={t:%m}/day={t:%d}/hour={t:%H}/")
        t += timedelta(hours=1)
    return out


def list_r2_keys(s3, bucket, region, since, until=None):
    until = until or datetime.now(timezone.utc).replace(tzinfo=None)
    keys = []
    for prefix in hour_prefixes(region, since, until):
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith((".json", ".json.gz"))]
    return sorted(keys)


def read_r2_snapshot(s3, bucket, key):
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(gzip.decompress(body) if key.endswith(".gz") else body)


def backfill(con, region="switzerland", since=None, until=None, env=None, log=print):
    """Load the class data of `region` from R2 into bronze – old snapshots that are already there are skipped
    (same merge key as the live collector). Returns (files read, new snapshots)."""
    s3, bucket = r2_client(env)
    since = since or datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=24)
    keys = list_r2_keys(s3, bucket, region, since, until)
    have = {t for (t,) in con.execute("SELECT time FROM bronze.snapshots WHERE region = ?", [region]).fetchall()}
    new = 0
    for i, key in enumerate(keys, 1):
        stamp = key.rsplit("_", 1)[-1].split(".")[0]          # <region>_<unix time>.json.gz – skip without download
        if stamp.isdigit() and int(stamp) in have:
            continue
        data = read_r2_snapshot(s3, bucket, key)
        new += store_snapshot(con, data, region, "r2", data.get("credits_remaining"))
        if i % 200 == 0:
            log(f"{i}/{len(keys)} files, {new} new snapshots")
    log(f"backfill {region}: {len(keys)} files, {new} new snapshots")
    return len(keys), new


def ensure_sample_data(con, hours=2, min_snapshots=20):
    """For notebooks you jump into: if bronze is (almost) empty, fill it – backfill of the last `hours` from R2 when
    a key is in .env, otherwise one live API call."""
    if con.execute("SELECT COUNT(*) FROM bronze.snapshots").fetchone()[0] >= min_snapshots:
        return
    env = load_env()
    if env.get("R2_ACCESS_KEY_ID"):
        backfill(con, "switzerland", since=datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=hours),
                 env=env, log=lambda *_: None)
    else:
        data, credits = fetch_states(REGIONS["switzerland"])
        store_snapshot(con, data, "switzerland", "api", credits)


# ------------------------------------------------------------------------------------------------ 05 silver
def build_silver(con):
    """bronze.states -> silver.flights, incrementally PER SNAPSHOT (not by time!): a backfill delivers OLD snapshots
    after newer ones are already in silver – a "newer than MAX(snapshot_time)" watermark would silently drop them.
    The lookups (airline name, aircraft category) are joined here, in silver – gold only reads them."""
    if not {"airlines", "aircraft_categories"} <= _silver_tables(con):
        load_airlines(con)
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver.flights (
            snapshot_time TIMESTAMP NOT NULL, icao24 VARCHAR NOT NULL, region VARCHAR, callsign VARCHAR,
            airline_icao VARCHAR, origin_country VARCHAR, latitude DOUBLE, longitude DOUBLE, on_ground BOOLEAN,
            altitude_m DOUBLE, altitude_ft DOUBLE, speed_kmh DOUBLE, heading DOUBLE, vertical_rate DOUBLE,
            flight_phase VARCHAR, squawk VARCHAR, category INTEGER, airline_name VARCHAR, aircraft_category VARCHAR,
            PRIMARY KEY (icao24, snapshot_time))""")
    _add_lookup_columns(con)
    con.execute("CREATE TABLE IF NOT EXISTS silver.processed_snapshots (region VARCHAR, time BIGINT, "
                "PRIMARY KEY (region, time))")
    todo = con.execute("""
        SELECT b.region, b.time FROM bronze.snapshots b
        ANTI JOIN silver.processed_snapshots p USING (region, time)""").df()
    if todo.empty:
        return 0
    con.execute("""
        INSERT INTO silver.flights
        SELECT
            make_timestamp(s.time * 1000000)                        AS snapshot_time,   -- UTC, whatever TimeZone says
            s.icao24, s.region,
            NULLIF(s.callsign, '')                                  AS callsign,
            CASE WHEN regexp_matches(s.callsign, '^[A-Z]{3}[0-9]') THEN substr(s.callsign, 1, 3) END AS airline_icao,
            s.origin_country, s.latitude, s.longitude, s.on_ground,
            ROUND(s.baro_altitude)                                  AS altitude_m,
            ROUND(s.baro_altitude * 3.28084)                        AS altitude_ft,
            ROUND(s.velocity * 3.6)                                 AS speed_kmh,
            ROUND(s.true_track)                                     AS heading,
            s.vertical_rate,
            CASE WHEN s.on_ground THEN 'on ground'
                 WHEN s.vertical_rate > 2.5 THEN 'climbing'
                 WHEN s.vertical_rate < -2.5 THEN 'descending'
                 ELSE 'cruise' END                                  AS flight_phase,
            s.squawk, s.category,
            a.airline_name, c.description                           AS aircraft_category
        FROM bronze.states s
        SEMI JOIN todo USING (region, time)
        LEFT JOIN silver.airlines a                                 -- one row per code, so no duplicates
               ON a.airline_icao = substr(s.callsign, 1, 3) AND regexp_matches(s.callsign, '^[A-Z]{3}[0-9]')
        LEFT JOIN silver.aircraft_categories c ON c.category = s.category
        WHERE s.latitude IS NOT NULL AND s.longitude IS NOT NULL
          AND (s.baro_altitude IS NULL OR s.baro_altitude BETWEEN -500 AND 20000)
        ON CONFLICT DO NOTHING      -- the same aircraft at the same time in two regions (switzerland + world)""")
    con.execute("INSERT INTO silver.processed_snapshots SELECT * FROM todo ON CONFLICT DO NOTHING")
    return len(todo)


def _silver_tables(con):
    return {r[0] for r in con.execute("SELECT table_name FROM information_schema.tables "
                                      "WHERE table_schema = 'silver'").fetchall()}


def _add_lookup_columns(con):
    """silver.flights from before the lookups moved into silver: add the two columns and fill them once."""
    cols = {r[0] for r in con.execute("SELECT column_name FROM information_schema.columns "
                                      "WHERE table_schema = 'silver' AND table_name = 'flights'").fetchall()}
    if {"airline_name", "aircraft_category"} <= cols:
        return
    con.execute("ALTER TABLE silver.flights ADD COLUMN IF NOT EXISTS airline_name VARCHAR")
    con.execute("ALTER TABLE silver.flights ADD COLUMN IF NOT EXISTS aircraft_category VARCHAR")
    con.execute("""UPDATE silver.flights f SET airline_name = a.airline_name
                   FROM silver.airlines a WHERE a.airline_icao = f.airline_icao""")
    con.execute("""UPDATE silver.flights f SET aircraft_category = c.description
                   FROM silver.aircraft_categories c WHERE c.category = f.category""")


# ------------------------------------------------------------------------------------------------ 06 reference + gold
AIRPORTS_URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"
AIRLINES_URL = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat"


def cached(url, name, max_age_days=30):
    """Download url once into cache/<name> (again after max_age_days) and return the path."""
    CACHE.mkdir(exist_ok=True)
    path = CACHE / name
    if not path.exists() or time.time() - path.stat().st_mtime > max_age_days * 86400:
        r = requests.get(url, timeout=120, headers={"User-Agent": "live-flight-tracking-local (teaching)"})
        r.raise_for_status()
        path.write_bytes(r.content)
    return path


# OpenSky's aircraft category codes (field 17 of a state vector, with extended=1), from
# https://openskynetwork.github.io/opensky-api/rest.html
AIRCRAFT_CATEGORIES = {
    0: "No information at all", 1: "No ADS-B Emitter Category Information", 2: "Light (< 15500 lbs)",
    3: "Small (15500 to 75000 lbs)", 4: "Large (75000 to 300000 lbs)", 5: "High Vortex Large (aircraft such as B-757)",
    6: "Heavy (> 300000 lbs)", 7: "High Performance (> 5g acceleration and 400 kts)", 8: "Rotorcraft",
    9: "Glider / sailplane", 10: "Lighter-than-air", 11: "Parachutist / Skydiver",
    12: "Ultralight / hang-glider / paraglider", 13: "Reserved", 14: "Unmanned Aerial Vehicle",
    15: "Space / Trans-atmospheric vehicle", 16: "Surface Vehicle – Emergency Vehicle",
    17: "Surface Vehicle – Service Vehicle", 18: "Point Obstacle (includes tethered balloons)",
    19: "Cluster Obstacle", 20: "Line Obstacle"}


def load_airlines(con):
    """The lookups silver.flights joins: airlines (ICAO code -> name) and OpenSky's aircraft categories."""
    cols = ["id", "name", "alias", "iata", "icao", "callsign", "country", "active"]
    al = pd.read_csv(cached(AIRLINES_URL, "airlines.dat"), header=None, names=cols, na_values=["\\N"],
                     keep_default_na=False)
    al = al[al["icao"].str.fullmatch(r"[A-Z]{3}", na=False)].sort_values("active", ascending=False, kind="stable")
    airlines = al.drop_duplicates("icao").rename(columns={"icao": "airline_icao", "name": "airline_name",
                                                          "iata": "airline_iata"})[
        ["airline_icao", "airline_iata", "airline_name", "callsign", "country"]]
    categories = pd.DataFrame({"category": list(AIRCRAFT_CATEGORIES), "description": list(AIRCRAFT_CATEGORIES.values())})
    con.execute("CREATE OR REPLACE TABLE silver.airlines AS SELECT * FROM airlines")
    con.execute("CREATE OR REPLACE TABLE silver.aircraft_categories AS SELECT * FROM categories")
    return len(airlines)


def load_airports(con):
    """Airports (large + medium) into silver – reference data for the gold views, not joined into silver.flights."""
    ap = pd.read_csv(cached(AIRPORTS_URL, "airports.csv"), keep_default_na=False, na_values=[""])
    ap = ap[ap["type"].isin(["large_airport", "medium_airport"])]
    airports = pd.DataFrame({
        "airport_icao": ap["ident"], "airport_iata": ap["iata_code"], "airport_name": ap["name"],
        "airport_type": ap["type"], "city": ap["municipality"], "country": ap["iso_country"],
        "latitude": ap["latitude_deg"], "longitude": ap["longitude_deg"], "elevation_ft": ap["elevation_ft"]})
    con.execute("CREATE OR REPLACE TABLE silver.airports AS SELECT * FROM airports")
    return len(airports)


def load_reference(con):
    """All reference data: airports, airlines, aircraft categories."""
    return load_airports(con), load_airlines(con)


def build_gold(con, window_minutes=8):
    """Views on silver – always current, nothing to refresh. window_minutes: how old a position may be to still count
    as 'current' (2 for one region, 8 when the world feed (every 6 min) is mixed in)."""
    con.execute(f"""
        CREATE OR REPLACE VIEW gold.current_positions AS
        WITH latest AS (SELECT MAX(snapshot_time) AS max_time FROM silver.flights)
        SELECT f.snapshot_time, f.icao24, f.callsign, f.airline_icao,
               COALESCE(f.airline_name, 'unknown / private') AS airline_name, f.origin_country,
               f.latitude, f.longitude, f.altitude_ft, f.altitude_m, f.speed_kmh, f.heading, f.flight_phase,
               f.on_ground, f.category, f.aircraft_category, f.region AS source_region,
               epoch(l.max_time) - epoch(f.snapshot_time) AS position_age_s
        FROM silver.flights f CROSS JOIN latest l
        WHERE f.snapshot_time >= l.max_time - INTERVAL {int(window_minutes)} MINUTE
        QUALIFY ROW_NUMBER() OVER (PARTITION BY f.icao24 ORDER BY f.snapshot_time DESC, f.region = 'world') = 1""")
    con.execute("""
        CREATE OR REPLACE VIEW gold.airport_approaches AS
        WITH candidates AS (
            SELECT p.*, ap.airport_icao, ap.airport_iata, ap.airport_name, ap.airport_type, ap.city, ap.country,
                   2 * 6371 * ASIN(SQRT(POWER(SIN(RADIANS(ap.latitude - p.latitude) / 2), 2)
                       + COS(RADIANS(p.latitude)) * COS(RADIANS(ap.latitude))
                         * POWER(SIN(RADIANS(ap.longitude - p.longitude) / 2), 2))) AS distance_km
            FROM gold.current_positions p
            JOIN silver.airports ap
              ON ap.latitude BETWEEN p.latitude - 0.5 AND p.latitude + 0.5
             AND ap.longitude BETWEEN p.longitude - 0.8 AND p.longitude + 0.8
            WHERE NOT p.on_ground AND p.altitude_m < 3000)
        SELECT *, CASE WHEN flight_phase = 'descending' THEN 'arriving'
                       WHEN flight_phase = 'climbing' THEN 'departing' ELSE 'nearby' END AS movement
        FROM candidates WHERE distance_km <= 40
        -- a large airport within 40 km wins, otherwise the nearest: "nearest" alone sends Zurich arrivals to the
        -- Dübendorf air base (10 km away) and Geneva arrivals to Annemasse
        QUALIFY ROW_NUMBER() OVER (PARTITION BY icao24 ORDER BY airport_type <> 'large_airport', distance_km) = 1""")
    con.execute("""
        CREATE OR REPLACE VIEW gold.traffic_per_minute AS
        SELECT date_trunc('minute', f.snapshot_time) AS minute, f.region, f.flight_phase,
               COALESCE(f.airline_name, 'unknown / private') AS airline_name,
               COUNT(DISTINCT f.icao24) AS aircraft, ROUND(AVG(f.altitude_ft)) AS avg_altitude_ft,
               ROUND(AVG(f.speed_kmh)) AS avg_speed_kmh
        FROM silver.flights f
        GROUP BY ALL""")
    con.execute("""
        CREATE OR REPLACE VIEW gold.api_usage AS
        SELECT make_timestamp(time * 1000000) AS snapshot_time, region, source, credits_remaining, aircraft,
               credits_remaining > 400 AS authenticated,           -- anonymous: at most 400 per day
               CASE WHEN credits_remaining > 400 THEN 'account' ELSE 'anonymous' END AS access
        FROM bronze.snapshots
        WHERE source = 'api'                                       -- your own calls, not the backfilled ones""")
