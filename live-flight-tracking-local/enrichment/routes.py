# Ported from the Databricks recipe (src_aircraft/04_silver_routes.py) – the pure pandas logic, without the Spark/pipeline part.
# Settings come from enrichment/config.py (CONF) instead of the pipeline configuration.
# Silver + gold: where does a flight come from and where is it going? ADS-B does not say – only the callsign
# (SWR100, EZY12AB). adsbdb (https://www.adsbdb.com, endpoint https://api.adsbdb.com/v0/callsign/<CALLSIGN>) maps a
# callsign to its scheduled route: airline, origin and destination airport with coordinates.
#
#   silver.flight_routes      one row per looked-up airline callsign: origin, destination, great-circle distance
#   gold.flights_with_routes  current positions + route + overflight / departing / arriving flags + progress in %
#
# adsbdb terms and limits (README of https://github.com/mrjackwills/adsbdb, read 2026-10-18):
#   * free, no API key; rate limit 512 requests per minute per IP (HTTP 429 above) – we stay far below: at most
#     aircraft.max_route_lookups (default 150) calls every aircraft.routes_interval (default 30 minutes), 4 per second.
#   * "The flight route data is the work of David Taylor, Edinburgh and Jim Mason, Glasgow, and may not be copied,
#     published, or incorporated into other databases without the explicit permission of David J Taylor, Edinburgh."
#     -> this table is a short-lived lookup cache for a private teaching dashboard: it is rebuilt on every refresh,
#     holds only the callsigns currently in the air and must NOT be exported, shared or published. Credit:
#     "Routes: adsbdb.com (flight route data © David Taylor & Jim Mason)".
#   * routes are SCHEDULED routes per callsign – a callsign can be reused or wrong. route_plausible below flags
#     routes where the aircraft is far away from the line between origin and destination.
#
# Network: api.adsbdb.com is NOT verified on the Free Edition allow-list. On a connection error the lookups stop at
# once, the table is created EMPTY (with schema) and a message is printed – the pipeline goes on.
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from enrichment.config import CONF

CATALOG = CONF.get("flights.catalog", "opensky")
SILVER = CONF.get("flights.silver_schema", "silver")
GOLD = CONF.get("flights.gold_schema", "gold")
WORLD = CONF.get("flights.region", "switzerland").lower() == "world"
MAX_LOOKUPS = int(CONF.get("aircraft.max_route_lookups" + ("_world" if WORLD else ""),
                                  "1500" if WORLD else "150"))
REGION = CONF.get("flights.region", "switzerland").lower()
BBOX = CONF.get("flights.bbox", "45.6,48.0,5.8,10.7")

URL = "https://api.adsbdb.com/v0/callsign/{}"
USER_AGENT = "databricks-cookbook-flight-tracking/1.0 (teaching demo)"
PAUSE_S = 0.25                  # 4 requests per second – far below the 512/minute limit
# same boxes as the OpenSky source (src/01_bronze_flights_raw.py): lamin, lamax, lomin, lomax
REGIONS = {
    "switzerland": (45.6, 48.0, 5.8, 10.7),
    "alps": (43.5, 49.0, 5.0, 16.5),
    "europe": (35.0, 72.0, -25.0, 45.0),
    "east": (36.0, 48.0, 26.0, 50.0),
    "world": (-90.0, 90.0, -180.0, 180.0),
}

ROUTE_SCHEMA = ("callsign STRING, airline_icao STRING, airline_name STRING, origin_icao STRING, origin_iata STRING, "
                "origin_name STRING, origin_country STRING, origin_lat DOUBLE, origin_lon DOUBLE, "
                "destination_icao STRING, destination_iata STRING, destination_name STRING, "
                "destination_country STRING, destination_lat DOUBLE, destination_lon DOUBLE, "
                "route_distance_km DOUBLE, looked_up_at TIMESTAMP")
GOLD_SCHEMA = ("snapshot_time TIMESTAMP, icao24 STRING, callsign STRING, airline_name STRING, latitude DOUBLE, "
               "longitude DOUBLE, altitude_ft DOUBLE, speed_kmh DOUBLE, heading DOUBLE, flight_phase STRING, "
               "on_ground BOOLEAN, has_route BOOLEAN, origin_icao STRING, origin_iata STRING, origin_name STRING, "
               "origin_lat DOUBLE, origin_lon DOUBLE, destination_icao STRING, destination_iata STRING, "
               "destination_name STRING, destination_lat DOUBLE, destination_lon DOUBLE, route_distance_km DOUBLE, "
               "flown_km DOUBLE, remaining_km DOUBLE, progress_pct DOUBLE, route_plausible BOOLEAN, "
               "departing_from_region BOOLEAN, arriving_in_region BOOLEAN, overflight BOOLEAN")


def region_bbox(region=REGION, bbox=BBOX):
    return REGIONS.get(region) or tuple(float(x) for x in bbox.split(","))


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in km (works on scalars and pandas Series)."""
    import numpy as np
    lat1, lon1, lat2, lon2 = (np.radians(pd.to_numeric(x, errors="coerce")) for x in (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * 6371 * np.arcsin(np.sqrt(a))


def airline_callsigns(pdf_positions, max_n=MAX_LOOKUPS):
    """Callsigns that look like airline flights (3-letter ICAO code + digit), airborne first, at most max_n.
    When there are more than max_n (region = world), a stable pseudo-random sample by hash: not alphabetical (that
    would be only AAL, ACA, AFR, ...) and the same callsigns on every refresh, so a flight keeps its route."""
    import hashlib
    df = pdf_positions[pdf_positions["callsign"].fillna("").str.match(r"^[A-Z]{3}[0-9]")]
    df = df.assign(ground=df["on_ground"].fillna(False).astype(bool),
                   h=df["callsign"].map(lambda c: hashlib.md5(c.encode()).hexdigest()))
    df = df.sort_values(["ground", "h"])
    return df["callsign"].drop_duplicates().head(max_n).tolist()


def parse_route(payload):
    """adsbdb JSON -> one flat dict (or None if the callsign is unknown)."""
    fr = (payload or {}).get("response")
    fr = fr.get("flightroute") if isinstance(fr, dict) else None
    if not fr or not fr.get("origin") or not fr.get("destination"):
        return None
    o, d, al = fr["origin"], fr["destination"], fr.get("airline") or {}
    row = {"callsign": fr.get("callsign"), "airline_icao": al.get("icao"), "airline_name": al.get("name")}
    for prefix, ap in (("origin", o), ("destination", d)):
        row.update({f"{prefix}_icao": ap.get("icao_code"), f"{prefix}_iata": ap.get("iata_code"),
                    f"{prefix}_name": ap.get("name"), f"{prefix}_country": ap.get("country_iso_name"),
                    f"{prefix}_lat": ap.get("latitude"), f"{prefix}_lon": ap.get("longitude")})
    row["route_distance_km"] = round(float(haversine_km(o.get("latitude"), o.get("longitude"),
                                                        d.get("latitude"), d.get("longitude"))), 1)
    return row


def lookup_routes(callsigns, session=None, pause_s=PAUSE_S):
    """Call adsbdb once per callsign. Unknown callsigns (404) are skipped; a connection error stops everything."""
    session = session or requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    rows, unknown, now = [], 0, datetime.now(timezone.utc)
    for i, cs in enumerate(callsigns):
        try:
            r = session.get(URL.format(cs), timeout=10)
        except requests.RequestException as e:
            print(f"flight_routes: adsbdb not reachable ({type(e).__name__}) after {i} lookups – stopping")
            break
        if r.status_code == 429:
            print(f"flight_routes: rate limited by adsbdb after {i} lookups – stopping")
            break
        if r.status_code == 404:
            unknown += 1
        elif r.ok:
            row = parse_route(r.json())
            if row:
                rows.append({**row, "looked_up_at": now})
        time.sleep(pause_s)
    print(f"flight_routes: {len(callsigns)} callsigns -> {len(rows)} routes, {unknown} unknown")
    return pd.DataFrame(rows, columns=[c.split()[0] for c in ROUTE_SCHEMA.split(", ")])


def build_flights_with_routes(pdf_positions, pdf_routes, bbox):
    """Current positions + route, flags relative to the region box and progress along the great circle."""
    lamin, lamax, lomin, lomax = bbox
    df = pdf_positions.merge(pdf_routes.drop(columns=["airline_icao", "airline_name", "looked_up_at"],
                                             errors="ignore"), on="callsign", how="left")
    df["has_route"] = df["origin_icao"].notna()

    def inside(lat, lon):
        return lat.between(lamin, lamax) & lon.between(lomin, lomax)

    df["flown_km"] = haversine_km(df["origin_lat"], df["origin_lon"], df["latitude"], df["longitude"]).round(1)
    df["remaining_km"] = haversine_km(df["latitude"], df["longitude"],
                                      df["destination_lat"], df["destination_lon"]).round(1)
    # progress: share of the way from origin to destination – flown / (flown + remaining) is robust even when the
    # aircraft is not exactly on the great circle (detours, holding, approach routes)
    df["progress_pct"] = (100 * df["flown_km"] / (df["flown_km"] + df["remaining_km"])).round(1)
    # plausible if the detour via the current position is at most 30 % + 150 km longer than the direct route
    df["route_plausible"] = (df["flown_km"] + df["remaining_km"]) <= 1.3 * df["route_distance_km"] + 150
    df["departing_from_region"] = inside(df["origin_lat"], df["origin_lon"])
    df["arriving_in_region"] = inside(df["destination_lat"], df["destination_lon"])
    df["overflight"] = ~df["departing_from_region"] & ~df["arriving_in_region"]
    for c in ["route_plausible", "departing_from_region", "arriving_in_region", "overflight"]:
        df[c] = df[c].astype(object).where(df["has_route"], None)      # unknown route -> NULL, not false
    return df[[c.split()[0] for c in GOLD_SCHEMA.split(", ")]]
