# Ported from the Databricks recipe (src_geo/04_gold_country_overflights.py) – the pure pandas logic, without the Spark/pipeline part.
# Settings come from enrichment/config.py (CONF) instead of the pipeline configuration.
# Gold: which country is below each aircraft – and how does the traffic per country develop?
#
#   gold.current_overflights           the country below every aircraft of gold.current_positions
#   silver.flights_country_per_minute  last position per aircraft and minute of the last 60 minutes, with country
#   gold.country_traffic_per_minute    minute x country: aircraft, airborne aircraft
#   gold.border_crossings              icao24, callsign, from_country, to_country, time – from consecutive positions
#
# Country lookup = point in polygon against silver.countries (03_silver_countries.py), pure Python/numpy:
#   1. bounding-box pre-filter per country (most aircraft are checked against 1–3 countries only)
#   2. ray casting with the even-odd rule over ALL rings of the country – exterior rings and holes together.
#      That makes multipolygons (islands) and holes correct without special cases: Italy has two holes, San Marino
#      and the Vatican; a point in San Marino crosses Italy's exterior ring once and the hole once = even = outside.
# Positions over the sea (or outside the countries kept for the region) get country NULL, shown as "Sea / outside
# land borders" in the per-minute table.
#
# Why per minute? The source delivers a snapshot every ~15 s: 60 minutes over Europe = ~700,000 positions. One
# position per aircraft and minute (the last one) is plenty for traffic per minute and border crossings and cuts the
# point-in-polygon work by 4x. The reduction runs in Spark, before the data is pulled into pandas.
#
# Border crossings compare each position with the aircraft's previous position over land, so a flight from France to
# Spain across the Bay of Biscay still counts as FR -> ES; gap_minutes tells how long the aircraft was over the sea
# or out of coverage in between.
#
# Settings: geo.overflight_interval (default "5 minutes"), geo.overflight_lookback_minutes (default "60").
import json

import numpy as np
import pandas as pd
from enrichment.config import CONF

CATALOG = CONF.get("flights.catalog", "opensky")
SILVER = CONF.get("flights.silver_schema", "silver")
GOLD = CONF.get("flights.gold_schema", "gold")
INTERVAL = CONF.get("geo.overflight_interval", "5 minutes")
WORLD = CONF.get("flights.region", "switzerland").lower() == "world"   # ~10,000 aircraft: smaller windows
LOOKBACK_MINUTES = int(CONF.get("geo.overflight_lookback_minutes" + ("_world" if WORLD else ""),
                                    "15" if WORLD else "60"))
SEA = "Sea / outside land borders"

CURRENT_SCHEMA = ("snapshot_time TIMESTAMP, icao24 STRING, callsign STRING, airline_name STRING, "
                  "origin_country STRING, latitude DOUBLE, longitude DOUBLE, altitude_ft DOUBLE, on_ground BOOLEAN, "
                  "country_iso2 STRING, country_name STRING")
PER_MINUTE_SCHEMA = ("minute TIMESTAMP, snapshot_time TIMESTAMP, icao24 STRING, callsign STRING, latitude DOUBLE, "
                     "longitude DOUBLE, altitude_ft DOUBLE, on_ground BOOLEAN, country_iso2 STRING, "
                     "country_name STRING")
TRAFFIC_SCHEMA = "minute TIMESTAMP, country_iso2 STRING, country_name STRING, aircraft BIGINT, airborne BIGINT"
CROSSINGS_SCHEMA = ("icao24 STRING, callsign STRING, from_iso2 STRING, from_country STRING, to_iso2 STRING, "
                    "to_country STRING, last_seen_in_from TIMESTAMP, crossed_at TIMESTAMP, gap_minutes DOUBLE, "
                    "latitude DOUBLE, longitude DOUBLE, altitude_ft DOUBLE")


def _cols(schema):
    return [c.split(" ")[0] for c in schema.split(", ")]


def points_in_rings(lon, lat, rings, max_cells=2_000_000):
    """Even-odd ray casting, vectorised: boolean array, True where (lon[i], lat[i]) lies inside `rings`.

    rings: list of rings, each a list of [lon, lat]. Crossings are counted over ALL rings together (even-odd rule),
    so holes and multi-part polygons work without special cases. Each ring only looks at the points inside its own
    bounding box – a point outside it cannot change the parity. (Same function as in 02_gold_flights_in_sigmet.py:
    every pipeline source file runs on its own, so it is repeated here.)
    """
    lon, lat = np.asarray(lon, dtype=float), np.asarray(lat, dtype=float)
    inside = np.zeros(len(lon), dtype=bool)
    for ring in rings:
        r = np.asarray(ring, dtype=float)
        if len(r) < 3:
            continue
        x1, y1 = r[:, 0], r[:, 1]
        x2, y2 = np.roll(x1, -1), np.roll(y1, -1)        # edge i goes from point i to point i+1 (wraps around)
        cand = np.flatnonzero((lon >= x1.min()) & (lon <= x1.max()) & (lat >= y1.min()) & (lat <= y1.max()))
        for chunk in np.array_split(cand, len(cand) * len(r) // max_cells + 1):   # bounded memory
            px, py = lon[chunk, None], lat[chunk, None]                           # shape (points, 1)
            straddles = (y1 > py) != (y2 > py)                                    # edge crosses the ray's latitude
            with np.errstate(divide="ignore", invalid="ignore"):
                x_cross = x1 + (py - y1) * (x2 - x1) / (y2 - y1)                  # where it crosses
            inside[chunk] ^= ((straddles & (px < x_cross)).sum(axis=1) % 2 == 1)
    return inside


def assign_countries(lon, lat, pdf_countries):
    """Country (iso2, name) for every point; None where the point is over no country of the table."""
    lon, lat = np.asarray(lon, dtype=float), np.asarray(lat, dtype=float)
    iso = np.full(len(lon), None, dtype=object)
    name = np.full(len(lon), None, dtype=object)
    # small countries first: they are cheap, and a point on a (simplified) shared border goes to the smaller one
    for c in pdf_countries.sort_values("vertices").itertuples(index=False):
        idx = np.flatnonzero(pd.isna(iso) & (lon >= c.min_lon) & (lon <= c.max_lon)    # still unassigned + bbox
                             & (lat >= c.min_lat) & (lat <= c.max_lat))
        if len(idx) == 0:
            continue
        rings = [ring for poly in json.loads(c.geometry_geojson)["coordinates"] for ring in poly]
        idx = idx[points_in_rings(lon[idx], lat[idx], rings)]
        iso[idx], name[idx] = c.country_iso2, c.country_name
    return iso, name


def current_overflights(pdf_positions, pdf_countries):
    """gold.current_positions + country_iso2 / country_name."""
    pdf = pdf_positions.copy()
    pdf["country_iso2"], pdf["country_name"] = assign_countries(pdf["longitude"], pdf["latitude"], pdf_countries)
    return pdf[_cols(CURRENT_SCHEMA)]


def last_position_per_minute(pdf_flights):
    """One row per aircraft and minute: the last position of that minute (the pipeline does this step in Spark)."""
    pdf = pdf_flights.copy()
    pdf["minute"] = pd.to_datetime(pdf["snapshot_time"]).dt.floor("min")
    return (pdf.sort_values("snapshot_time").drop_duplicates(["icao24", "minute"], keep="last")
            .reset_index(drop=True))


def flights_with_country(pdf_minutes, pdf_countries):
    """Per-minute positions + country."""
    pdf = pdf_minutes.copy()
    pdf["country_iso2"], pdf["country_name"] = assign_countries(pdf["longitude"], pdf["latitude"], pdf_countries)
    return pdf[_cols(PER_MINUTE_SCHEMA)]


def country_traffic_per_minute(pdf):
    """minute x country: distinct aircraft and airborne aircraft."""
    if pdf.empty:
        return pd.DataFrame(columns=_cols(TRAFFIC_SCHEMA))
    d = pdf.assign(country_name=pdf["country_name"].fillna(SEA),
                   airborne_icao=pdf["icao24"].where(~pdf["on_ground"].fillna(False).astype(bool)))
    out = (d.groupby(["minute", "country_name"], dropna=False)
           .agg(country_iso2=("country_iso2", "first"), aircraft=("icao24", "nunique"),
                airborne=("airborne_icao", "nunique"))
           .reset_index())
    return out[_cols(TRAFFIC_SCHEMA)].sort_values(["minute", "aircraft"], ascending=[True, False])


def border_crossings(pdf):
    """Consecutive positions (over land) of the same aircraft in two different countries = a border crossing."""
    land = pdf[pdf["country_iso2"].notna()].sort_values(["icao24", "snapshot_time"]).copy()
    if land.empty:
        return pd.DataFrame(columns=_cols(CROSSINGS_SCHEMA))
    prev = land.groupby("icao24")[["country_iso2", "country_name", "snapshot_time"]].shift(1)
    changed = prev["country_iso2"].notna() & (prev["country_iso2"] != land["country_iso2"])
    x = land[changed]
    p = prev[changed]
    out = pd.DataFrame({
        "icao24": x["icao24"], "callsign": x["callsign"],
        "from_iso2": p["country_iso2"], "from_country": p["country_name"],
        "to_iso2": x["country_iso2"], "to_country": x["country_name"],
        "last_seen_in_from": p["snapshot_time"], "crossed_at": x["snapshot_time"],
        "gap_minutes": (pd.to_datetime(x["snapshot_time"]) - pd.to_datetime(p["snapshot_time"])).dt.total_seconds() / 60,
        "latitude": x["latitude"], "longitude": x["longitude"], "altitude_ft": x["altitude_ft"],
    })
    return out.sort_values("crossed_at").reset_index(drop=True)
