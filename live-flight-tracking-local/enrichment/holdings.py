# Ported from the Databricks recipe (src_airport_ops/05_gold_holding_patterns.py) – the pure pandas logic, without the Spark/pipeline part.
# Settings come from enrichment/config.py (CONF) instead of the pipeline configuration.
# Gold: aircraft flying holding patterns right now. When an airport cannot accept arrivals fast enough (weather,
# runway change, peak hour), approach control parks aircraft in a "holding": a racetrack over a fixed point – turns
# of 180°, outbound leg 1 minute at or below 14,000 ft, 1½ minutes above (ICAO Doc 8168 Vol I, Part II, Section 6).
# For Zurich, the vACC Switzerland chart (flight simulation use only, vacc.ch/file/547) shows holdings at AMIKI,
# GIPOL and RILAX; the official Swiss eAIP is not openly available.
#
# Detection per aircraft over the last airport_ops.holding_lookback_minutes (default 20) of silver.flights:
#   1. cumulative absolute heading change > 360° – i.e. at least one full lap. The heading (track) is rounded to
#      whole degrees, so steps below 2° count as noise and are ignored; steps across 0°/360° are wrapped
#      (from 355° to 5° is +10°, not -350°)
#   2. small area: every position within 15 km of the track's centre (our threshold)
#   3. roughly constant altitude: max − min below 2,000 ft (our tolerance – aircraft are moved down in the stack)
#   4. within 60 km of a large airport, at least 250 km/h on average, aeroplanes only – this keeps out gliders
#      thermalling, helicopters and light aircraft circling over a town
# laps = cumulative turn / 360. turn_direction: holdings are flown with all turns in one direction (standard:
# right), radar vectoring zig-zags left and right.
# Refreshes every airport_ops.holding_interval (default "2 minutes").
from datetime import timedelta

import numpy as np
import pandas as pd
from enrichment.config import CONF

EARTH_KM = 6371.0
NOT_AEROPLANES = [8, 9, 10, 11, 12, 14]      # rotorcraft, glider, lighter-than-air, parachutist, ultralight, UAV

SCHEMA = ("icao24 STRING, callsign STRING, nearest_airport_icao STRING, nearest_airport_name STRING, "
          "airport_distance_km DOUBLE, center_lat DOUBLE, center_lon DOUBLE, radius_km DOUBLE, laps DOUBLE, "
          "cumulative_turn_deg DOUBLE, turn_direction STRING, duration_min DOUBLE, altitude_min_ft DOUBLE, "
          "altitude_max_ft DOUBLE, altitude_band STRING, avg_speed_kmh DOUBLE, first_seen TIMESTAMP, "
          "last_seen TIMESTAMP")
COLUMNS = [c.split()[0] for c in SCHEMA.split(", ")]


def haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = np.radians(lat1), np.radians(lat2)
    a = np.sin((p2 - p1) / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(np.radians(lon2 - lon1) / 2) ** 2
    return 2 * EARTH_KM * np.arcsin(np.sqrt(a))


def turn_stats(headings, min_step=2.0):
    """(cumulative absolute turn, signed turn) of a heading series in degrees."""
    steps = (np.diff(np.asarray(headings, dtype=float)) + 180) % 360 - 180     # wrapped to [-180, 180)
    steps = steps[np.abs(steps) >= min_step]
    return float(np.abs(steps).sum()), float(steps.sum())


def detect_holdings(flights, airports, min_turn_deg=360, max_radius_km=15, max_alt_range_ft=2000,
                    max_airport_km=60, min_speed_kmh=250, min_points=8):
    """One row per aircraft that flew at least one lap in a small area at roughly constant altitude."""
    f = flights[(flights["on_ground"] != True) & flights["heading"].notna() & flights["altitude_ft"].notna()]  # noqa
    if "category" in f:
        f = f[~f["category"].isin(NOT_AEROPLANES)]
    large = airports[airports["airport_type"] == "large_airport"]
    rows = []
    for icao24, t in f.sort_values("snapshot_time").groupby("icao24"):
        if len(t) < min_points:
            continue
        total, signed = turn_stats(t["heading"])
        if total <= min_turn_deg:
            continue
        clat, clon = t["latitude"].mean(), t["longitude"].mean()
        radius = haversine_km(t["latitude"], t["longitude"], clat, clon).max()
        alt_min, alt_max = t["altitude_ft"].min(), t["altitude_ft"].max()
        speed = t["speed_kmh"].mean()
        if radius > max_radius_km or alt_max - alt_min > max_alt_range_ft or not speed >= min_speed_kmh:
            continue
        d_large = haversine_km(large["latitude"], large["longitude"], clat, clon)
        if len(large) == 0 or d_large.min() > max_airport_km:
            continue
        d_all = haversine_km(airports["latitude"], airports["longitude"], clat, clon)
        near = airports.loc[d_all.idxmin()]
        rows.append({
            "icao24": icao24, "callsign": t["callsign"].dropna().iloc[-1] if t["callsign"].notna().any() else None,
            "nearest_airport_icao": near["airport_icao"], "nearest_airport_name": near["airport_name"],
            "airport_distance_km": round(float(d_all.min()), 1), "center_lat": round(clat, 4),
            "center_lon": round(clon, 4), "radius_km": round(float(radius), 1), "laps": round(total / 360, 1),
            "cumulative_turn_deg": round(total), "turn_direction": "right" if signed >= 0.8 * total
            else "left" if signed <= -0.8 * total else "mixed",
            "duration_min": round((t["snapshot_time"].max() - t["snapshot_time"].min()).total_seconds() / 60, 1),
            "altitude_min_ft": alt_min, "altitude_max_ft": alt_max,
            "altitude_band": f"{int(alt_min // 1000 * 1000):,}–{int(-(-alt_max // 1000) * 1000):,} ft",
            "avg_speed_kmh": round(float(speed)), "first_seen": t["snapshot_time"].min(),
            "last_seen": t["snapshot_time"].max()})
    return pd.DataFrame(rows, columns=COLUMNS)


# ---------------------------------------------------------------------------------------------------------------
# Pipeline: a thin wrapper – read the last 20 minutes in Spark, the detection runs in pandas
# ---------------------------------------------------------------------------------------------------------------
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
WORLD = CONF.get("flights.region", "switzerland").lower() == "world"   # ~10,000 aircraft: smaller windows
LOOKBACK = int(CONF.get("airport_ops.holding_lookback_minutes" + ("_world" if WORLD else ""),
                               "10" if WORLD else "20"))
