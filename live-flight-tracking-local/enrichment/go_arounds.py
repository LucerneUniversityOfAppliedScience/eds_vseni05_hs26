# Ported from the Databricks recipe (src_airport_ops/04_gold_go_arounds.py) – the pure pandas logic, without the Spark/pipeline part.
# Settings come from enrichment/config.py (CONF) instead of the pipeline configuration.
# Gold: go-arounds of the last hour. A go-around (missed approach) is an aborted landing: the aircraft is on final
# approach, close to the runway, and instead of touching down it climbs away again – because the runway is not
# clear, the approach is unstable, there is wind shear or the pilots cannot see the runway. It happens in roughly
# 1 to 3 of 1,000 approaches – rare, but clearly visible in the altitude profile.
#
# Detection per aircraft (time series of silver.flights over the last airport_ops.go_around_lookback_minutes):
#   1. low point: below 1,500 ft above the airport elevation, within 10 km of a large/medium airport
#   2. it came DOWN to that point: within the 5 minutes before, it was at least 300 ft higher, without touching the
#      ground in between (this excludes departures – they start on the ground or are first seen low and climbing)
#   3. it climbs AGAIN: within 5 minutes after the low point it gains more than 800 ft while still within 25 km of
#      the airport – and it does not touch the ground first (a landing followed by a new departure is no go-around;
#      touch-and-go training circuits are excluded the same way)
#   4. rotorcraft, gliders, balloons, drones are ignored (OpenSky aircraft category)
# Heights are QNH-corrected with the airport's METAR when available (see 03_gold_runway_in_use.py).
# Limits: ADS-B coverage near the ground is patchy, so the true lowest point may be lower than the observed one.
# Refreshes every airport_ops.go_around_interval (default "2 minutes").
from datetime import timedelta

import numpy as np
import pandas as pd
from enrichment.config import CONF

EARTH_KM = 6371.0
FT_PER_HPA = 27.0
NOT_AEROPLANES = [8, 9, 10, 11, 12, 14]      # rotorcraft, glider, lighter-than-air, parachutist, ultralight, UAV

SCHEMA = ("icao24 STRING, callsign STRING, airport_icao STRING, airport_name STRING, lowest_at TIMESTAMP, "
          "min_height_ft DOUBLE, min_altitude_ft DOUBLE, distance_km DOUBLE, heading_at_lowest DOUBLE, "
          "descent_before_ft DOUBLE, max_climb_ft DOUBLE, climb_until TIMESTAMP, qnh_corrected BOOLEAN")
COLUMNS = [c.split()[0] for c in SCHEMA.split(", ")]


def haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = np.radians(lat1), np.radians(lat2)
    a = np.sin((p2 - p1) / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(np.radians(lon2 - lon1) / 2) ** 2
    return 2 * EARTH_KM * np.arcsin(np.sqrt(a))


def nearest_airport(flights, airports, max_km):
    """Nearest airport (and distance) per position, None beyond max_km. Broadcast distance matrix in chunks."""
    lat, lon = flights["latitude"].to_numpy()[:, None], flights["longitude"].to_numpy()[:, None]
    best_d = np.full(len(flights), np.inf)
    best_i = np.full(len(flights), -1)
    for start in range(0, len(airports), 200):
        ap = airports.iloc[start:start + 200]
        d = haversine_km(lat, lon, ap["latitude"].to_numpy()[None, :], ap["longitude"].to_numpy()[None, :])
        j = d.argmin(axis=1)
        dj = d[np.arange(len(flights)), j]
        better = dj < best_d
        best_d[better], best_i[better] = dj[better], start + j[better]
    found = best_d <= max_km
    out = pd.DataFrame(index=flights.index)
    out["airport_icao"] = np.where(found, airports["airport_icao"].to_numpy()[np.clip(best_i, 0, None)], None)
    out["distance_km"] = np.where(found, best_d, np.nan)
    return out


def detect_go_arounds(flights, airports, metar=None, low_ft=1500, low_km=10, near_km=25, min_climb_ft=800,
                      min_descent_ft=300, window_min=5):
    """Go-around events from the position time series (one row per event)."""
    f = flights.copy()
    if "category" in f:
        f = f[~f["category"].isin(NOT_AEROPLANES)]
    f = f.sort_values(["icao24", "snapshot_time"]).reset_index(drop=True)
    if f.empty:
        return pd.DataFrame(columns=COLUMNS)
    f = f.join(nearest_airport(f, airports, near_km))
    ap = airports.set_index("airport_icao")
    qnh = metar.set_index("station")["qnh_hpa"] if metar is not None and len(metar) else pd.Series(dtype=float)
    f["qnh"] = f["airport_icao"].map(qnh).astype(float)
    f["height_ft"] = (f["altitude_ft"] + ((f["qnh"] - 1013.25) * FT_PER_HPA).fillna(0)
                      - f["airport_icao"].map(ap["elevation_ft"]).astype(float))
    on_ground = f["on_ground"].fillna(False).astype(bool)
    window = pd.Timedelta(minutes=window_min)
    events = []
    low = f[~on_ground & (f["height_ft"] < low_ft) & (f["distance_km"] <= low_km)]
    for (icao24, airport), lows in low.groupby(["icao24", "airport_icao"]):
        track = f[f["icao24"] == icao24]
        # consecutive low positions form one low segment (a gap of > 2 minutes starts a new one)
        segment_id = (lows["snapshot_time"].diff() > pd.Timedelta(minutes=2)).cumsum()
        for _, seg in lows.groupby(segment_id):
            lo = seg.loc[seg["height_ft"].idxmin()]
            t0 = lo["snapshot_time"]
            before = track[(track["snapshot_time"] >= t0 - window) & (track["snapshot_time"] < t0)]
            after = track[(track["snapshot_time"] > t0) & (track["snapshot_time"] <= t0 + window)]
            # 2. came down: no ground contact before, and higher up within the window
            if before["on_ground"].fillna(False).astype(bool).any():
                continue                                      # on the ground shortly before: a departure
            descent = before["height_ft"].max() - lo["height_ft"] if len(before) else np.nan
            if not descent >= min_descent_ft:
                continue
            # 3. climbs again before touching the ground, still near the same airport
            first_ground = after[after["on_ground"].fillna(False).astype(bool)]["snapshot_time"].min()
            if pd.notna(first_ground):
                after = after[after["snapshot_time"] < first_ground]
            after = after[(after["airport_icao"] == airport) & after["height_ft"].notna()]
            if after.empty:
                continue
            top = after.loc[after["height_ft"].idxmax()]
            climb = top["height_ft"] - lo["height_ft"]
            if climb <= min_climb_ft:
                continue
            events.append({
                "icao24": icao24, "callsign": track["callsign"].dropna().iloc[-1] if track["callsign"].notna().any()
                else None, "airport_icao": airport, "airport_name": ap.loc[airport, "airport_name"],
                "lowest_at": t0, "min_height_ft": round(lo["height_ft"]), "min_altitude_ft": lo["altitude_ft"],
                "distance_km": round(lo["distance_km"], 2), "heading_at_lowest": lo["heading"],
                "descent_before_ft": round(descent), "max_climb_ft": round(climb),
                "climb_until": top["snapshot_time"], "qnh_corrected": bool(pd.notna(lo["qnh"]))})
    return pd.DataFrame(events, columns=COLUMNS)


# ---------------------------------------------------------------------------------------------------------------
# Pipeline: a thin wrapper – read the last hour of low positions in Spark, the detection runs in pandas
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
LOOKBACK = int(CONF.get("airport_ops.go_around_lookback_minutes" + ("_world" if WORLD else ""),
                               "20" if WORLD else "60"))
