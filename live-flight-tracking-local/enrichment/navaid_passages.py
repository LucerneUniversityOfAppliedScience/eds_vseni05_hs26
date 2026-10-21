# Ported from the Databricks recipe (src_airport_ops/07_gold_navaid_passages.py) – the pure pandas logic, without the Spark/pipeline part.
# Settings come from enrichment/config.py (CONF) instead of the pipeline configuration.
# Gold: aircraft passing radio navigation aids – "SWR123 passing ZUE (Zurich East VOR-DME)".
#
# Airliners do not fly great circles from A to B. They follow airways and arrival/departure routes, and many of those
# are still built on VORs and NDBs (silver.navaids): an aircraft is cleared "direct ZUE", flies over the beacon, turns
# onto the next airway. Counting the aircraft that fly over each navaid makes this structure visible – a handful of
# navaids collect a large share of the traffic, most see none. (Modern RNAV routes also use pure GPS waypoints that
# are not radio stations, so not every turn happens over a navaid.)
#
# Method, over the last airport_ops.navaid_lookback_minutes (default 15; world 5) of silver.flights, airborne only:
#   1. grid prefilter – every navaid is registered in the 0.1° x 0.1° grid cells its radius touches (more cells in
#      longitude towards the poles); only positions in those cells are compared. This keeps the world region cheap:
#      ~13,000 aircraft x 20 snapshots vs ~11,000 navaids becomes a join on two integer keys instead of a cross join.
#   2. exact great-circle (haversine) distance; "passing" = within airport_ops.navaid_radius_km (default 5 km)
#      horizontally. Altitude is not checked: an airliner at FL380 straight above ZUE is still "passing ZUE".
#   3. one passage per aircraft and navaid in the window: closest approach (time, distance, altitude, heading),
#      first/last time within the radius, and is_current = the aircraft is within the radius in the latest snapshot.
# Two tables:
#   gold.navaid_passages  one row per aircraft x navaid passage in the window (is_current = "passing right now")
#   gold.navaid_traffic   one row per navaid of the region: aircraft passing in the window (0 included)
# Refreshes every airport_ops.navaid_passages_interval (default "2 minutes").
from datetime import timedelta

import numpy as np
import pandas as pd
from enrichment.config import CONF

EARTH_KM = 6371.0
KM_PER_DEG = 111.195
CELL_DEG = 0.1                         # grid cell size for the prefilter (~11 km in latitude)
LON_CELLS = int(round(360 / CELL_DEG))

PASSAGES_SCHEMA = ("icao24 STRING, callsign STRING, navaid_ident STRING, navaid_name STRING, navaid_type STRING, "
                   "navaid_lat DOUBLE, navaid_lon DOUBLE, closest_km DOUBLE, closest_time TIMESTAMP, "
                   "first_time_near TIMESTAMP, last_time_near TIMESTAMP, altitude_ft DOUBLE, heading DOUBLE, "
                   "speed_kmh DOUBLE, latitude DOUBLE, longitude DOUBLE, is_current BOOLEAN, label STRING")
PASSAGES_COLUMNS = [c.split()[0] for c in PASSAGES_SCHEMA.split(", ")]
TRAFFIC_SCHEMA = ("navaid_ident STRING, navaid_name STRING, navaid_type STRING, latitude DOUBLE, longitude DOUBLE, "
                  "country STRING, usage_type STRING, aircraft_passing INT, aircraft_now INT, "
                  "median_altitude_ft DOUBLE, last_passage TIMESTAMP, recent_callsigns STRING, window_minutes INT, "
                  "rank_in_region INT")
TRAFFIC_COLUMNS = [c.split()[0] for c in TRAFFIC_SCHEMA.split(", ")]


def haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = np.radians(lat1), np.radians(lat2)
    a = np.sin((p2 - p1) / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(np.radians(lon2 - lon1) / 2) ** 2
    return 2 * EARTH_KM * np.arcsin(np.sqrt(a))


def cell_keys(lat, lon, cell_deg=CELL_DEG):
    """Grid cell (cy, cx) of positions; cx wraps around the date line. Same formula as the Spark prefilter."""
    n_lon = int(round(360 / cell_deg))
    cy = np.floor(np.asarray(lat, dtype=float) / cell_deg).astype("int64")
    cx = np.mod(np.floor(np.asarray(lon, dtype=float) / cell_deg).astype("int64") + n_lon // 2, n_lon) - n_lon // 2
    return cy, cx


def navaid_cells(pdf_navaids, radius_km=5.0, cell_deg=CELL_DEG):
    """Every grid cell touched by a navaid's radius: columns navaid_ident, latitude, longitude, cy, cx (one row per
    navaid and cell, ~9 cells per navaid in mid latitudes). The navaid row index is kept as nav_idx."""
    if pdf_navaids is None or pdf_navaids.empty:
        return pd.DataFrame(columns=["nav_idx", "cy", "cx"])
    n_lon = int(round(360 / cell_deg))
    lat = pdf_navaids["latitude"].to_numpy(float)
    lon = pdf_navaids["longitude"].to_numpy(float)
    dlat = radius_km / KM_PER_DEG
    dlon = radius_km / (KM_PER_DEG * np.maximum(np.cos(np.radians(np.minimum(np.abs(lat) + dlat, 89.9))), 1e-3))
    cy0, cy1 = np.floor((lat - dlat) / cell_deg).astype(int), np.floor((lat + dlat) / cell_deg).astype(int)
    cx0, cx1 = np.floor((lon - dlon) / cell_deg).astype(int), np.floor((lon + dlon) / cell_deg).astype(int)
    rows_idx, rows_cy, rows_cx = [], [], []
    for i in range(len(lat)):
        ys = np.arange(cy0[i], cy1[i] + 1)
        xs = np.arange(cx0[i], min(cx1[i], cx0[i] + n_lon - 1) + 1)
        yy, xx = np.meshgrid(ys, xs, indexing="ij")
        rows_idx.append(np.full(yy.size, i))
        rows_cy.append(yy.ravel())
        rows_cx.append(xx.ravel())
    cells = pd.DataFrame({"nav_idx": np.concatenate(rows_idx), "cy": np.concatenate(rows_cy),
                          "cx": np.mod(np.concatenate(rows_cx) + n_lon // 2, n_lon) - n_lon // 2})
    return cells.drop_duplicates().reset_index(drop=True)


def positions_near_navaids(pdf_positions, pdf_navaids, radius_km=5.0, cell_deg=CELL_DEG, cells=None):
    """All (position, navaid) pairs within radius_km: the position columns plus navaid_ident, navaid_name,
    navaid_type, navaid_lat, navaid_lon, distance_km."""
    nav = pdf_navaids.reset_index(drop=True)
    pos = pdf_positions.dropna(subset=["latitude", "longitude"]).reset_index(drop=True)
    if pos.empty or nav.empty:
        return pd.DataFrame()
    cells = navaid_cells(nav, radius_km, cell_deg) if cells is None else cells
    cy, cx = cell_keys(pos["latitude"], pos["longitude"], cell_deg)
    pairs = pd.DataFrame({"pos_idx": np.arange(len(pos)), "cy": cy, "cx": cx}).merge(cells, on=["cy", "cx"])
    if pairs.empty:
        return pd.DataFrame()
    p = pos.loc[pairs["pos_idx"]].reset_index(drop=True)
    n = nav.loc[pairs["nav_idx"], ["navaid_ident", "navaid_name", "navaid_type", "latitude", "longitude"]]
    n = n.rename(columns={"latitude": "navaid_lat", "longitude": "navaid_lon"}).reset_index(drop=True)
    p = pd.concat([p, n], axis=1)
    p["distance_km"] = haversine_km(p["latitude"], p["longitude"], p["navaid_lat"], p["navaid_lon"])
    return p[p["distance_km"] <= radius_km].reset_index(drop=True)


def navaid_passages(pdf_positions, pdf_navaids, radius_km=5.0, latest_time=None):
    """silver.flights positions (snapshot_time, icao24, callsign, latitude, longitude, altitude_ft, heading,
    speed_kmh; airborne) + silver.navaids -> one row per aircraft x navaid passage (closest approach)."""
    if "on_ground" in pdf_positions:                               # aircraft on the ground do not "pass" a navaid
        pdf_positions = pdf_positions[pdf_positions["on_ground"] != True]  # noqa: E712
    near = positions_near_navaids(pdf_positions, pdf_navaids, radius_km)
    if near.empty:
        return pd.DataFrame(columns=PASSAGES_COLUMNS)
    latest_time = latest_time if latest_time is not None else pdf_positions["snapshot_time"].max()
    near = near.sort_values("distance_km")
    keys = ["icao24", "navaid_ident", "navaid_lat"]               # idents repeat worldwide – lat makes them unique
    spans = near.groupby(keys)["snapshot_time"].agg(first_time_near="min", last_time_near="max").reset_index()
    best = near.drop_duplicates(keys).merge(spans, on=keys)
    best["is_current"] = best["last_time_near"] >= latest_time
    best["callsign"] = best["callsign"].where(best["callsign"].notna() & (best["callsign"] != ""), None)
    best["label"] = (best["callsign"].fillna(best["icao24"]) + " passing " + best["navaid_ident"] + " ("
                     + best["navaid_name"].fillna("?").astype(str) + " " + best["navaid_type"].astype(str) + ")")
    best = best.rename(columns={"distance_km": "closest_km", "snapshot_time": "closest_time"})
    best["closest_km"] = best["closest_km"].round(2)
    return best[PASSAGES_COLUMNS].sort_values(["is_current", "closest_time"], ascending=[False, False]) \
        .reset_index(drop=True)


def navaid_traffic(pdf_passages, pdf_navaids, window_minutes, n_callsigns=5):
    """One row per navaid of the region: how many aircraft passed it in the window (navaids without traffic: 0)."""
    nav = pdf_navaids[["navaid_ident", "navaid_name", "navaid_type", "latitude", "longitude", "country",
                       "usage_type"]]
    nav = nav.assign(navaid_lat=nav["latitude"])                  # idents repeat worldwide – lat makes them unique
    keys = ["navaid_ident", "navaid_lat"]
    if pdf_passages is None or pdf_passages.empty:
        agg = pd.DataFrame(columns=keys + ["aircraft_passing", "aircraft_now", "median_altitude_ft",
                                           "last_passage", "recent_callsigns"])
    else:
        p = pdf_passages.sort_values("closest_time", ascending=False)
        g = p.groupby(keys)
        agg = g.agg(aircraft_passing=("icao24", "nunique"), aircraft_now=("is_current", "sum"),
                    median_altitude_ft=("altitude_ft", "median"), last_passage=("closest_time", "max")).reset_index()
        calls = g["callsign"].apply(lambda s: ", ".join(s.dropna().drop_duplicates().head(n_callsigns)) or None)
        agg = agg.merge(calls.rename("recent_callsigns").reset_index(), on=keys)
    out = nav.merge(agg, on=keys, how="left")
    out["aircraft_passing"] = out["aircraft_passing"].fillna(0).astype(int)
    out["aircraft_now"] = out["aircraft_now"].fillna(0).astype(int)
    out["window_minutes"] = int(window_minutes)
    out = out.sort_values(["aircraft_passing", "navaid_ident"], ascending=[False, True]).reset_index(drop=True)
    out["rank_in_region"] = np.arange(1, len(out) + 1)
    return out[TRAFFIC_COLUMNS]


# ---------------------------------------------------------------------------------------------------------------
# Pipeline: Spark keeps only the positions in grid cells near a navaid (semi join), pandas does the rest
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
WORLD = CONF.get("flights.region", "switzerland").lower() == "world"   # ~13,000 aircraft: smaller window
LOOKBACK = int(CONF.get("airport_ops.navaid_lookback_minutes" + ("_world" if WORLD else ""),
                              "5" if WORLD else "15"))
RADIUS_KM = float(CONF.get("airport_ops.navaid_radius_km", "5"))
INTERVAL = CONF.get("airport_ops.navaid_passages_interval", "2 minutes")
