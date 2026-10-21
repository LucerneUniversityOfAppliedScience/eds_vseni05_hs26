# Ported from the Databricks recipe (src_geo/02_gold_flights_in_sigmet.py) – the pure pandas logic, without the Spark/pipeline part.
# Settings come from enrichment/config.py (CONF) instead of the pipeline configuration.
# Gold: which aircraft are inside an active SIGMET area right now – horizontally (point in polygon) AND vertically
# (altitude between the SIGMET's base and top)?
#
# Input: gold.current_positions (latest position per aircraft) and silver.sigmets (01_silver_sigmets.py).
# Only airborne aircraft count; a missing base means "from the surface", a missing top "no upper limit".
#
# Point in polygon – ray casting: shoot a ray from the point to the east and count how many polygon edges it
# crosses. Odd = inside, even = outside. Implemented in numpy for ALL aircraft at once (one row per aircraft, one
# column per polygon edge), after a cheap bounding-box pre-filter – no geo library needed. Positions are treated
# as flat lon/lat coordinates, which is fine for SIGMET-sized areas (not across the antimeridian or the poles).
#
# Teaching alternative – the same with Databricks SQL geospatial functions (NOT used here: their availability on
# Free Edition serverless is unverified; try it in a SQL editor):
#
#   SELECT p.icao24, p.callsign, s.sigmet_id, s.hazard
#   FROM opensky.gold.current_positions p
#   JOIN opensky.silver.sigmets s
#     ON p.longitude BETWEEN s.min_lon AND s.max_lon AND p.latitude BETWEEN s.min_lat AND s.max_lat   -- bbox first
#    AND ST_Contains(ST_GeomFromWKT(s.polygon_wkt), ST_Point(p.longitude, p.latitude))
#    AND p.altitude_ft BETWEEN COALESCE(s.base_ft, 0) AND COALESCE(s.top_ft, 99999)
#   WHERE NOT p.on_ground AND current_timestamp() BETWEEN s.valid_from AND s.valid_to;
#
# ... or with H3 (hexagonal grid cells): cover each polygon with cells once, then a plain equi-join on the cell id –
# fast for many points, approximate at the polygon edges:
#
#   WITH cells AS (SELECT s.*, explode(h3_polyfillash3(s.polygon_wkt, 5)) AS cell FROM opensky.silver.sigmets s)
#   SELECT p.icao24, c.sigmet_id FROM opensky.gold.current_positions p
#   JOIN cells c ON h3_longlatash3(p.longitude, p.latitude, 5) = c.cell;
#
# Setting: geo.sigmet_check_interval (default "1 minute") – cheap, a few thousand points against a few polygons.
import json

import numpy as np
import pandas as pd
from enrichment.config import CONF

CATALOG = CONF.get("flights.catalog", "opensky")
SILVER = CONF.get("flights.silver_schema", "silver")
GOLD = CONF.get("flights.gold_schema", "gold")

SCHEMA = ("snapshot_time TIMESTAMP, icao24 STRING, callsign STRING, airline STRING, latitude DOUBLE, "
          "longitude DOUBLE, altitude_ft DOUBLE, sigmet_id STRING, area_no INT, hazard STRING, "
          "hazard_description STRING, qualifier STRING, fir_id STRING, fir_name STRING, base_ft INT, top_ft INT, "
          "valid_to TIMESTAMP")


def points_in_rings(lon, lat, rings, max_cells=2_000_000):
    """Even-odd ray casting, vectorised: boolean array, True where (lon[i], lat[i]) lies inside `rings`.

    rings: list of rings, each a list of [lon, lat]. Crossings are counted over ALL rings together (even-odd rule),
    so holes and multi-part polygons work without special cases. Each ring only looks at the points inside its own
    bounding box – a point outside it cannot change the parity.
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


def flights_in_sigmet(pdf_positions, pdf_sigmets):
    """Aircraft (current_positions) inside the given (already active) SIGMET polygons – one row per pair."""
    cols = [c.split(" ")[0] for c in SCHEMA.split(", ")]
    air = pdf_positions[(~pdf_positions["on_ground"].fillna(False).astype(bool))
                        & pdf_positions["altitude_ft"].notna()].reset_index(drop=True)
    sig = pdf_sigmets[pdf_sigmets["polygon_json"].notna()]
    if air.empty or sig.empty:
        return pd.DataFrame(columns=cols)
    lon, lat, alt = air["longitude"].to_numpy(float), air["latitude"].to_numpy(float), air["altitude_ft"].to_numpy(float)
    hits = []
    for s in sig.itertuples(index=False):
        base = 0 if pd.isna(s.base_ft) else s.base_ft
        top = 99999 if pd.isna(s.top_ft) else s.top_ft
        pre = ((lon >= s.min_lon) & (lon <= s.max_lon) & (lat >= s.min_lat) & (lat <= s.max_lat)
               & (alt >= base) & (alt <= top))
        idx = np.flatnonzero(pre)                         # bbox + altitude pre-filter
        if len(idx) == 0:
            continue
        idx = idx[points_in_rings(lon[idx], lat[idx], [json.loads(s.polygon_json)])]
        for i in idx:
            hits.append({"snapshot_time": air.at[i, "snapshot_time"], "icao24": air.at[i, "icao24"],
                         "callsign": air.at[i, "callsign"], "airline": air.at[i, "airline_name"],
                         "latitude": lat[i], "longitude": lon[i], "altitude_ft": alt[i],
                         "sigmet_id": s.sigmet_id, "area_no": s.area_no, "hazard": s.hazard,
                         "hazard_description": s.hazard_description, "qualifier": s.qualifier,
                         "fir_id": s.fir_id, "fir_name": s.fir_name, "base_ft": s.base_ft, "top_ft": s.top_ft,
                         "valid_to": s.valid_to})
    out = pd.DataFrame(hits, columns=cols)
    for c in ["area_no", "base_ft", "top_ft"]:
        out[c] = pd.to_numeric(out[c]).astype("Int64")   # nullable integers for the INT columns (NaN -> NULL)
    return out
