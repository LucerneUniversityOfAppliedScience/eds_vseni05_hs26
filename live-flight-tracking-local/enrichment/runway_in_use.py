# Ported from the Databricks recipe (src_airport_ops/03_gold_runway_in_use.py) – the pure pandas logic, without the Spark/pipeline part.
# Settings come from enrichment/config.py (CONF) instead of the pipeline configuration.
# Gold: which runway is in use right now, per airport? Derived purely from the aircraft movements – plus the METAR
# wind and its headwind / crosswind component for that runway ("Zurich lands on 14 – wind 350° 4 kt → 3 kt tailwind").
#
# Idea: an aircraft landing on runway 14 flies the extended centerline of runway 14 towards its threshold, on a
# track close to the runway heading, descending. A departure from runway 14 climbs out beyond that threshold on the
# same track. So for every position of the last 15 minutes (silver.flights, not only the current positions – a
# single snapshot contains just one or two landing aircraft per airport):
#   1. airports within 25 km, height above the airport below 4,000 ft
#   2. arrivals: descending (vertical rate < -1 m/s), track within ±20° of a runway end heading, BEFORE the threshold
#      departures: climbing (> +1 m/s), track within ±20°, BEYOND the threshold (on or after the runway)
#   3. close to the extended centerline: cross-track distance below 0.5 km + 8 % of the along-track distance (a cone
#      of ~4.6°, our tolerance for approaches and departures). The cross-track distance tells
#      parallel or near-parallel runways apart: Zurich 14 (137°) and 16 (155°) differ by only 18°.
#   4. per airport and movement: the runway end with the most aircraft = runway in use (rank 1)
# Geometry on a local flat plane around the threshold (x = east, y = north in km) – good enough within 25 km:
#   u = (sin h, cos h) unit vector of the runway heading h, d = aircraft position − threshold
#   along-track = d · u (negative = before the threshold), cross-track = d × u (positive = right of the centerline)
# Altitudes: OpenSky's barometric altitude is a PRESSURE altitude (standard pressure 1013.25 hPa). Corrected with the
# airport's METAR QNH it becomes the altitude above sea level: altitude ≈ pressure altitude + (QNH − 1013.25) × 27 ft
# (1 hPa ≈ 27 ft near the ground). Without a METAR the uncorrected value is used.
# Refreshes every airport_ops.runway_interval (default "1 minute").
from datetime import timedelta

import numpy as np
import pandas as pd
from enrichment.config import CONF

EARTH_KM = 6371.0
FT_PER_HPA = 27.0

SCHEMA = ("airport_icao STRING, airport_name STRING, movement STRING, runway_end STRING, runway STRING, "
          "heading_true_deg DOUBLE, aircraft INT, positions INT, callsigns STRING, last_seen TIMESTAMP, rank INT, "
          "in_use BOOLEAN, wind_dir_deg INT, wind_speed_kt INT, wind_gust_kt INT, wind_variable BOOLEAN, "
          "headwind_kt DOUBLE, crosswind_kt DOUBLE, crosswind_from STRING, metar_observed_at TIMESTAMP, "
          "summary STRING, window_start TIMESTAMP, window_end TIMESTAMP")
COLUMNS = [c.split()[0] for c in SCHEMA.split(", ")]


def haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = np.radians(lat1), np.radians(lat2)
    a = np.sin((p2 - p1) / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(np.radians(lon2 - lon1) / 2) ** 2
    return 2 * EARTH_KM * np.arcsin(np.sqrt(a))


def angle_diff(a, b):
    """Signed difference a − b in degrees, wrapped to [-180, 180)."""
    return (np.asarray(a) - np.asarray(b) + 180) % 360 - 180


def positions_near_airports(flights, airports, max_km):
    """All (position, airport) pairs closer than max_km – a broadcast distance matrix, chunked by airport."""
    pairs = []
    lat, lon = flights["latitude"].to_numpy(), flights["longitude"].to_numpy()
    for start in range(0, len(airports), 200):
        ap = airports.iloc[start:start + 200]
        dist = haversine_km(lat[:, None], lon[:, None], ap["latitude"].to_numpy()[None, :],
                            ap["longitude"].to_numpy()[None, :])
        i, j = np.nonzero(dist <= max_km)
        pairs.append(pd.DataFrame({"pos": flights.index.to_numpy()[i], "airport_icao": ap["airport_icao"].to_numpy()[j],
                                   "distance_km": dist[i, j]}))
    return pd.concat(pairs, ignore_index=True) if pairs else pd.DataFrame(columns=["pos", "airport_icao", "distance_km"])


def height_above_airport(altitude_ft, qnh_hpa, elevation_ft):
    """Pressure altitude -> height above the airport, QNH-corrected when a METAR is available."""
    correction = ((qnh_hpa - 1013.25) * FT_PER_HPA).fillna(0)
    return altitude_ft + correction - elevation_ft.fillna(0)


def match_runway_movements(flights, airports, runways, metar=None, max_km=25, max_height_ft=4000,
                           heading_tol=20, min_vrate=1.0):
    """One row per position that flies an arrival or departure on a runway end (the best-matching one)."""
    f = flights[(flights["on_ground"] != True) & flights["altitude_ft"].notna() & flights["heading"].notna()  # noqa
                & flights["vertical_rate"].notna() & (flights["vertical_rate"].abs() >= min_vrate)]
    ap = airports[airports["airport_icao"].isin(runways["airport_icao"])]
    pairs = positions_near_airports(f, ap, max_km)
    if pairs.empty:
        return pd.DataFrame()
    p = pairs.join(f, on="pos").merge(ap[["airport_icao", "airport_name", "latitude", "longitude", "elevation_ft"]]
                                      .rename(columns={"latitude": "ap_lat", "longitude": "ap_lon"}), on="airport_icao")
    qnh = metar.set_index("station")["qnh_hpa"] if metar is not None and len(metar) else pd.Series(dtype=float)
    p["height_ft"] = height_above_airport(p["altitude_ft"], p["airport_icao"].map(qnh).astype(float), p["elevation_ft"])
    p = p[p["height_ft"] < max_height_ft]
    rw = runways[~runways["closed"].fillna(False).astype(bool)]
    m = p.merge(rw[["airport_icao", "runway", "runway_end", "heading_true_deg", "threshold_lat", "threshold_lon",
                    "length_ft"]], on="airport_icao")
    m["hdg_diff"] = angle_diff(m["heading"], m["heading_true_deg"])
    m = m[m["hdg_diff"].abs() <= heading_tol].copy()
    if m.empty:
        return pd.DataFrame()
    # runways without threshold coordinates: use the airport reference point instead
    thr_lat = m["threshold_lat"].fillna(m["ap_lat"])
    thr_lon = m["threshold_lon"].fillna(m["ap_lon"])
    x = (m["longitude"] - thr_lon) * np.cos(np.radians(thr_lat)) * np.pi / 180 * EARTH_KM      # km east
    y = (m["latitude"] - thr_lat) * np.pi / 180 * EARTH_KM                                     # km north
    h = np.radians(m["heading_true_deg"])
    m["along_km"] = x * np.sin(h) + y * np.cos(h)
    m["cross_km"] = x * np.cos(h) - y * np.sin(h)
    runway_km = (m["length_ft"].fillna(8000) * 0.0003048)
    arrival = (m["vertical_rate"] < 0) & (m["along_km"] < 0.5)
    departure = (m["vertical_rate"] > 0) & (m["along_km"] > 0) & (m["along_km"] < runway_km + max_km)
    cone = m["cross_km"].abs() <= 0.5 + 0.08 * m["along_km"].abs()
    m = m[(arrival | departure) & cone].copy()
    m["movement"] = np.where(m["vertical_rate"] < 0, "arrivals", "departures")
    # several runway ends may match one position (e.g. near-parallel runways): keep the closest centerline
    m = m.sort_values("cross_km", key=np.abs).drop_duplicates("pos")
    return m[["snapshot_time", "icao24", "callsign", "airport_icao", "airport_name", "runway", "runway_end",
              "heading_true_deg", "movement", "height_ft", "distance_km", "along_km", "cross_km", "hdg_diff"]]


def wind_components(wind_dir_deg, wind_speed_kt, runway_heading_deg):
    """Headwind and crosswind component of the wind for a runway.
    The METAR wind direction is where the wind COMES FROM, relative to true north – like our true runway headings.
    With the angle a = wind direction − runway heading:
        headwind  = speed × cos(a)   (negative = tailwind)
        crosswind = speed × sin(a)   (positive = wind from the right, negative = from the left)
    Example: runway 14 (137°), wind 350° 4 kt: a = 213° -> headwind = 4 × cos(213°) = −3.4 kt (3 kt tailwind),
    crosswind = 4 × sin(213°) = −2.2 kt (2 kt from the left)."""
    a = np.radians(np.asarray(wind_dir_deg, dtype=float) - np.asarray(runway_heading_deg, dtype=float))
    speed = np.asarray(wind_speed_kt, dtype=float)
    return speed * np.cos(a), speed * np.sin(a)


def _summary(r):
    verb = "lands on" if r["movement"] == "arrivals" else "departs from"
    text = f"{r['airport_icao']} {verb} {r['runway_end']}"
    if pd.isna(r["wind_speed_kt"]):
        return text + " – no METAR"
    if r["wind_speed_kt"] == 0:
        return text + " – wind calm"
    if pd.isna(r["wind_dir_deg"]):
        return text + f" – wind variable {int(r['wind_speed_kt'])} kt"
    hw, cw = r["headwind_kt"], r["crosswind_kt"]
    gust = f" gusts {int(r['wind_gust_kt'])} kt" if pd.notna(r["wind_gust_kt"]) else ""
    return (f"{text} – wind {int(r['wind_dir_deg']):03d}° {int(r['wind_speed_kt'])} kt{gust} → "
            f"{abs(hw):.0f} kt {'headwind' if hw >= 0 else 'tailwind'}, {abs(cw):.0f} kt crosswind"
            + (f" from the {r['crosswind_from']}" if round(abs(cw)) > 0 else ""))


def build_runway_in_use(matches, metar, window_start, window_end):
    """Aggregate the matched positions per airport / movement / runway end and add the METAR wind."""
    if matches.empty:
        return pd.DataFrame(columns=COLUMNS)
    # one runway per aircraft and movement: the one matched closest to the threshold (least ambiguous – further out,
    # the centerlines of near-parallel runways like Zurich 32 / 34 come close to each other)
    key = ["icao24", "airport_icao", "movement"]
    # an aircraft that climbs away after it was matched as an arrival flies a go-around, not a departure
    first_arrival = (matches[matches["movement"] == "arrivals"].groupby(["icao24", "airport_icao"])["snapshot_time"]
                     .min().rename("first_arrival").reset_index())
    matches = matches.merge(first_arrival, on=["icao24", "airport_icao"], how="left")
    matches = matches[~((matches["movement"] == "departures") & (matches["snapshot_time"] > matches["first_arrival"]))]
    best =(matches.assign(abs_along=matches["along_km"].abs()).sort_values("abs_along")
            .drop_duplicates(key)[key + ["runway_end"]])
    matches = matches.merge(best, on=key + ["runway_end"])
    g = (matches.groupby(["airport_icao", "airport_name", "movement", "runway_end", "runway", "heading_true_deg"])
         .agg(aircraft=("icao24", "nunique"), positions=("icao24", "size"), last_seen=("snapshot_time", "max"),
              callsigns=("callsign", lambda s: ", ".join(sorted(set(s.dropna()))[:10])))
         .reset_index())
    g = g.sort_values(["airport_icao", "movement", "aircraft", "last_seen"], ascending=[True, True, False, False])
    g["rank"] = g.groupby(["airport_icao", "movement"]).cumcount() + 1
    g["in_use"] = g["rank"] == 1
    if metar is not None and len(metar):
        w = metar[["station", "wind_dir_deg", "wind_speed_kt", "wind_gust_kt", "wind_variable", "observed_at"]]
        g = g.merge(w.rename(columns={"station": "airport_icao", "observed_at": "metar_observed_at"}),
                    on="airport_icao", how="left")
    else:
        for c in ("wind_dir_deg", "wind_speed_kt", "wind_gust_kt", "wind_variable", "metar_observed_at"):
            g[c] = None
    hw, cw = wind_components(pd.to_numeric(g["wind_dir_deg"], errors="coerce"),
                             pd.to_numeric(g["wind_speed_kt"], errors="coerce"), g["heading_true_deg"])
    g["headwind_kt"], g["crosswind_kt"] = np.round(hw, 1), np.round(cw, 1)
    g["crosswind_from"] = np.where(np.isnan(cw), None, np.where(cw >= 0, "right", "left"))
    g["summary"] = g.apply(_summary, axis=1)
    g["window_start"], g["window_end"] = window_start, window_end
    for c in ("wind_dir_deg", "wind_speed_kt", "wind_gust_kt"):
        g[c] = pd.to_numeric(g[c], errors="coerce").astype("Int64")
    return g[COLUMNS].reset_index(drop=True)


# ---------------------------------------------------------------------------------------------------------------
# Pipeline: a thin wrapper – read the last 15 minutes in Spark (filtered), the geometry runs in pandas
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
LOOKBACK = int(CONF.get("airport_ops.runway_lookback_minutes", "15"))
