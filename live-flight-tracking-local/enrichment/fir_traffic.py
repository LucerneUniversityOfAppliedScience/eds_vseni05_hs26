# Ported from the Databricks recipe (src_geo/08_gold_fir_traffic.py) – the pure pandas logic, without the Spark/pipeline part.
# Settings come from enrichment/config.py (CONF) instead of the pipeline configuration.
# Gold: traffic per Flight Information Region (FIR) – who flies in closed / avoided airspace, and which detours do
# closed FIRs explain?
#
#   gold.restricted_fir_shapes   one row per restricted FIR: worst status/severity of silver.restricted_airspace +
#                                the FIR polygon (silver.firs) – the map layer
#   gold.current_firs            every aircraft of gold.current_positions with its FIR and, if it is inside a
#                                restricted FIR, the restriction and whether it applies to its registration country
#   gold.flights_per_fir         FIR x (aircraft, airborne, inside-restricted, restriction applies) right now
#   gold.restricted_fir_traffic  "who still flies there?": restricted FIR x airline x registration country
#   gold.route_restrictions      per flight of gold.route_efficiency: does the GREAT CIRCLE origin -> destination cross
#                                a restricted FIR, and did the recorded track stay out of it (-> detour explained)?
#
# FIR lookup = point in polygon (same numpy ray casting as 04_gold_country_overflights.py) with two rules:
#   * VATSIM boundaries overlap in places (terminal areas, upper areas, sectors – see 06_silver_firs.py), so each
#     position gets the SMALLEST containing boundary ("most specific wins"): FIRs are tried smallest first and only
#     still-unassigned points are tested. A bounding-box pre-filter per FIR keeps ~13,000 aircraft (region = world)
#     at well under a second.
#   * "Inside a restricted FIR" is tested directly against the restricted polygons, independent of that assignment –
#     an aircraft in OERD (Riyadh upper) is still inside OEJD (Jeddah).
#
# Restriction applies? silver.restricted_airspace lists the registration countries a restriction applies to (or ALL).
# OpenSky's origin_country is the country of REGISTRATION (from the transponder address), not the airline's nationality
# – a leased Irish-registered aircraft of a non-EU airline counts as "EU". Read restriction_applies as a hint.
#
# Route check (gold.route_restrictions), how it works:
#   1. sample the great circle origin -> destination every geo.route_gc_step_km (default 50 km)
#   2. point in polygon against the restricted FIRs with avoid_on_routes = TRUE (closed, or "do not operate at all
#      levels"); FIRs that contain the origin or the destination airport do not count (a flight TO Tehran must enter
#      OIIX) -> restricted_firs_on_gc, gc_restricted_km
#   3. the recorded track (silver.flights, one position per aircraft every geo.route_track_sample_minutes) against the
#      same polygons -> track_restricted_firs
#   4. track_avoids = TRUE when we saw the aircraft BEFORE the point where the great circle enters the first
#      restricted FIR and it is now beside or past it (progress_km >= first_restricted_at_km), without ever entering
#      one of them; FALSE when it entered; NULL when it has not got there yet, was only seen after it (small region,
#      short lookback) or no track was seen. progress_km = how far along the great circle the aircraft is.
#      Detours of aircraft with track_avoids = TRUE are explained (at least partly) by closed airspace – compare
#      with extra_km / max_cross_track_km of gold.route_efficiency.
#   Limits: FIR-level only (a partial restriction like "west of 60°E" is never used for routes: avoid_on_routes is
#   FALSE for it in the curated list); 2-D (no flight levels); the great circle is not the planned route – airways,
#   winds (jet stream!) and route charges also make real routes differ.
#
# Settings: geo.fir_traffic_interval (default "5 minutes", world "10 minutes"), geo.route_restrictions_interval
# (default "10 minutes", world "15 minutes"), geo.route_gc_step_km (50), geo.route_track_sample_minutes (5),
# geo.route_lookback_minutes (180, world 720 – same as aircraft.efficiency_lookback_minutes).
import json

import numpy as np
import pandas as pd
from enrichment.config import CONF

CATALOG = CONF.get("flights.catalog", "opensky")
SILVER = CONF.get("flights.silver_schema", "silver")
GOLD = CONF.get("flights.gold_schema", "gold")
WORLD = CONF.get("flights.region", "switzerland").lower() == "world"   # ~13,000 aircraft: refresh less often
_W = "_world" if WORLD else ""
FIR_INTERVAL = CONF.get("geo.fir_traffic_interval" + _W, "10 minutes" if WORLD else "5 minutes")
ROUTE_INTERVAL = CONF.get("geo.route_restrictions_interval" + _W, "15 minutes" if WORLD else "10 minutes")
GC_STEP_KM = float(CONF.get("geo.route_gc_step_km", "50"))
TRACK_SAMPLE_MIN = int(CONF.get("geo.route_track_sample_minutes", "5"))
LOOKBACK_MIN = int(CONF.get("geo.route_lookback_minutes" + _W, "720" if WORLD else "180"))

R = 6371.0
# Implausible positions: an airborne aircraft above 10,000 ft slower than 250 km/h, or anything faster than 1,300 km/h.
# Typical causes: a frozen / stale position (the transponder keeps sending the last GNSS fix) and GNSS spoofing, which
# is common around the Black Sea, the Baltic and the Middle East – exactly where the restricted FIRs are. Most
# "EU aircraft inside a closed FIR" in a single snapshot turn out to be such positions: check suspect_position first.
SUSPECT_MIN_KMH, SUSPECT_MIN_FT, SUSPECT_MAX_KMH = 250, 10000, 1300
MIN_DETOUR_KM = 50          # below that, extra_km (a lower bound) is too small to call it a detour
SEVERITY_RANK = {"closed": 3, "do_not_operate": 2, "below_flight_level": 1, "caution": 0}
STATUS_RANK = {"closed": 2, "avoided_by_eu_carriers": 1, "conflict_zone_advisory": 0}
CONFIDENCE_RANK = {"low": 0, "medium": 1, "high": 2}

SHAPES_SCHEMA = ("fir_icao STRING, fir_name STRING, country STRING, status STRING, severity STRING, "
                 "restriction_ids STRING, avoid_on_routes BOOLEAN, applies_to_countries STRING, min_safe_fl INT, "
                 "valid_until DATE, confidence STRING, label_lat DOUBLE, label_lon DOUBLE, min_lon DOUBLE, "
                 "max_lon DOUBLE, min_lat DOUBLE, max_lat DOUBLE, geometry_geojson STRING")
CURRENT_SCHEMA = ("snapshot_time TIMESTAMP, icao24 STRING, callsign STRING, airline_name STRING, "
                  "origin_country STRING, latitude DOUBLE, longitude DOUBLE, altitude_ft DOUBLE, on_ground BOOLEAN, "
                  "speed_kmh DOUBLE, fir_icao STRING, fir_name STRING, fir_country STRING, "
                  "in_restricted_fir BOOLEAN, restricted_fir_icao STRING, restricted_fir_name STRING, "
                  "restriction_status STRING, restriction_severity STRING, restriction_applies BOOLEAN, "
                  "below_min_fl BOOLEAN, suspect_position BOOLEAN")
PER_FIR_SCHEMA = ("fir_icao STRING, fir_name STRING, fir_country STRING, aircraft BIGINT, airborne BIGINT, "
                  "restricted BOOLEAN, restriction_status STRING, restriction_severity STRING, "
                  "aircraft_inside_restricted BIGINT, aircraft_restriction_applies BIGINT, label_lat DOUBLE, "
                  "label_lon DOUBLE")
RESTRICTED_TRAFFIC_SCHEMA = ("restricted_fir_icao STRING, fir_name STRING, restriction_status STRING, "
                             "restriction_severity STRING, airline_name STRING, origin_country STRING, "
                             "restriction_applies BOOLEAN, aircraft BIGINT, airborne BIGINT, below_min_fl BIGINT, "
                             "suspect_positions BIGINT, callsigns STRING")
ROUTE_SCHEMA = ("icao24 STRING, callsign STRING, airline_name STRING, origin_country STRING, origin_icao STRING, "
                "destination_icao STRING, route_status STRING, gcd_km DOUBLE, extra_km DOUBLE, "
                "max_cross_track_km DOUBLE, restricted_firs_on_gc STRING, gc_crosses_restricted BOOLEAN, "
                "applicable_firs_on_gc STRING, restriction_applies BOOLEAN, gc_restricted_km DOUBLE, "
                "first_restricted_at_km DOUBLE, progress_km DOUBLE, track_positions INT, "
                "track_restricted_firs STRING, track_avoids BOOLEAN, avoidance_status STRING, explanation STRING")


def _cols(schema):
    return [c.split(" ")[0] for c in schema.split(", ")]


def _typed(pdf, schema):
    """Columns in schema order with matching pandas dtypes (DOUBLE -> float, INT/BIGINT -> nullable int), so that
    spark.createDataFrame(..., schema) accepts them and a mock app gets the same types as the table."""
    pdf = pdf[_cols(schema)].copy()
    for name, typ in (c.split(" ") for c in schema.split(", ")):
        if typ == "DOUBLE":
            pdf[name] = pd.to_numeric(pdf[name], errors="coerce").astype(float)
        elif typ in ("INT", "BIGINT"):
            pdf[name] = pd.to_numeric(pdf[name], errors="coerce").round().astype("Int64")
    return pdf


def points_in_rings(lon, lat, rings, max_cells=2_000_000):
    """Even-odd ray casting, vectorised: boolean array, True where (lon[i], lat[i]) lies inside `rings`.

    rings: list of rings, each a list of [lon, lat]. Crossings are counted over ALL rings together (even-odd rule),
    so holes and multi-part polygons work without special cases. Each ring only looks at the points inside its own
    bounding box. (Same function as in 04_gold_country_overflights.py: every pipeline source file runs on its own.)
    """
    lon, lat = np.asarray(lon, dtype=float), np.asarray(lat, dtype=float)
    inside = np.zeros(len(lon), dtype=bool)
    for ring in rings:
        r = np.asarray(ring, dtype=float)
        if len(r) < 3:
            continue
        x1, y1 = r[:, 0], r[:, 1]
        x2, y2 = np.roll(x1, -1), np.roll(y1, -1)
        cand = np.flatnonzero((lon >= x1.min()) & (lon <= x1.max()) & (lat >= y1.min()) & (lat <= y1.max()))
        for chunk in np.array_split(cand, len(cand) * len(r) // max_cells + 1):
            px, py = lon[chunk, None], lat[chunk, None]
            straddles = (y1 > py) != (y2 > py)
            with np.errstate(divide="ignore", invalid="ignore"):
                x_cross = x1 + (py - y1) * (x2 - x1) / (y2 - y1)
            inside[chunk] ^= ((straddles & (px < x_cross)).sum(axis=1) % 2 == 1)
    return inside


def _rings(geometry_geojson):
    return [ring for poly in json.loads(geometry_geojson)["coordinates"] for ring in poly]


def _inside_fir(lon, lat, fir):
    """Boolean array: points inside one FIR row (bbox pre-filter, then ray casting)."""
    inside = np.zeros(len(lon), dtype=bool)
    idx = np.flatnonzero((lon >= fir.min_lon) & (lon <= fir.max_lon) & (lat >= fir.min_lat) & (lat <= fir.max_lat))
    if len(idx):
        inside[idx] = points_in_rings(lon[idx], lat[idx], _rings(fir.geometry_geojson))
    return inside


def haversine_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = (np.radians(np.asarray(x, dtype=float)) for x in (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * R * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


def great_circle_points(lat1, lon1, lat2, lon2, step_km=50.0):
    """Points on the great circle from 1 to 2, about every step_km (both ends included).
    -> (lat, lon, km from point 1), numpy arrays. Spherical linear interpolation of the unit vectors."""
    p1, l1, p2, l2 = np.radians([lat1, lon1, lat2, lon2])
    a = np.array([np.cos(p1) * np.cos(l1), np.cos(p1) * np.sin(l1), np.sin(p1)])
    b = np.array([np.cos(p2) * np.cos(l2), np.cos(p2) * np.sin(l2), np.sin(p2)])
    omega = np.arccos(np.clip(a @ b, -1, 1))
    n = max(int(np.ceil(omega * R / step_km)), 1) + 1
    t = np.linspace(0, 1, n)
    if omega < 1e-9:
        v = np.repeat(a[None, :], n, axis=0)
    else:
        v = (np.sin((1 - t) * omega)[:, None] * a + np.sin(t * omega)[:, None] * b) / np.sin(omega)
    lat = np.degrees(np.arctan2(v[:, 2], np.hypot(v[:, 0], v[:, 1])))
    lon = np.degrees(np.arctan2(v[:, 1], v[:, 0]))
    return lat, lon, t * omega * R


def restricted_fir_shapes(pdf_restricted, pdf_firs):
    """silver.restricted_airspace (one row per restriction and FIR) + silver.firs -> one row per restricted FIR with
    the WORST status / severity, the union of the countries it applies to and the polygon."""
    cols = _cols(SHAPES_SCHEMA)
    if pdf_restricted is None or pdf_restricted.empty or pdf_firs is None or pdf_firs.empty:
        return pd.DataFrame(columns=cols)
    r = pdf_restricted.copy()
    if "expired" in r:
        r = r[~r["expired"].fillna(False).astype(bool)]
    rows = []
    for fir, g in r.groupby("fir_icao"):
        countries = sorted({c for s in g["applies_to_countries"].dropna() for c in s.split(",")})
        fl = pd.to_numeric(g["min_safe_fl"], errors="coerce")
        rows.append({
            "fir_icao": fir,
            "status": max(g["status"], key=lambda s: STATUS_RANK.get(s, -1)),
            "severity": max(g["severity"], key=lambda s: SEVERITY_RANK.get(s, -1)),
            "restriction_ids": ",".join(sorted(g["restriction_id"])),
            "avoid_on_routes": bool(g["avoid_on_routes"].fillna(False).astype(bool).any()),
            "applies_to_countries": "ALL" if "ALL" in countries else ",".join(countries),
            "min_safe_fl": fl.max() if fl.notna().all() else np.nan,   # any all-levels row wins
            "valid_until": pd.to_datetime(g["valid_until"]).min(),
            "confidence": min(g["confidence"], key=lambda s: CONFIDENCE_RANK.get(s, -1)),
        })
    out = pd.DataFrame(rows).merge(
        pdf_firs[["fir_icao", "fir_name", "country", "label_lat", "label_lon", "min_lon", "max_lon", "min_lat",
                  "max_lat", "geometry_geojson"]], on="fir_icao", how="inner")
    out["min_safe_fl"] = out["min_safe_fl"].astype("Int64")
    out["valid_until"] = out["valid_until"].dt.date
    return _typed(out, SHAPES_SCHEMA).sort_values("fir_icao").reset_index(drop=True)


def assign_firs(lon, lat, pdf_firs):
    """FIR code for every point: the smallest containing boundary; None where no FIR of the table contains it."""
    lon, lat = np.asarray(lon, dtype=float), np.asarray(lat, dtype=float)
    fir = np.full(len(lon), None, dtype=object)
    for f in pdf_firs.sort_values("area_sq_deg").itertuples(index=False):
        idx = np.flatnonzero(pd.isna(fir) & (lon >= f.min_lon) & (lon <= f.max_lon)
                             & (lat >= f.min_lat) & (lat <= f.max_lat))
        if len(idx) == 0:
            continue
        idx = idx[points_in_rings(lon[idx], lat[idx], _rings(f.geometry_geojson))]
        fir[idx] = f.fir_icao
    return fir


def _applies(countries, origin_country):
    """Does a restriction (comma list of registration countries, or ALL) apply to this registration country?"""
    if not isinstance(countries, str):
        return None
    if countries == "ALL":
        return True
    if not isinstance(origin_country, str):
        return None
    return origin_country in countries.split(",")


def current_firs(pdf_positions, pdf_firs, pdf_shapes):
    """gold.current_positions -> + FIR (smallest containing, in_region FIRs only) + restricted-FIR flags."""
    cols = _cols(CURRENT_SCHEMA)
    if pdf_positions is None or pdf_positions.empty:
        return pd.DataFrame(columns=cols)
    pdf = pdf_positions[pdf_positions["latitude"].notna() & pdf_positions["longitude"].notna()].copy()
    lon, lat = pdf["longitude"].to_numpy(float), pdf["latitude"].to_numpy(float)
    firs = pdf_firs if pdf_firs is not None and not pdf_firs.empty else None
    if firs is not None and "in_region" in firs:
        firs = firs[firs["in_region"].fillna(True).astype(bool)]
    if firs is not None and not firs.empty:
        pdf["fir_icao"] = assign_firs(lon, lat, firs)
        names = firs.set_index("fir_icao")
        pdf["fir_name"] = pdf["fir_icao"].map(names["fir_name"])
        pdf["fir_country"] = pdf["fir_icao"].map(names["country"])
    else:
        pdf["fir_icao"], pdf["fir_name"], pdf["fir_country"] = None, None, None

    restricted = np.full(len(pdf), None, dtype=object)
    if pdf_shapes is not None and not pdf_shapes.empty:
        # most severe first, so an aircraft in two overlapping restricted boundaries gets the worse one
        shapes = pdf_shapes.assign(_rank=pdf_shapes["severity"].map(SEVERITY_RANK)).sort_values("_rank",
                                                                                                ascending=False)
        for s in shapes.itertuples(index=False):
            todo = np.flatnonzero(pd.isna(restricted))
            hit = _inside_fir(lon[todo], lat[todo], s)
            restricted[todo[hit]] = s.fir_icao
        sh = pdf_shapes.set_index("fir_icao")
    pdf["restricted_fir_icao"] = restricted
    pdf["in_restricted_fir"] = pdf["restricted_fir_icao"].notna()
    if "speed_kmh" not in pdf:
        pdf["speed_kmh"] = np.nan
    airborne = ~pdf["on_ground"].fillna(False).astype(bool)
    spd = pd.to_numeric(pdf["speed_kmh"], errors="coerce")
    pdf["suspect_position"] = (airborne & (((spd < SUSPECT_MIN_KMH) & (pdf["altitude_ft"] > SUSPECT_MIN_FT))
                                           | (spd > SUSPECT_MAX_KMH))).where(spd.notna(), None)
    if pdf["in_restricted_fir"].any():
        pdf["restricted_fir_name"] = pdf["restricted_fir_icao"].map(sh["fir_name"])
        pdf["restriction_status"] = pdf["restricted_fir_icao"].map(sh["status"])
        pdf["restriction_severity"] = pdf["restricted_fir_icao"].map(sh["severity"])
        countries = pdf["restricted_fir_icao"].map(sh["applies_to_countries"])
        pdf["restriction_applies"] = [_applies(c, o) for c, o in zip(countries, pdf["origin_country"])]
        min_fl = pd.to_numeric(pdf["restricted_fir_icao"].map(sh["min_safe_fl"]), errors="coerce")
        below = (pdf["altitude_ft"] < min_fl * 100) & ~pdf["on_ground"].fillna(False).astype(bool)
        pdf["below_min_fl"] = below.where(min_fl.notna(), None)
    else:
        for c in ["restricted_fir_name", "restriction_status", "restriction_severity", "restriction_applies",
                  "below_min_fl"]:
            pdf[c] = None
    return _typed(pdf, CURRENT_SCHEMA).reset_index(drop=True)


def flights_per_fir(pdf_current, pdf_firs=None, pdf_shapes=None):
    """gold.current_firs -> one row per FIR with aircraft right now (+ restricted FIRs of the region without any).

    aircraft / airborne count by the most specific FIR (fir_icao); aircraft_inside_restricted and
    aircraft_restriction_applies count by the restricted polygon (restricted_fir_icao) – OEJD (Jeddah) has few
    aircraft of its own because the terminal / upper areas OERK, OERD inside it are smaller, but all of them are
    inside the restricted OEJD polygon."""
    cols = _cols(PER_FIR_SCHEMA)
    if pdf_current is None or pdf_current.empty:
        return pd.DataFrame(columns=cols)
    d = pdf_current[pdf_current["fir_icao"].notna()].assign(
        airborne_icao=lambda x: x["icao24"].where(~x["on_ground"].fillna(False).astype(bool)))
    out = (d.groupby("fir_icao")
           .agg(fir_name=("fir_name", "first"), fir_country=("fir_country", "first"),
                aircraft=("icao24", "nunique"), airborne=("airborne_icao", "nunique"))
           .reset_index())
    # restricted counts by the restricted polygon (not the most-specific FIR) -> count them separately
    rc = (pdf_current[pdf_current["in_restricted_fir"].fillna(False).astype(bool)]
          .assign(applies_icao=lambda x: x["icao24"].where(x["restriction_applies"].fillna(False).astype(bool)))
          .groupby("restricted_fir_icao").agg(inside=("icao24", "nunique"), applies=("applies_icao", "nunique")))
    if pdf_shapes is not None and not pdf_shapes.empty:
        sh = pdf_shapes.set_index("fir_icao")
        if pdf_firs is not None and "in_region" in pdf_firs:   # restricted FIRs in the region without traffic
            region = set(pdf_firs.loc[pdf_firs["in_region"].fillna(False).astype(bool), "fir_icao"])
            missing = [f for f in sh.index if f in region and f not in set(out["fir_icao"])]
            if missing:
                extra = pd.DataFrame({"fir_icao": missing, "fir_name": sh.loc[missing, "fir_name"].values,
                                      "fir_country": sh.loc[missing, "country"].values, "aircraft": 0, "airborne": 0})
                out = pd.concat([out, extra], ignore_index=True)
        out["restricted"] = out["fir_icao"].isin(sh.index)
        out["restriction_status"] = out["fir_icao"].map(sh["status"])
        out["restriction_severity"] = out["fir_icao"].map(sh["severity"])
    else:
        out["restricted"], out["restriction_status"], out["restriction_severity"] = False, None, None
    for col, src in [("aircraft_inside_restricted", "inside"), ("aircraft_restriction_applies", "applies")]:
        out[col] = out["fir_icao"].map(rc[src]).where(out["restricted"]).astype("Int64")
        out.loc[out["restricted"] & out[col].isna(), col] = 0
    if pdf_firs is not None and not pdf_firs.empty:
        lab = pdf_firs.set_index("fir_icao")
        out["label_lat"] = out["fir_icao"].map(lab["label_lat"])
        out["label_lon"] = out["fir_icao"].map(lab["label_lon"])
    else:
        out["label_lat"], out["label_lon"] = np.nan, np.nan
    return _typed(out, PER_FIR_SCHEMA).sort_values("aircraft", ascending=False).reset_index(drop=True)


def restricted_fir_traffic(pdf_current, max_callsigns=8):
    """'Who still flies there?' – aircraft inside restricted FIRs per FIR x airline x registration country."""
    cols = _cols(RESTRICTED_TRAFFIC_SCHEMA)
    if pdf_current is None or pdf_current.empty:
        return pd.DataFrame(columns=cols)
    d = pdf_current[pdf_current["in_restricted_fir"].fillna(False).astype(bool)].copy()
    if d.empty:
        return pd.DataFrame(columns=cols)
    d["airline_name"] = d["airline_name"].fillna("unknown / private")
    d["origin_country"] = d["origin_country"].fillna("unknown")
    d["airborne_icao"] = d["icao24"].where(~d["on_ground"].fillna(False).astype(bool))
    d["below_icao"] = d["icao24"].where(d["below_min_fl"].fillna(False).astype(bool))
    d["suspect_icao"] = d["icao24"].where(d["suspect_position"].fillna(False).astype(bool))
    d["restriction_applies"] = d["restriction_applies"].astype(object)
    out = (d.groupby(["restricted_fir_icao", "airline_name", "origin_country"], dropna=False)
           .agg(restriction_status=("restriction_status", "first"),
                restriction_severity=("restriction_severity", "first"),
                restriction_applies=("restriction_applies", "first"),
                aircraft=("icao24", "nunique"), airborne=("airborne_icao", "nunique"),
                below_min_fl=("below_icao", "nunique"), suspect_positions=("suspect_icao", "nunique"),
                fir_name=("restricted_fir_name", "first"),
                callsigns=("callsign", lambda s: ",".join(sorted(s.dropna().astype(str).str.strip().unique())
                                                          [:max_callsigns])))
           .reset_index())
    out = _typed(out, RESTRICTED_TRAFFIC_SCHEMA)
    out["_applies"] = out["restriction_applies"].fillna(False).astype(bool)   # restriction applies first
    return out.sort_values(["_applies", "aircraft"], ascending=False).drop(columns="_applies").reset_index(drop=True)


def route_restrictions(pdf_routes, pdf_shapes, pdf_tracks=None, pdf_positions=None, step_km=GC_STEP_KM,
                       only_avoid_on_routes=True):
    """Per flight: restricted FIRs on the great circle origin -> destination, and did the track avoid them?

    pdf_routes: gold.route_efficiency (icao24, callsign, origin/destination icao + lat/lon; optional airline_name,
                route_status, gcd_km, extra_km, max_cross_track_km)
    pdf_shapes: gold.restricted_fir_shapes
    pdf_tracks: optional positions (icao24, callsign, snapshot_time, latitude, longitude[, origin_country])
    pdf_positions: optional gold.current_positions – only for origin_country when pdf_tracks has none
    """
    cols = _cols(ROUTE_SCHEMA)
    if pdf_routes is None or pdf_routes.empty or pdf_shapes is None or pdf_shapes.empty:
        return pd.DataFrame(columns=cols)
    routes = pdf_routes.dropna(subset=["origin_lat", "origin_lon", "destination_lat", "destination_lon"]) \
        .drop_duplicates(["icao24", "callsign"]).reset_index(drop=True)
    shapes = pdf_shapes[pdf_shapes["avoid_on_routes"].fillna(False).astype(bool)] if only_avoid_on_routes \
        else pdf_shapes
    shapes = shapes.reset_index(drop=True)
    n_r, n_s = len(routes), len(shapes)

    # 1. all great-circle samples of all routes in one array (route index ri, km from origin)
    lat_l, lon_l, km_l, ri_l = [], [], [], []
    for i, r in enumerate(routes.itertuples(index=False)):
        la, lo, km = great_circle_points(r.origin_lat, r.origin_lon, r.destination_lat, r.destination_lon, step_km)
        lat_l.append(la), lon_l.append(lo), km_l.append(km), ri_l.append(np.full(len(la), i))
    glat, glon, gkm, gri = (np.concatenate(x) for x in (lat_l, lon_l, km_l, ri_l))
    end_lat = np.concatenate([routes["origin_lat"], routes["destination_lat"]]).astype(float)
    end_lon = np.concatenate([routes["origin_lon"], routes["destination_lon"]]).astype(float)

    # 2. one polygon test per restricted FIR for all samples (and for the airports themselves)
    gc_in = np.zeros((n_s, len(glat)), dtype=bool)
    endpoint = np.zeros((n_s, n_r), dtype=bool)
    for k, s in enumerate(shapes.itertuples(index=False)):
        gc_in[k] = _inside_fir(glon, glat, s)
        e = _inside_fir(end_lon, end_lat, s)
        endpoint[k] = e[:n_r] | e[n_r:]
    gc_in &= ~endpoint[:, gri]                     # FIRs of the origin / destination airport do not count

    # 3. tracks
    tracks = None
    if pdf_tracks is not None and not pdf_tracks.empty:
        t = pdf_tracks.dropna(subset=["latitude", "longitude"]).copy()
        t["callsign"] = t["callsign"].astype(str).str.strip()
        key = routes[["icao24", "callsign"]].assign(ri=np.arange(n_r))
        key["callsign"] = key["callsign"].astype(str).str.strip()
        t = t.merge(key, on=["icao24", "callsign"], how="inner")
        if "snapshot_time" in t:
            t = t.sort_values(["ri", "snapshot_time"])
        tracks = t.reset_index(drop=True)
    track_in = np.zeros((n_s, 0 if tracks is None else len(tracks)), dtype=bool)
    if tracks is not None and len(tracks):
        tlon, tlat = tracks["longitude"].to_numpy(float), tracks["latitude"].to_numpy(float)
        for k, s in enumerate(shapes.itertuples(index=False)):
            track_in[k] = _inside_fir(tlon, tlat, s)

    # registration country per aircraft
    country = {}
    if tracks is not None and "origin_country" in tracks:
        country = tracks.dropna(subset=["origin_country"]).drop_duplicates("icao24") \
            .set_index("icao24")["origin_country"].to_dict()
    if pdf_positions is not None and not pdf_positions.empty and "origin_country" in pdf_positions:
        for k, v in pdf_positions.dropna(subset=["origin_country"]).drop_duplicates("icao24") \
                .set_index("icao24")["origin_country"].items():
            country.setdefault(k, v)

    fir_codes = shapes["fir_icao"].to_numpy()
    # group sample / track indices per route once (sorted by route index already)
    gc_bounds = np.searchsorted(gri, np.arange(n_r + 1))
    tr_bounds = np.searchsorted(tracks["ri"].to_numpy(), np.arange(n_r + 1)) if tracks is not None \
        else np.zeros(n_r + 1, dtype=int)
    rows = []
    for i, r in enumerate(routes.itertuples(index=False)):
        a, b = gc_bounds[i], gc_bounds[i + 1]
        seg_in = gc_in[:, a:b]                                       # FIR x samples
        on_gc = np.flatnonzero(seg_in.any(axis=1))
        # order the FIRs along the route
        on_gc = on_gc[np.argsort([np.argmax(seg_in[k]) for k in on_gc])] if len(on_gc) else on_gc
        oc = country.get(r.icao24)
        applies = [k for k in on_gc if _applies(shapes.at[k, "applies_to_countries"], oc)]
        relevant = applies if applies else list(on_gc)
        any_in = seg_in[relevant].any(axis=0) if relevant else np.zeros(b - a, dtype=bool)
        step = gkm[a + 1] - gkm[a] if b - a > 1 else 0.0
        gc_restricted_km = float(any_in.sum() * step)
        first_km = float(gkm[a:b][np.argmax(any_in)]) if any_in.any() else None

        ta, tb = tr_bounds[i], tr_bounds[i + 1]
        n_track = int(tb - ta)
        entered = [fir_codes[k] for k in range(n_s) if n_track and track_in[k, ta:tb].any()]
        progress = start = None
        if n_track:                              # km along the great circle of the last / first track position
            p = tracks.iloc[[ta, tb - 1]]
            d_o = haversine_km(r.origin_lat, r.origin_lon, p["latitude"], p["longitude"])
            d_d = haversine_km(p["latitude"], p["longitude"], r.destination_lat, r.destination_lon)
            start, progress = (gkm[b - 1] * d_o / np.maximum(d_o + d_d, 1e-9)).tolist()

        gc_total = gkm[b - 1]
        relevant_codes = [fir_codes[k] for k in relevant]
        if not len(on_gc):
            status, avoids = "no restricted FIR on great circle", None
        elif not n_track:
            status, avoids = "no track", None
        elif set(entered) & set(relevant_codes):
            status, avoids = "entered restricted FIR", False
        elif first_km is not None and progress >= first_km and start < first_km:
            status, avoids = "avoided", True     # seen before the restricted section and beside / after it
        elif first_km is not None and progress >= first_km:
            status, avoids = "past restricted section, not seen before it", None
        else:
            status, avoids = "before restricted section", None
        if len(on_gc) and not applies:
            status += " (restriction does not apply to this registration)"

        expl = None
        if len(on_gc):
            who = ", ".join(sorted({str(shapes.at[k, "country"] or fir_codes[k]) for k in relevant}))
            expl = (f"Great circle crosses {', '.join(relevant_codes)} ({who}) for ~{gc_restricted_km:,.0f} km"
                    + (f"; restriction applies to {oc} registrations" if applies else
                       f"; restriction does not apply to {oc or 'unknown'} registrations"))
            extra = getattr(r, "extra_km", None)
            if avoids is True:
                expl += " – track stayed outside" + (f": explains (part of) the {extra:,.0f} km detour"
                                                     if extra is not None and pd.notna(extra)
                                                     and extra >= MIN_DETOUR_KM else "")
            elif avoids is False:
                expl += f" – track entered {', '.join(sorted(set(entered) & set(relevant_codes)))}"
            if getattr(r, "route_status", None) == "contradicted":
                expl = "ROUTE CONTRADICTED by our own positions (wrong adsbdb route?) – ignore. " + expl
        rows.append({
            "icao24": r.icao24, "callsign": r.callsign, "airline_name": getattr(r, "airline_name", None),
            "origin_country": oc, "origin_icao": getattr(r, "origin_icao", None),
            "destination_icao": getattr(r, "destination_icao", None),
            "route_status": getattr(r, "route_status", None),
            "gcd_km": getattr(r, "gcd_km", None) or round(float(gc_total)),
            "extra_km": getattr(r, "extra_km", None), "max_cross_track_km": getattr(r, "max_cross_track_km", None),
            "restricted_firs_on_gc": ",".join(fir_codes[k] for k in on_gc) or None,
            "gc_crosses_restricted": bool(len(on_gc)),
            "applicable_firs_on_gc": ",".join(fir_codes[k] for k in applies) or None,
            "restriction_applies": bool(applies) if len(on_gc) else None,
            "gc_restricted_km": round(gc_restricted_km) if len(on_gc) else None,
            "first_restricted_at_km": round(first_km) if first_km is not None else None,
            "progress_km": round(progress) if progress is not None else None,
            "track_positions": n_track,
            "track_restricted_firs": ",".join(entered) or None,
            "track_avoids": avoids, "avoidance_status": status, "explanation": expl,
        })
    out = _typed(pd.DataFrame(rows, columns=cols), ROUTE_SCHEMA)
    return out.sort_values(["gc_crosses_restricted", "gc_restricted_km"], ascending=False, na_position="last") \
        .reset_index(drop=True)
