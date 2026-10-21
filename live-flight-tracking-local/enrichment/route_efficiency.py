# Ported from the Databricks recipe (src_aircraft/06_gold_route_efficiency.py) – the pure pandas logic, without the Spark/pipeline part.
# Settings come from enrichment/config.py (CONF) instead of the pipeline configuration.
# Gold: how much longer than the great circle do aircraft really fly – and what does that cost in fuel and CO2?
#
# CO2 calculators (myclimate, ICAO, airline offset pages) start from the GREAT-CIRCLE distance between the two
# airports and add a flat allowance for detours:
#   * myclimate: + 95 km per flight, independent of the distance (as documented in secondary sources, e.g. the BOKU
#     carbon offset system paper – check myclimate's own methodology before you quote it)
#   * ICAO Carbon Emissions Calculator: + 50 km (< 550 km), + 100 km (550–5,500 km), + 125 km (> 5,500 km)
# Closed airspace (Ukraine, Russia for EU carriers, parts of the Middle East), weather, holding and airway structure
# make real routes longer. This table measures it with our own ADS-B positions.
#
#   gold.route_efficiency   one row per flight (icao24 + callsign) with a known route (silver.flight_routes) that we
#                           saw airborne in the last aircraft.efficiency_lookback_minutes (default 180)
#
# How it is measured (read before you quote a number!)
#   * observed_km: length of the track we recorded – sum of the distances between consecutive positions. Jumps that
#     would need more than 1,300 km/h are dropped (bad positions); gaps are bridged with a straight line.
#   * We only see the part of the flight inside our region. Outside it we assume the aircraft flies DIRECTLY:
#       estimated_route_km = great circle(origin -> first seen) + observed_km + great circle(last seen -> destination)
#     -> extra_km = estimated_route_km - gcd_km is a LOWER BOUND: the real detour is at least this big.
#     The bigger the region (switzerland < alps < east / europe), the more of the detour we actually see.
#   * max_cross_track_km: the largest distance between the aircraft and the great circle origin -> destination.
#     Tens of km = normal airway structure; hundreds of km = the aircraft avoids an area (closed airspace, weather).
#   * extra fuel / CO2: extra_km × fuel per km of THIS aircraft, from gold.fuel_burn (OpenAP estimate, same
#     assumptions as there). NULL when OpenAP has no model for the type.
#   * calculator comparison: myclimate_extra_km = 95, icao_extra_km = 50/100/125. beyond_calculators = TRUE when our
#     lower bound is already bigger than the allowance – the calculator underestimates this flight.
#   * Routes come from adsbdb per callsign and can be WRONG: airlines reuse callsigns (EZY12AB, BEL1BT mean another
#     leg on another day) and the database is maintained by volunteers. So we check them with our own positions:
#       route_status = 'confirmed'     we saw the take-off at (or the landing on) the airport adsbdb names
#                      'contradicted'  we saw it take off / land at ANOTHER airport, or the aircraft is not on the
#                                      way from origin to destination at all (route_check says why)
#                      'unverified'    neither seen (typical for overflights in a small region)
#     Take-off seen = first airborne position CLIMBING below 5,000 ft within 25 km of an airport; landing seen =
#     last position DESCENDING below 5,000 ft within 25 km of an airport and not seen for 3 minutes. (Climbing /
#     descending matters: an aircraft entering the region low to land in Basel did not take off there.) The bigger the region, the
#     more flights are confirmed – with region = world most departures are seen.
#     Contradicted rows stay in the table (for teaching: how good is the enrichment source?) but their extra_km
#     means nothing – filter on route_status <> 'contradicted'.
#
# Refreshes every aircraft.efficiency_interval (default 5 minutes). Pure pandas/numpy, no external calls.
import numpy as np
import pandas as pd
from enrichment.config import CONF

CATALOG = CONF.get("flights.catalog", "opensky")
SILVER = CONF.get("flights.silver_schema", "silver")
GOLD = CONF.get("flights.gold_schema", "gold")
WORLD = CONF.get("flights.region", "switzerland").lower() == "world"   # whole flights: look back 12 h
LOOKBACK_MIN = int(CONF.get("aircraft.efficiency_lookback_minutes" + ("_world" if WORLD else ""),
                                   "720" if WORLD else "180"))

MAX_SPEED_KMH = 1300            # faster than that between two positions = bad position, segment dropped
MIN_OBSERVED_KM = 20            # shorter tracks say nothing
MYCLIMATE_EXTRA_KM = 95
AIRPORT_RADIUS_KM = 25          # take-off / landing seen within this distance of an airport ...
LOW_FT = 5000                   # ... below this altitude (one position per minute: a climb covers ~2,500 ft/min)
GONE_MIN = 3                    # landing: not seen for at least this many minutes
CO2_PER_KG_FUEL = 3.16

SCHEMA = ("icao24 STRING, callsign STRING, airline_name STRING, typecode STRING, origin_icao STRING, "
          "origin_name STRING, origin_lat DOUBLE, origin_lon DOUBLE, destination_icao STRING, destination_name STRING, "
          "destination_lat DOUBLE, destination_lon DOUBLE, first_seen TIMESTAMP, "
          "last_seen TIMESTAMP, positions INT, gcd_km DOUBLE, observed_km DOUBLE, observed_direct_km DOUBLE, "
          "estimated_route_km DOUBLE, extra_km DOUBLE, extra_pct DOUBLE, max_cross_track_km DOUBLE, "
          "fuel_kg_per_km DOUBLE, extra_fuel_kg DOUBLE, extra_co2_kg DOUBLE, myclimate_extra_km DOUBLE, "
          "icao_extra_km DOUBLE, beyond_calculators BOOLEAN, observed_origin_icao STRING, "
          "observed_destination_icao STRING, route_status STRING, route_check STRING")
R = 6371.0


def haversine_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = (np.radians(np.asarray(x, dtype=float)) for x in (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * R * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


def cross_track_km(lat, lon, lat1, lon1, lat2, lon2):
    """Distance of points (lat, lon) from the great circle through point 1 and point 2 (km, always >= 0)."""
    d13 = haversine_km(lat1, lon1, lat, lon) / R
    p1, l1, p2, l2, p3, l3 = (np.radians(np.asarray(x, dtype=float)) for x in (lat1, lon1, lat2, lon2, lat, lon))
    b12 = np.arctan2(np.sin(l2 - l1) * np.cos(p2), np.cos(p1) * np.sin(p2) - np.sin(p1) * np.cos(p2) * np.cos(l2 - l1))
    b13 = np.arctan2(np.sin(l3 - l1) * np.cos(p3), np.cos(p1) * np.sin(p3) - np.sin(p1) * np.cos(p3) * np.cos(l3 - l1))
    return np.abs(np.arcsin(np.clip(np.sin(d13) * np.sin(b13 - b12), -1, 1)) * R)


def icao_extra_km(gcd_km):
    """ICAO Carbon Emissions Calculator distance correction on top of the great circle."""
    return np.where(gcd_km < 550, 50.0, np.where(gcd_km <= 5500, 100.0, 125.0))


def observed_track_km(track):
    """Length of one aircraft's recorded track (sorted by time), without implausible jumps."""
    lat, lon = track["latitude"].to_numpy(float), track["longitude"].to_numpy(float)
    if len(lat) < 2:
        return 0.0
    seg = haversine_km(lat[:-1], lon[:-1], lat[1:], lon[1:])
    hours = track["snapshot_time"].diff().dt.total_seconds().to_numpy()[1:] / 3600   # unit-safe (s / ns)
    ok = (hours > 0) & (seg / np.where(hours > 0, hours, 1) <= MAX_SPEED_KMH)
    return float(seg[ok].sum())


def nearest_airport(lat, lon, airports):
    """ICAO code of the closest airport within AIRPORT_RADIUS_KM, or None. airports: arrays (icao, lat, lon)."""
    if airports is None or lat is None or np.isnan(lat):
        return None
    codes, alat, alon = airports
    d = haversine_km(lat, lon, alat, alon)
    i = int(np.argmin(d))
    return codes[i] if d[i] <= AIRPORT_RADIUS_KM else None


def check_route(first, last, latest, adsbdb_origin, adsbdb_destination, airports):
    """-> (observed_origin, observed_destination, route_status, route_check) from our own first/last position."""
    low = lambda p: pd.notna(p.get("altitude_ft")) and p["altitude_ft"] < LOW_FT
    vr = lambda p: p.get("vertical_rate") if pd.notna(p.get("vertical_rate")) else 0.0
    origin = nearest_airport(first["latitude"], first["longitude"], airports) \
        if low(first) and vr(first) > 1 else None                              # m/s: climbing
    landed = (latest - last["snapshot_time"]).total_seconds() >= GONE_MIN * 60
    dest = nearest_airport(last["latitude"], last["longitude"], airports) \
        if low(last) and vr(last) < -1 and landed else None                    # descending, then gone
    if origin and origin != adsbdb_origin:
        return origin, dest, "contradicted", f"took off at {origin}, adsbdb says {adsbdb_origin}"
    if dest and dest != adsbdb_destination:
        return origin, dest, "contradicted", f"landed at {dest}, adsbdb says {adsbdb_destination}"
    if origin or dest:
        return origin, dest, "confirmed", " and ".join(x for x in (f"take-off at {origin} seen" if origin else "",
                                                                    f"landing at {dest} seen" if dest else "") if x)
    return None, None, "unverified", "neither take-off nor landing seen"


def build_route_efficiency(pdf_flights, pdf_routes, pdf_burn=None, pdf_types=None, pdf_airlines=None,
                           pdf_airports=None):
    """silver.flights (lookback window) + silver.flight_routes (+ gold.fuel_burn, aircraft types, airline names)
    -> one row per flight with great circle vs. flown distance and the extra fuel / CO2."""
    cols = [c.split()[0] for c in SCHEMA.split(", ")]
    if pdf_flights is None or pdf_flights.empty or pdf_routes is None or pdf_routes.empty:
        return pd.DataFrame(columns=cols)
    f = pdf_flights[~pdf_flights["on_ground"].fillna(False).astype(bool)].copy()
    f = f[f["latitude"].notna() & f["longitude"].notna()]
    f["callsign"] = f["callsign"].fillna("").str.strip()
    f["snapshot_time"] = pd.to_datetime(f["snapshot_time"], utc=True)
    routes = pdf_routes.dropna(subset=["origin_lat", "origin_lon", "destination_lat", "destination_lon"])
    routes = routes.drop_duplicates("callsign").set_index("callsign")
    f = f[f["callsign"].isin(routes.index)].sort_values(["icao24", "callsign", "snapshot_time"])
    latest = pd.to_datetime(pdf_flights["snapshot_time"], utc=True).max()
    airports = None
    if pdf_airports is not None and not pdf_airports.empty:
        a = pdf_airports.dropna(subset=["airport_icao", "latitude", "longitude"])
        airports = (a["airport_icao"].to_numpy(), a["latitude"].to_numpy(float), a["longitude"].to_numpy(float))

    rows = []
    for (icao24, cs), t in f.groupby(["icao24", "callsign"], sort=False):
        r = routes.loc[cs]
        observed = observed_track_km(t)
        if observed < MIN_OBSERVED_KM:
            continue
        o, d = (r["origin_lat"], r["origin_lon"]), (r["destination_lat"], r["destination_lon"])
        first, last = t.iloc[0], t.iloc[-1]
        gcd = float(haversine_km(*o, *d))
        before = float(haversine_km(*o, first["latitude"], first["longitude"]))
        after = float(haversine_km(last["latitude"], last["longitude"], *d))
        estimated = before + observed + after
        # wrong routes (adsbdb knows the callsign from another day / another leg). A flight must
        #   * not fly away from its destination (a detour may be almost sideways: Paris -> Tokyo south of Russia
        #     crosses Switzerland heading south-east), and
        #   * stay plausible overall: at most 30 % + 150 km longer than the great circle (same rule as
        #     route_plausible in flights_with_routes; real detours around closed airspace stay below it)
        closer_km = float(haversine_km(first["latitude"], first["longitude"], *d)) - after
        obs_o, obs_d, status, why = check_route(first, last, latest, r.get("origin_icao"),
                                                r.get("destination_icao"), airports)
        if status != "contradicted" and (closer_km <= 0 or estimated > 1.3 * gcd + 150):
            status, why = "contradicted", "aircraft is not on the way from origin to destination"
        xt = cross_track_km(t["latitude"], t["longitude"], *o, *d)
        rows.append({"icao24": icao24, "callsign": cs, "origin_icao": r.get("origin_icao"),
                     "origin_name": r.get("origin_name"), "origin_lat": o[0], "origin_lon": o[1],
                     "destination_icao": r.get("destination_icao"), "destination_lat": d[0], "destination_lon": d[1],
                     "destination_name": r.get("destination_name"), "first_seen": first["snapshot_time"],
                     "last_seen": last["snapshot_time"], "positions": len(t), "gcd_km": gcd,
                     "observed_km": observed,
                     "observed_direct_km": float(haversine_km(first["latitude"], first["longitude"],
                                                              last["latitude"], last["longitude"])),
                     "estimated_route_km": estimated, "extra_km": estimated - gcd,
                     "max_cross_track_km": float(np.nanmax(xt)), "airline_icao": first.get("airline_icao"),
                     "observed_origin_icao": obs_o, "observed_destination_icao": obs_d, "route_status": status,
                     "route_check": why})
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=cols)
    df["extra_km"] = df["extra_km"].clip(lower=0)           # rounding: a lower bound is never negative
    df["extra_pct"] = 100 * df["extra_km"] / df["gcd_km"]

    # fuel per km of this aircraft: OpenAP fuel flow / ground speed while airborne (cruise preferred)
    df["fuel_kg_per_km"] = np.nan
    if pdf_burn is not None and not pdf_burn.empty:
        b = pdf_burn[pdf_burn["fuel_kg_per_h"].notna() & (pdf_burn["speed_kt"] > 0)].copy()
        b["kg_per_km"] = b["fuel_kg_per_h"] / (b["speed_kt"] * 1.852)
        cruise = b[b["flight_phase"] == "cruise"].groupby("icao24")["kg_per_km"].median()
        anyphase = b.groupby("icao24")["kg_per_km"].median()
        df["fuel_kg_per_km"] = df["icao24"].map(cruise).fillna(df["icao24"].map(anyphase))
    df["extra_fuel_kg"] = df["extra_km"] * df["fuel_kg_per_km"]
    df["extra_co2_kg"] = df["extra_fuel_kg"] * CO2_PER_KG_FUEL
    df["myclimate_extra_km"] = float(MYCLIMATE_EXTRA_KM)
    df["icao_extra_km"] = icao_extra_km(df["gcd_km"].to_numpy())
    df["beyond_calculators"] = ((df["extra_km"] > np.maximum(df["myclimate_extra_km"], df["icao_extra_km"]))
                                .astype(object).where(df["route_status"] != "contradicted", None))

    df["typecode"] = (df["icao24"].map(pdf_types.drop_duplicates("icao24").set_index("icao24")["typecode"])
                      if pdf_types is not None and not pdf_types.empty else None)
    df["airline_name"] = (df["airline_icao"].map(pdf_airlines.drop_duplicates("airline_icao")
                                                 .set_index("airline_icao")["airline_name"])
                          if pdf_airlines is not None and not pdf_airlines.empty else None)
    for c in ["gcd_km", "observed_km", "observed_direct_km", "estimated_route_km", "extra_km", "max_cross_track_km",
              "extra_fuel_kg", "extra_co2_kg"]:
        df[c] = df[c].round(0)
    df["extra_pct"] = df["extra_pct"].round(1)
    df["fuel_kg_per_km"] = df["fuel_kg_per_km"].round(2)
    return df[cols].sort_values("extra_km", ascending=False)
