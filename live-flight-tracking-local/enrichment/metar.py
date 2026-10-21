# Ported from the Databricks recipe (src_airport_ops/01_silver_metar.py) – the pure pandas logic, without the Spark/pipeline part.
# Settings come from enrichment/config.py (CONF) instead of the pipeline configuration.
# Silver: current METAR weather reports of the airports in the region (aviationweather.gov Data API, JSON).
#
# A METAR is the routine weather observation of an airport, issued every 30 or 60 minutes (plus SPECI reports when
# the weather changes quickly). Pilots and controllers use it to choose the runway (wind!) and the approach type
# (visibility, cloud ceiling). Example:  METAR LSZH 181750Z 35005KT 9999 OVC027 11/08 Q1024 NOSIG
#   35005KT = wind from 350° (true north) at 5 knots | 9999 = visibility 10 km or more | OVC027 = overcast at
#   2,700 ft above the airport | 11/08 = temperature / dewpoint °C | Q1024 = QNH (pressure at sea level) 1024 hPa
#
# Which airports? The ICAO codes of silver.airports inside the pipeline's bounding box (same region presets as
# src/01_bronze_flights_raw.py). We ask by station id rather than by bbox: a large bbox query is thinned out by the
# API (Europe: 122 stations instead of the ~560 airports of silver.airports).
#
# API terms and usage guidance (https://aviationweather.gov/data/api/, US National Weather Service, public data):
#   - set a custom User-Agent, keep requests "limited in scope and frequency", max 100 requests per minute
#   - most endpoints return at most 400 entries -> we send at most 100 station ids per request, 1 s apart
#   - METARs update about once per hour -> this table refreshes only every airport_ops.metar_interval
#     (default 15 minutes) while the positions keep streaming
#   - status 204 = valid request without data
# Settings (pipeline configuration):
#   airport_ops.metar_interval       refresh interval of this table (default "15 minutes")
#   airport_ops.metar_max_stations   cap on stations per refresh (default 600 -> at most 6 requests)
import re
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from enrichment.config import CONF

METAR_URL = "https://aviationweather.gov/api/data/metar"
USER_AGENT = "databricks-cookbook-flight-tracking/1.0 (teaching demo)"
IDS_PER_REQUEST = 100
KT_TO_KMH = 1.852
SM_TO_M = 1609.344

# lamin, lamax, lomin, lomax – identical to the presets of src/01_bronze_flights_raw.py
REGIONS = {
    "switzerland": (45.6, 48.0, 5.8, 10.7),
    "alps": (43.5, 49.0, 5.0, 16.5),
    "europe": (35.0, 72.0, -25.0, 45.0),
    "east": (36.0, 48.0, 26.0, 50.0),
    "world": (-90.0, 90.0, -180.0, 180.0),
}

SCHEMA = ("station STRING, station_name STRING, latitude DOUBLE, longitude DOUBLE, elevation_m DOUBLE, "
          "observed_at TIMESTAMP, report_type STRING, wind_dir_deg INT, wind_variable BOOLEAN, wind_speed_kt INT, "
          "wind_gust_kt INT, wind_speed_kmh DOUBLE, wind_gust_kmh DOUBLE, visibility_m INT, ceiling_ft INT, "
          "cloud_layers STRING, weather STRING, temperature_c DOUBLE, dewpoint_c DOUBLE, qnh_hpa DOUBLE, "
          "flight_category STRING, flight_category_source STRING, raw_text STRING, fetched_at TIMESTAMP")
COLUMNS = [c.split()[0] for c in SCHEMA.split(", ")]
INT_COLUMNS = ["wind_dir_deg", "wind_speed_kt", "wind_gust_kt", "visibility_m", "ceiling_ft"]


def region_bbox(region, bbox):
    """(lamin, lamax, lomin, lomax) of the pipeline region – custom regions use flights.bbox."""
    region = (region or "switzerland").lower()
    if region == "custom":
        return tuple(float(x) for x in bbox.split(","))
    return REGIONS[region]


def stations_in_bbox(pdf_airports, bbox):
    """ICAO codes of the airports (silver.airports) inside the bounding box – large airports first, then by code
    (for big regions like world only the first airport_ops.metar_max_stations are fetched)."""
    lamin, lamax, lomin, lomax = bbox
    inside = pdf_airports[pdf_airports["latitude"].between(lamin, lamax)
                          & pdf_airports["longitude"].between(lomin, lomax)].dropna(subset=["airport_icao"])
    if "airport_type" in inside:
        inside = inside.assign(rank=(inside["airport_type"] != "large_airport").astype(int))
        return inside.sort_values(["rank", "airport_icao"])["airport_icao"].drop_duplicates().tolist()
    return sorted(inside["airport_icao"].unique().tolist())


def fetch_metars(ids, bbox=None, timeout=30):
    """Latest METARs as a list of dicts (API JSON). By station id in batches of 100, or by bbox if no ids given.
    Never raises: a failed batch is printed and skipped."""
    headers = {"User-Agent": USER_AGENT}
    if ids:
        queries = [{"ids": ",".join(ids[i:i + IDS_PER_REQUEST])} for i in range(0, len(ids), IDS_PER_REQUEST)]
    elif bbox:
        lamin, lamax, lomin, lomax = bbox
        queries = [{"bbox": f"{lamin},{lomin},{lamax},{lomax}"}]       # API order: lat0, lon0, lat1, lon1
    else:
        return []
    records = []
    for n, query in enumerate(queries):
        if n:
            time.sleep(1.0)                                            # stay far below 100 requests per minute
        try:
            r = requests.get(METAR_URL, params={**query, "format": "json"}, headers=headers, timeout=timeout)
            if r.status_code == 204:                                   # valid request, no data
                continue
            r.raise_for_status()
            records.extend(r.json())
        except Exception as e:
            print(f"METAR request {n + 1}/{len(queries)} failed: {e}")
    return records


def _num(v):
    """'6+' -> 6.0, 1.37 -> 1.37, None/'' -> None"""
    if v is None or v == "":
        return None
    try:
        return float(str(v).rstrip("+"))
    except ValueError:
        return None


def parse_visibility_m(raw, visib_sm):
    """Visibility in metres. European METARs report metres (4 digits, 9999 = 10 km or more) – take them from the raw
    text; CAVOK means >= 10 km. Otherwise convert the API's statute miles ('6+' = 6 SM or more, '10+' in the US)."""
    raw = raw or ""
    if re.search(r"\bCAVOK\b", raw):
        return 10000
    # the visibility group follows the wind group (dddffKT, VRBffKT, optional dddVddd variation)
    m = re.search(r"\b(?:\d{3}|VRB)\d{2,3}(?:G\d{2,3})?(?:KT|MPS)\s+(?:\d{3}V\d{3}\s+)?(\d{4})(?:NDV)?\b", raw)
    if m:
        return 10000 if m.group(1) == "9999" else int(m.group(1))
    sm = _num(visib_sm)
    return int(round(sm * SM_TO_M)) if sm is not None else None


def ceiling_ft(clouds, vert_vis):
    """Ceiling = base of the lowest broken (BKN) or overcast (OVC) layer in feet above the airport. A vertical
    visibility (VV, sky obscured by fog) also counts as ceiling. None = no ceiling (clear or only FEW/SCT)."""
    bases = [c.get("base") for c in (clouds or []) if c.get("cover") in ("BKN", "OVC", "OVX") and c.get("base") is not None]
    if vert_vis is not None:
        bases.append(vert_vis)
    return int(min(bases)) if bases else None


def derive_flight_category(ceiling, visibility_m):
    """FAA flight categories (used by aviationweather.gov):
    LIFR ceiling < 500 ft or visibility < 1 SM | IFR < 1,000 ft or < 3 SM | MVFR <= 3,000 ft or <= 5 SM | else VFR"""
    if ceiling is None and visibility_m is None:
        return None
    c = ceiling if ceiling is not None else 99999
    v = visibility_m / SM_TO_M if visibility_m is not None else 99.0
    if c < 500 or v < 1:
        return "LIFR"
    if c < 1000 or v < 3:
        return "IFR"
    if c <= 3000 or v <= 5:
        return "MVFR"
    return "VFR"


def parse_metars(records, fetched_at=None):
    """API JSON records -> one row per station (latest report), columns of SCHEMA."""
    fetched_at = fetched_at or datetime.now(timezone.utc)
    rows = []
    for rec in records:
        raw = rec.get("rawOb") or ""
        wdir = rec.get("wdir")
        variable = wdir == "VRB" or bool(re.search(r"\b\d{3}V\d{3}\b", raw))   # VRB or a variation sector 320V020
        wdir_deg = int(wdir) if isinstance(wdir, (int, float)) else None
        wspd, wgst = rec.get("wspd"), rec.get("wgst")
        vis = parse_visibility_m(raw, rec.get("visib"))
        ceil = ceiling_ft(rec.get("clouds"), rec.get("vertVis"))
        api_cat = rec.get("fltCat")
        obs = rec.get("obsTime")
        rows.append({
            "station": rec.get("icaoId"),
            "station_name": rec.get("name"),
            "latitude": rec.get("lat"),
            "longitude": rec.get("lon"),
            "elevation_m": rec.get("elev"),
            "observed_at": pd.Timestamp(obs, unit="s", tz="UTC") if obs else None,
            "report_type": rec.get("metarType"),
            "wind_dir_deg": wdir_deg,
            "wind_variable": variable,
            "wind_speed_kt": wspd,
            "wind_gust_kt": wgst,
            "wind_speed_kmh": round(wspd * KT_TO_KMH, 1) if wspd is not None else None,
            "wind_gust_kmh": round(wgst * KT_TO_KMH, 1) if wgst is not None else None,
            "visibility_m": vis,
            "ceiling_ft": ceil,
            "cloud_layers": " ".join(f"{c.get('cover')}{int(c['base']):,}ft" if c.get("base") is not None
                                     else str(c.get("cover")) for c in (rec.get("clouds") or [])) or None,
            "weather": rec.get("wxString"),
            "temperature_c": rec.get("temp"),
            "dewpoint_c": rec.get("dewp"),
            "qnh_hpa": rec.get("altim"),                                # the JSON API already returns hPa
            "flight_category": api_cat or derive_flight_category(ceil, vis),
            "flight_category_source": "api" if api_cat else "derived",
            "raw_text": raw or None,
            "fetched_at": pd.Timestamp(fetched_at),
        })
    pdf = pd.DataFrame(rows, columns=COLUMNS)
    if pdf.empty:
        return pdf
    # several reports per station possible (METAR + SPECI within the lookback window): keep the latest
    pdf = pdf.sort_values(["station", "observed_at"]).drop_duplicates("station", keep="last")
    for c in INT_COLUMNS:
        pdf[c] = pd.to_numeric(pdf[c], errors="coerce").round().astype("Int64")
    return pdf.reset_index(drop=True)


# ---------------------------------------------------------------------------------------------------------------
# Pipeline: a thin wrapper around the plain pandas / requests functions above
# ---------------------------------------------------------------------------------------------------------------
CATALOG = CONF.get("flights.catalog", "opensky")
SILVER = CONF.get("flights.silver_schema", "silver")
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
