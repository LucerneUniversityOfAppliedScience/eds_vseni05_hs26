# Ported from the Databricks recipe (src_geo/01_silver_sigmets.py) – the pure pandas logic, without the Spark/pipeline part.
# Settings come from enrichment/config.py (CONF) instead of the pipeline configuration.
# Silver: current international SIGMETs – official warnings of hazardous weather for aircraft in flight.
#
# A SIGMET ("significant meteorological information") is issued by a meteorological watch office for its flight
# information region (FIR) and describes an AREA (polygon), a vertical band (base/top) and a validity window
# (usually 4 hours, 6 for volcanic ash / tropical cyclones). Hazards: TS thunderstorms, TURB turbulence, ICE icing,
# MTW mountain waves, VA volcanic ash, TC tropical cyclone, DS/SS dust/sand storm, RDOACT CLD radioactive cloud.
#
# Source: aviationweather.gov Data API (NOAA / US National Weather Service), endpoint `isigmet` = international
# SIGMETs of all FIRs worldwide (~150–200 at a time). Data of the US government: public domain, no key needed.
# The API guidelines ask for a descriptive User-Agent, at most 100 requests per minute and local caching – this
# table makes ONE request per refresh (geo.sigmet_interval, default 15 minutes), the gold tables read the cache.
# Not used: `airsigmet` (US domestic SIGMETs/AIRMETs) and `gairmet` (graphical AIRMETs) cover the USA only.
#
# One row per polygon: a few SIGMETs describe several areas ("AREAS") – they become several rows with area_no 1, 2, …
# The polygon is stored twice: as WKT text (for Databricks SQL geospatial functions / maps) and as a JSON array of
# [lon, lat] points (for the pure-Python point-in-polygon test in 02_gold_flights_in_sigmet.py).
# Caveat: many SIGMETs describe two areas in the text – the observed position and a forecast position ("FCST AT
# 1900Z WI ...") – but the API delivers only one polygon per area, sometimes the observed and sometimes the forecast
# one. raw_text keeps the full message. Base/top: if the API leaves both empty, they are parsed from the text.
#
# Settings (pipeline configuration): geo.sigmet_interval (default "15 minutes"), flights.region / flights.bbox
# (only used to flag the SIGMETs that overlap the pipeline's region: column in_region).
import json
import re
from datetime import datetime, timezone

import pandas as pd
import requests
from enrichment.config import CONF

CATALOG = CONF.get("flights.catalog", "opensky")
SILVER = CONF.get("flights.silver_schema", "silver")
URL = "https://aviationweather.gov/api/data/isigmet"
USER_AGENT = "databricks-cookbook-flight-tracking/1.0 (teaching demo)"

# lamin, lamax, lomin, lomax – same presets as src/01_bronze_flights_raw.py
REGIONS = {
    "switzerland": (45.6, 48.0, 5.8, 10.7),
    "alps": (43.5, 49.0, 5.0, 16.5),
    "europe": (35.0, 72.0, -25.0, 45.0),
    "east": (36.0, 48.0, 26.0, 50.0),
    "world": (-90.0, 90.0, -180.0, 180.0),
}

HAZARDS = {
    "TS": "thunderstorms", "TSGR": "thunderstorms with hail", "TURB": "turbulence", "ICE": "icing",
    "MTW": "mountain waves", "VA": "volcanic ash", "TC": "tropical cyclone", "DS": "dust storm", "SS": "sand storm",
    "RDOACT CLD": "radioactive cloud",
}

SCHEMA = ("sigmet_id STRING, area_no INT, icao_id STRING, fir_id STRING, fir_name STRING, series_id STRING, "
          "hazard STRING, hazard_description STRING, qualifier STRING, valid_from TIMESTAMP, valid_to TIMESTAMP, "
          "base_ft INT, top_ft INT, movement_dir STRING, movement_speed_kt INT, change STRING, "
          "polygon_wkt STRING, polygon_json STRING, min_lon DOUBLE, max_lon DOUBLE, min_lat DOUBLE, max_lat DOUBLE, "
          "in_region BOOLEAN, receipt_time TIMESTAMP, raw_text STRING")


def region_bbox(region, bbox_text):
    """(lamin, lamax, lomin, lomax) of the pipeline region – a preset or the custom bbox."""
    region = (region or "switzerland").lower()
    if region == "custom":
        return tuple(float(x) for x in bbox_text.split(","))
    return REGIONS[region]


def fetch_isigmets(timeout=30):
    """Raw list of international SIGMETs (JSON) – or [] if the API is not reachable."""
    try:
        r = requests.get(URL, params={"format": "json"}, headers={"User-Agent": USER_AGENT}, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else []
    except Exception as e:                       # the pipeline must never fail because of an enrichment source
        print(f"SIGMET request failed ({e}) – returning an empty table")
        return []


def _flight_level_ft(token):
    """'FL320' -> 32000, 'SFC' -> 0, '12000FT' -> 12000, '3000M' -> 9843 (feet)."""
    token = token.strip()
    if token == "SFC":
        return 0
    if token.startswith("FL"):
        return int(token[2:]) * 100
    if token.endswith("FT"):
        return int(token[:-2])
    if token.endswith("M"):
        return round(int(token[:-1]) * 3.28084)
    return None


_LEVEL = r"(SFC|FL\d{3}|\d{3,5}FT|\d{3,5}M)"


def levels_from_text(raw):
    """Fallback when the API's base/top are empty: parse 'FL320/370', 'SFC/FL200', 'TOP FL430', 'ABV FL250'."""
    text = " ".join((raw or "").split())
    m = re.search(rf"\b{_LEVEL}/{_LEVEL}\b", text)
    if m:
        return _flight_level_ft(m.group(1)), _flight_level_ft(m.group(2))
    m = re.search(rf"\b{_LEVEL}/(\d{{3}})\b", text)            # 'FL320/370' – second level without 'FL'
    if m:
        return _flight_level_ft(m.group(1)), int(m.group(2)) * 100
    m = re.search(r"\bTOP (?:ABV |BLW )?FL(\d{3})\b", text)
    if m:
        return None, int(m.group(1)) * 100
    m = re.search(r"\bABV FL(\d{3})\b", text)
    if m:
        return int(m.group(1)) * 100, None
    m = re.search(r"\bBLW FL(\d{3})\b", text)
    if m:
        return 0, int(m.group(1)) * 100
    return None, None


def _ts(unix):
    return datetime.fromtimestamp(unix, tz=timezone.utc) if unix is not None else None


def _int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _areas(coords):
    """The API returns a list of {lon, lat} for one area, or a list of such lists for 'AREAS'."""
    if not coords:
        return []
    if isinstance(coords[0], list):
        return coords
    return [coords]


def parse_sigmets(raw, bbox=None):
    """Raw API list -> one row per SIGMET polygon (pandas). bbox = (lamin, lamax, lomin, lomax) of the region."""
    rows = []
    for s in raw:
        if not isinstance(s, dict) or "firId" not in s:
            continue                             # the API occasionally returns an empty object
        base, top = s.get("base"), s.get("top")
        if base is None and top is None:
            base, top = levels_from_text(s.get("rawSigmet"))
        elif base is None and top is not None:
            base = levels_from_text(s.get("rawSigmet"))[0]      # 'TOP FL420' -> base stays unknown (= surface)
        for area_no, area in enumerate(_areas(s.get("coords")), start=1):
            pts = [[round(float(p["lon"]), 4), round(float(p["lat"]), 4)] for p in area
                   if p.get("lon") is not None and p.get("lat") is not None]
            if len(pts) >= 3 and pts[0] != pts[-1]:
                pts.append(pts[0])               # close the ring
            ok = len(pts) >= 4 and s.get("geom") in ("AREA", "AREAS")
            lons, lats = [p[0] for p in pts], [p[1] for p in pts]
            row = {
                "sigmet_id": f"{s['firId']}-{s.get('seriesId')}-{s.get('validTimeFrom')}",
                "area_no": area_no,
                "icao_id": s.get("icaoId"), "fir_id": s["firId"], "fir_name": s.get("firName"),
                "series_id": s.get("seriesId"),
                "hazard": s.get("hazard"), "hazard_description": HAZARDS.get(s.get("hazard")),
                "qualifier": s.get("qualifier"),
                "valid_from": _ts(s.get("validTimeFrom")), "valid_to": _ts(s.get("validTimeTo")),
                "base_ft": _int(base), "top_ft": _int(top),
                "movement_dir": None if s.get("dir") in (None, "-") else s.get("dir"),
                "movement_speed_kt": _int(s.get("spd")),
                "change": s.get("chng"),        # INTSF intensifying, WKN weakening, NC no change
                "polygon_wkt": ("POLYGON((" + ", ".join(f"{x} {y}" for x, y in pts) + "))") if ok else None,
                "polygon_json": json.dumps(pts) if ok else None,
                "min_lon": min(lons) if ok else None, "max_lon": max(lons) if ok else None,
                "min_lat": min(lats) if ok else None, "max_lat": max(lats) if ok else None,
                "receipt_time": pd.Timestamp(s["receiptTime"]).to_pydatetime() if s.get("receiptTime") else None,
                "raw_text": s.get("rawSigmet"),
            }
            if bbox is not None and ok:
                lamin, lamax, lomin, lomax = bbox
                row["in_region"] = bool(row["min_lat"] <= lamax and row["max_lat"] >= lamin
                                        and row["min_lon"] <= lomax and row["max_lon"] >= lomin)
            else:
                row["in_region"] = False if ok else None
            rows.append(row)
    pdf = pd.DataFrame(rows, columns=[c.split(" ")[0] for c in SCHEMA.split(", ")])
    # The same warning often arrives several times: once per bulletin (series "U03" and "U03F" with identical text)
    # or re-issued under a new number with the same area. Identical FIR + hazard + validity + levels + polygon =
    # one warning: keep the most recently received copy.
    key = ["fir_id", "hazard", "valid_from", "valid_to", "base_ft", "top_ft", "polygon_wkt", "area_no"]
    pdf = pdf.sort_values(["receipt_time", "series_id"]).drop_duplicates(key, keep="last")
    pdf = pdf.drop_duplicates(["sigmet_id", "area_no"], keep="last").sort_values("sigmet_id").reset_index(drop=True)
    for c in ["area_no", "base_ft", "top_ft", "movement_speed_kt"]:
        pdf[c] = pdf[c].astype("Int64")         # nullable integers – otherwise pandas turns them into floats
    return pdf
