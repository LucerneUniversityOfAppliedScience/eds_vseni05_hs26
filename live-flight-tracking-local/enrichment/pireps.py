# Ported from the Databricks recipe (src_airport_ops/09_silver_pireps.py) – the pure pandas logic, without the Spark/pipeline part.
# Settings come from enrichment/config.py (CONF) instead of the pipeline configuration.
# Silver: pilot reports (PIREP / AIREP) – what pilots actually experienced in the air: turbulence, icing, wind,
# temperature. One row per report, with a normalised severity for map colours.
#
#   PIREP  (North America) "BDR UA /OV BDR/TM 0702/FL230/TP B752/TB OCNL LGT CHOP"
#          UA = routine, UUA = urgent | /OV over (or radial/distance from) BDR | /TM 07:02 UTC | /FL230 = 23,000 ft
#          /TP aircraft type B752 | /TB turbulence: occasional light chop | /IC icing, /TA temperature, /SK sky
#   AIREP  (ICAO, mostly oceanic) "ARP UAL932 5434N02527W 0731 F370 ... MS52 223/78 KT"
#          callsign, position 54°34'N 025°27'W, time, FL370, temperature -52 °C, wind 223° at 78 kt. Most AIREPs are
#          automatic position reports (ADS-C/CPDLC over the North Atlantic) without any turbulence/icing remark.
# Coverage (measured on a Monday morning UTC): the US/Canada deliver hundreds of PIREPs per day (more than 400 in
# 12 hours even overnight); Europe has almost no PIREP culture in the public feed – over Europe the data are North
# Atlantic AIREPs west of Ireland (~280 per hour) plus a handful with turbulence remarks. Switzerland and the Alps
# had 0 reports in 3 hours; a 500-mile radius around LSZH gave 7 AIREPs in 24 hours. Expect an empty table for
# region = switzerland most of the time – the module is most useful with region = world or a US custom bbox.
#
# Severity (column severity, for map colours): the worst of turbulence and icing intensity
#   none (NEG, SMTH) | light (LGT, TRC, SMTH-LGT, TRC-LGT) | moderate (LGT-MOD, MOD) | severe (MOD-SEV, SEV, HVY,
#   EXTM) | "not reported" = the report has no turbulence/icing information (e.g. routine AIREP position reports)
#
# Source: aviationweather.gov (US National Weather Service, public data, https://aviationweather.gov/data/api/):
#   - small regions: Data API /api/data/pirep?bbox=lat0,lon0,lat1,lon1&age=<hours>&format=json. A bbox (or id +
#     distance) is REQUIRED, at most 400 reports per response -> a full response is split into 4 quadrants if
#     airport_ops.pirep_max_requests allows (requests 1 s apart)
#   - large regions (europe, world: bbox > 1,000 sq deg): the bulk cache file
#     https://aviationweather.gov/data/cache/aircraftreports.cache.xml.gz – all reports of the last ~1.5 hours
#     worldwide, ~120 KB, updated every minute. AWC asks to use the cache files instead of many API calls for large
#     downloads: one request instead of dozens of tiles, filtered to the bbox here
#   - custom User-Agent, max 100 requests per minute, keep requests limited in scope and frequency
# Settings (pipeline configuration):
#   airport_ops.pirep_interval       refresh interval (default "10 minutes")
#   airport_ops.pirep_age_hours      look-back in hours for the API (default 6; the cache always has ~1.5 hours)
#   airport_ops.pirep_source         auto | api | cache (default auto = cache for bboxes > 1,000 sq deg, else api)
#   airport_ops.pirep_max_requests   cap on API requests per refresh (default 5 = 1 + one split into 4 quadrants)
import gzip
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import pandas as pd
import requests
from enrichment.config import CONF

PIREP_URL = "https://aviationweather.gov/api/data/pirep"
CACHE_URL = "https://aviationweather.gov/data/cache/aircraftreports.cache.xml.gz"
USER_AGENT = "databricks-cookbook-flight-tracking/1.0 (teaching demo)"
API_MAX_RESULTS = 400
CACHE_MIN_SQ_DEG = 1000            # source auto: boxes larger than this (europe ~2,600, world 64,800) use the cache

# lamin, lamax, lomin, lomax – identical to the presets of src/01_bronze_flights_raw.py
REGIONS = {
    "switzerland": (45.6, 48.0, 5.8, 10.7),
    "alps": (43.5, 49.0, 5.0, 16.5),
    "europe": (35.0, 72.0, -25.0, 45.0),
    "east": (36.0, 48.0, 26.0, 50.0),
    "world": (-90.0, 90.0, -180.0, 180.0),
}

# intensity code -> rank (0 none, 1 light, 2 moderate, 3 severe); "LGT-MOD" counts as the worse value
INTENSITY_RANK = {"NEG": 0, "NIL": 0, "NONE": 0, "SMTH": 0, "SMOOTH": 0, "CLR": 0,
                  "TRC": 1, "TRACE": 1, "LGT": 1, "LIGHT": 1, "SMTH-LGT": 1, "TRC-LGT": 1,
                  "LGT-MOD": 2, "MOD": 2, "MODERATE": 2,
                  "MOD-SEV": 3, "SEV": 3, "SEVERE": 3, "HVY": 3, "MOD-HVY": 3, "SEV-EXTM": 3, "EXTM": 3}
SEVERITY_LABEL = {0: "none", 1: "light", 2: "moderate", 3: "severe"}

SCHEMA = ("observed_at TIMESTAMP, received_at TIMESTAMP, report_type STRING, urgent BOOLEAN, aircraft_type STRING, "
          "callsign STRING, latitude DOUBLE, longitude DOUBLE, altitude_ft INT, altitude_note STRING, "
          "temperature_c DOUBLE, wind_dir_deg INT, wind_speed_kt INT, turbulence_intensity STRING, "
          "turbulence_type STRING, turbulence_freq STRING, turbulence_base_ft INT, turbulence_top_ft INT, "
          "icing_intensity STRING, icing_type STRING, icing_base_ft INT, icing_top_ft INT, turbulence_rank INT, "
          "icing_rank INT, severity_rank INT, severity STRING, hazard_text STRING, raw_text STRING, source STRING, "
          "fetched_at TIMESTAMP")
COLUMNS = [c.split()[0] for c in SCHEMA.split(", ")]
INT_COLUMNS = ["altitude_ft", "wind_dir_deg", "wind_speed_kt", "turbulence_base_ft", "turbulence_top_ft",
               "icing_base_ft", "icing_top_ft", "turbulence_rank", "icing_rank", "severity_rank"]


def region_bbox(region, bbox):
    """(lamin, lamax, lomin, lomax) of the pipeline region – custom regions use flights.bbox."""
    region = (region or "switzerland").lower()
    if region == "custom":
        return tuple(float(x) for x in bbox.split(","))
    return REGIONS[region]


def fetch_pireps_api(bbox, age_hours=6, max_requests=5, timeout=30):
    """PIREPs/AIREPs in a bbox from the Data API as a list of dicts (API JSON). A response with 400 reports is
    probably cut off -> the box is split into 4 quadrants (while requests are left). Never raises."""
    headers = {"User-Agent": USER_AGENT}
    queue, records, n = [tuple(bbox)], [], 0
    while queue and n < max_requests:
        lamin, lamax, lomin, lomax = queue.pop(0)
        if n:
            time.sleep(1.0)                                            # stay far below 100 requests per minute
        n += 1
        try:
            r = requests.get(PIREP_URL, headers=headers, timeout=timeout,
                             params={"bbox": f"{lamin},{lomin},{lamax},{lomax}", "age": age_hours, "format": "json"})
            if r.status_code == 204:                                   # valid request, no data
                continue
            r.raise_for_status()
            batch = r.json()
        except Exception as e:
            print(f"PIREP request {n} failed: {e}")
            continue
        if len(batch) >= API_MAX_RESULTS and n + len(queue) + 4 <= max_requests:
            mlat, mlon = (lamin + lamax) / 2, (lomin + lomax) / 2      # cut off: ask again in 4 smaller boxes
            queue += [(lamin, mlat, lomin, mlon), (lamin, mlat, mlon, lomax),
                      (mlat, lamax, lomin, mlon), (mlat, lamax, mlon, lomax)]
            continue
        if len(batch) >= API_MAX_RESULTS:
            print(f"PIREP: {API_MAX_RESULTS} reports in one box – probably incomplete "
                  "(raise airport_ops.pirep_max_requests or use the cache)")
        records.extend(batch)
    return records


def _int_or_none(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _float_or_none(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def cache_xml_to_records(xml_bytes):
    """aircraftreports.cache.xml(.gz) -> list of dicts with the SAME keys as the API JSON (so parse_pireps() handles
    both). The XML gives bases/tops in feet, the API JSON in hundreds of feet -> converted to hundreds here."""
    if xml_bytes[:2] == b"\x1f\x8b":
        xml_bytes = gzip.decompress(xml_bytes)
    root = ET.fromstring(xml_bytes)
    records = []
    for rep in root.iter("AircraftReport"):
        def txt(tag):
            el = rep.find(tag)
            return el.text if el is not None else None
        obs = txt("observation_time")
        alt = _int_or_none(txt("altitude_ft_msl"))
        rec = {"obsTime": int(pd.Timestamp(obs).timestamp()) if obs else None, "receiptTime": txt("receipt_time"),
               "acType": txt("aircraft_ref"), "lat": float(txt("latitude")), "lon": float(txt("longitude")),
               "fltLvl": alt / 100 if alt is not None else None, "temp": txt("temp_c"),
               "wdir": txt("wind_dir_degrees"), "wspd": txt("wind_speed_kt"), "pirepType": txt("report_type"),
               "rawOb": txt("raw_text"), "fltLvlType": None}
        for prefix, tag, attr in (("tb", "turbulence_condition", "turbulence"), ("icg", "icing_condition", "icing")):
            for i, el in enumerate(rep.findall(tag)[:2], start=1):
                bas, top = _int_or_none(el.get(f"{attr}_base_ft_msl")), _int_or_none(el.get(f"{attr}_top_ft_msl"))
                rec[f"{prefix}Int{i}"] = el.get(f"{attr}_intensity")
                rec[f"{prefix}Type{i}"] = el.get(f"{attr}_type")
                rec[f"{prefix}Bas{i}"] = bas / 100 if bas is not None else None
                rec[f"{prefix}Top{i}"] = top / 100 if top is not None else None
                if prefix == "tb":
                    rec[f"tbFreq{i}"] = el.get("turbulence_freq")
        records.append(rec)
    return records


def fetch_pireps_cache(timeout=60):
    """All PIREPs/AIREPs of the last ~1.5 hours worldwide from the AWC cache file (one request). Never raises."""
    try:
        r = requests.get(CACHE_URL, headers={"User-Agent": USER_AGENT}, timeout=timeout)
        r.raise_for_status()
        return cache_xml_to_records(r.content)
    except Exception as e:
        print(f"PIREP cache download failed: {e}")
        return []


def canonical_intensity(code):
    """'MOD' -> 'MOD', 'NEGclr' -> 'NEG', 'moderate' -> 'MODERATE'; unknown codes / '' / None -> None."""
    if not code:
        return None
    c = str(code).strip().upper()
    if c in INTENSITY_RANK:
        return c
    for prefix in sorted(INTENSITY_RANK, key=len, reverse=True):    # longest match first: 'NEGCLR' -> 'NEG'
        if c.startswith(prefix):
            return prefix
    return None


def intensity_rank(code):
    """'MOD' -> 2, 'LGT-MOD' -> 2, 'NEGclr' -> 0, '' / None -> None (not reported)."""
    c = canonical_intensity(code)
    return INTENSITY_RANK[c] if c else None


def _worst(rec, prefix):
    """The worse of the two reported layers (tbInt1/tbInt2, icgInt1/icgInt2): (rank, intensity, type, base, top)."""
    best = (None, None, None, None, None)
    for i in (1, 2):
        intensity = rec.get(f"{prefix}Int{i}") or None
        rank = intensity_rank(intensity)
        if rank is not None and (best[0] is None or rank > best[0]):
            bas, top = rec.get(f"{prefix}Bas{i}"), rec.get(f"{prefix}Top{i}")
            best = (rank, canonical_intensity(intensity), rec.get(f"{prefix}Type{i}") or None,
                    int(round(float(bas) * 100)) if bas not in (None, "") else None,
                    int(round(float(top) * 100)) if top not in (None, "") else None)
    return best


def _layer_text(kind, intensity, typ, base, top):
    if intensity is None:
        return None
    levels = f" {base:,}–{top:,} ft" if base and top else f" at {base or top:,} ft" if (base or top) else ""
    return f"{intensity} {typ + ' ' if typ else ''}{kind}{levels}"


def parse_pireps(records, bbox=None, source="api", fetched_at=None):
    """API JSON records (or cache_xml_to_records() output) -> one row per report inside the bbox, columns of SCHEMA."""
    fetched_at = pd.Timestamp(fetched_at or datetime.now(timezone.utc))
    rows = []
    for rec in records:
        lat, lon = rec.get("lat"), rec.get("lon")
        if lat is None or lon is None:
            continue
        if bbox is not None and not (bbox[0] <= lat <= bbox[1] and bbox[2] <= lon <= bbox[3]):
            continue
        raw = (rec.get("rawOb") or "").strip()
        rtype = rec.get("pirepType") or ("AIREP" if raw.startswith("ARP") else "PIREP")
        urgent = rtype.upper().startswith("URGENT") or " UUA " in f" {raw} "
        tb = _worst(rec, "tb")
        ic = _worst(rec, "icg")
        ranks = [r for r in (tb[0], ic[0]) if r is not None]
        sev = max(ranks) if ranks else None
        is_airep = rtype == "AIREP" or raw.startswith("ARP")
        m = re.match(r"ARP\s+(\S+)", raw)
        lvl = rec.get("fltLvl")
        obs = rec.get("obsTime")
        rows.append({
            "observed_at": pd.Timestamp(obs, unit="s", tz="UTC") if obs else None,
            "received_at": pd.Timestamp(rec["receiptTime"]) if rec.get("receiptTime") else None,
            "report_type": "PIREP" if rtype == "Urgent PIREP" else rtype,
            "urgent": urgent,
            # acType holds the aircraft type in PIREPs (/TP B752) but the callsign in AIREPs
            "aircraft_type": None if is_airep else rec.get("acType"),
            "callsign": m.group(1) if (is_airep and m) else None,
            "latitude": float(lat),
            "longitude": float(lon),
            "altitude_ft": int(round(float(lvl) * 100)) if lvl not in (None, "") else None,
            "altitude_note": {"DURD": "during descent", "DURC": "during climb", "GRND": "on ground",
                              "UNKN": "unknown"}.get(rec.get("fltLvlType")),
            "temperature_c": _float_or_none(rec.get("temp")),
            "wind_dir_deg": _int_or_none(rec.get("wdir")),
            "wind_speed_kt": _int_or_none(rec.get("wspd")),
            "turbulence_intensity": tb[1], "turbulence_type": tb[2],
            "turbulence_freq": rec.get("tbFreq1") or None,
            "turbulence_base_ft": tb[3], "turbulence_top_ft": tb[4],
            "icing_intensity": ic[1], "icing_type": ic[2], "icing_base_ft": ic[3], "icing_top_ft": ic[4],
            "turbulence_rank": tb[0], "icing_rank": ic[0], "severity_rank": sev,
            "severity": SEVERITY_LABEL.get(sev, "not reported"),
            "hazard_text": "; ".join(t for t in (_layer_text("turbulence", *tb[1:]), _layer_text("icing", *ic[1:]))
                                     if t) or None,
            "raw_text": raw or None,
            "source": source,
            "fetched_at": fetched_at,
        })
    pdf = pd.DataFrame(rows, columns=COLUMNS)
    if pdf.empty:
        return pdf
    pdf = pdf.drop_duplicates(["observed_at", "raw_text"])
    for c in INT_COLUMNS:
        pdf[c] = pd.to_numeric(pdf[c], errors="coerce").round().astype("Int64")
    return pdf.sort_values("observed_at", ascending=False).reset_index(drop=True)


def bbox_area_sq_deg(bbox):
    return (bbox[1] - bbox[0]) * (bbox[3] - bbox[2])


def fetch_and_parse_pireps(bbox, source="auto", age_hours=6, max_requests=5):
    """Convenience for the pipeline and the app: pick the source, fetch, parse, filter to the bbox.
    auto = the cache file for large boxes (> CACHE_MIN_SQ_DEG, i.e. europe and world: the API would cut them off at
    400 reports – mostly routine North Atlantic AIREPs), the API with age_hours look-back for small regions."""
    use_cache = source == "cache" or (source == "auto" and bbox_area_sq_deg(bbox) > CACHE_MIN_SQ_DEG)
    if use_cache:
        return parse_pireps(fetch_pireps_cache(), bbox, source="cache")
    return parse_pireps(fetch_pireps_api(bbox, age_hours, max_requests), bbox, source="api")


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
