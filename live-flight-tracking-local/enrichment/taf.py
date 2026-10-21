# Ported from the Databricks recipe (src_airport_ops/08_silver_taf.py) – the pure pandas logic, without the Spark/pipeline part.
# Settings come from enrichment/config.py (CONF) instead of the pipeline configuration.
# Silver: TAF forecasts of the airports in the region (aviationweather.gov Data API, JSON) – one row per forecast
# period. The METAR (01_silver_metar.py) says what the weather IS, the TAF says what it WILL BE at the airport over
# the next 24–30 hours. Airlines plan fuel and alternates with it. Example (Zurich):
#   TAF LSZH 190525Z 1906/2012 VRB02KT 9999 FEW015 BKN020
#       PROB40 TEMPO 1906/1909 BKN012
#       BECMG 1908/1910 05008KT
#   190525Z = issued on the 19th at 05:25 UTC | 1906/2012 = valid from the 19th 06 UTC to the 20th 12 UTC
#   then change groups, each becomes one row (change_indicator):
#     INITIAL  the base forecast at the start of the validity
#     FM       "from" – conditions change completely at this time (FM191400), common in the US
#     BECMG    "becoming" – a gradual change during period_from .. becmg_complete_at, valid until the next change
#     TEMPO    temporary fluctuations (each < 1 hour, together < half of the period)
#     PROB30/PROB40 – probability in % of a (temporary) condition; "PROB40 TEMPO" = 40 % chance of a TEMPO
# The API already "resolves" FM and BECMG periods: their rows carry the complete forecast state (wind, visibility,
# clouds), while TEMPO/PROB rows only contain the elements that change. period_text is the raw text of the group.
#
# Which airports? The same as the METAR table: ICAO codes of silver.airports in the region bbox, large airports first,
# capped by airport_ops.taf_max_stations (default: the METAR cap airport_ops.metar_max_stations, 600). Small airports
# often have no TAF – the API simply returns fewer TAFs than ids.
#
# API terms and usage guidance (https://aviationweather.gov/data/api/, US National Weather Service, public data):
#   - set a custom User-Agent, keep requests "limited in scope and frequency", max 100 requests per minute
#   - at most 400 entries per response -> we send at most 100 station ids per request, 1 s apart
#   - TAFs are issued every 6 hours (some every 3 h, plus amendments) -> refresh every airport_ops.taf_interval
#     (default "30 minutes")
#   - status 204 = valid request without data
# Settings (pipeline configuration):
#   airport_ops.taf_interval       refresh interval of this table (default "30 minutes")
#   airport_ops.taf_max_stations   cap on stations per refresh (default = airport_ops.metar_max_stations or 600)
import re
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from enrichment.config import CONF

TAF_URL = "https://aviationweather.gov/api/data/taf"
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

SCHEMA = ("station STRING, station_name STRING, latitude DOUBLE, longitude DOUBLE, issue_time TIMESTAMP, "
          "valid_from TIMESTAMP, valid_to TIMESTAMP, period_index INT, change_indicator STRING, probability INT, "
          "period_from TIMESTAMP, period_to TIMESTAMP, becmg_complete_at TIMESTAMP, wind_dir_deg INT, "
          "wind_variable BOOLEAN, wind_speed_kt INT, wind_gust_kt INT, wind_speed_kmh DOUBLE, visibility_m INT, "
          "weather STRING, cloud_layers STRING, ceiling_ft INT, flight_category STRING, period_text STRING, "
          "raw_text STRING, fetched_at TIMESTAMP")
COLUMNS = [c.split()[0] for c in SCHEMA.split(", ")]
INT_COLUMNS = ["period_index", "probability", "wind_dir_deg", "wind_speed_kt", "wind_gust_kt", "visibility_m",
               "ceiling_ft"]
BASE_CHANGES = ("INITIAL", "FM", "BECMG")          # rows that describe the prevailing conditions


def region_bbox(region, bbox):
    """(lamin, lamax, lomin, lomax) of the pipeline region – custom regions use flights.bbox."""
    region = (region or "switzerland").lower()
    if region == "custom":
        return tuple(float(x) for x in bbox.split(","))
    return REGIONS[region]


def stations_in_bbox(pdf_airports, bbox):
    """ICAO codes of the airports (silver.airports) inside the bounding box – large airports first, then by code.
    Same selection as 01_silver_metar.py, so METAR and TAF cover the same airports."""
    lamin, lamax, lomin, lomax = bbox
    inside = pdf_airports[pdf_airports["latitude"].between(lamin, lamax)
                          & pdf_airports["longitude"].between(lomin, lomax)].dropna(subset=["airport_icao"])
    if "airport_type" in inside:
        inside = inside.assign(rank=(inside["airport_type"] != "large_airport").astype(int))
        return inside.sort_values(["rank", "airport_icao"])["airport_icao"].drop_duplicates().tolist()
    return sorted(inside["airport_icao"].unique().tolist())


def fetch_tafs(ids, timeout=30):
    """Current TAFs as a list of dicts (API JSON), by station id in batches of 100, 1 s apart.
    Never raises: a failed batch is printed and skipped."""
    headers = {"User-Agent": USER_AGENT}
    records = []
    batches = [ids[i:i + IDS_PER_REQUEST] for i in range(0, len(ids or []), IDS_PER_REQUEST)]
    for n, batch in enumerate(batches):
        if n:
            time.sleep(1.0)                                            # stay far below 100 requests per minute
        try:
            r = requests.get(TAF_URL, params={"ids": ",".join(batch), "format": "json"}, headers=headers,
                             timeout=timeout)
            if r.status_code == 204:                                   # valid request, no data
                continue
            r.raise_for_status()
            records.extend(r.json())
        except Exception as e:
            print(f"TAF request {n + 1}/{len(batches)} failed: {e}")
    return records


def split_taf_groups(raw):
    """Raw TAF -> list of group texts: [base forecast, 'PROB40 TEMPO 1906/1909 BKN012', 'BECMG 1908/1910 05008KT',
    ...]. 'PROB40 TEMPO' stays together; trailing remarks (RMK ...) are dropped."""
    text = re.sub(r"\s+", " ", raw or "").strip()
    text = re.split(r" RMK ", text)[0]
    return re.split(r" (?=FM\d{6}\b|BECMG\b|PROB\d{2}\b)|(?<!PROB\d\d) (?=TEMPO\b)", text)


def group_visibility_m(group_text):
    """Visibility in metres written in a TAF group: '9999' -> 10000, '4000' -> 4000, CAVOK -> 10000, None if the
    group has no metric visibility (US TAFs use statute miles, e.g. P6SM -> handled via the API value)."""
    if not group_text:
        return None
    if re.search(r"\bCAVOK\b", group_text):
        return 10000
    # a 4-digit token that is not part of a time (190525Z, 1906/1909, FM190600), a wind (05008KT) or a temperature
    # group (TX14/1915Z); QNH groups (QNH2992INS, military TAFs only) are skipped
    m = re.search(r"(?<![\d/A-Z])(\d{4})(?:NDV)?(?![\d/A-Z])", group_text)
    if m:
        return 10000 if m.group(1) == "9999" else int(m.group(1))
    return None


def _visibility_m(group_text, visib_sm, carried=None):
    """Visibility of a period in metres: written in the group -> as written; else the value carried over from the
    previous INITIAL/FM/BECMG period (BECMG 1908/1910 05008KT keeps the visibility); else the API's statute miles
    ('6+' = 6 SM or more ~ 10 km or more), rounded to 100 m."""
    v = group_visibility_m(group_text)
    if v is not None:
        return v
    if carried is not None:
        return carried
    if visib_sm in (None, ""):
        return None
    if str(visib_sm).endswith("+"):
        return 10000
    try:
        return int(round(float(visib_sm) * SM_TO_M, -2))
    except ValueError:
        return None


def ceiling_ft(clouds, vert_vis):
    """Lowest BKN/OVC layer (or vertical visibility) in feet above the airport; None = no ceiling."""
    bases = [c.get("base") for c in (clouds or [])
             if c.get("cover") in ("BKN", "OVC", "OVX") and c.get("base") is not None]
    if vert_vis is not None:
        bases.append(vert_vis)
    return int(min(bases)) if bases else None


def derive_flight_category(ceiling, visibility_m):
    """FAA flight categories, as in 01_silver_metar.py: LIFR < 500 ft / < 1 SM | IFR < 1,000 ft / < 3 SM |
    MVFR <= 3,000 ft / <= 5 SM | else VFR."""
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


def _ts(unix):
    return pd.Timestamp(unix, unit="s", tz="UTC") if unix not in (None, "") else None


def _iso(text):
    return pd.Timestamp(text) if text else None


def parse_tafs(records, fetched_at=None):
    """API JSON records -> one row per forecast period (latest TAF per station), columns of SCHEMA."""
    fetched_at = pd.Timestamp(fetched_at or datetime.now(timezone.utc))
    # several TAFs per station possible (amendments): keep the most recently issued one
    latest = {}
    for rec in records:
        st = rec.get("icaoId")
        if st and (st not in latest or (rec.get("issueTime") or "") > (latest[st].get("issueTime") or "")):
            latest[st] = rec
    rows = []
    for st, rec in latest.items():
        raw = rec.get("rawTAF") or ""
        fcsts = rec.get("fcsts") or []
        groups = split_taf_groups(raw)
        if len(groups) != len(fcsts):                  # unusual TAF layout: keep the API values, no group text
            groups = [None] * len(fcsts)
        prevailing_vis = None                          # visibility of the last INITIAL/FM/BECMG period
        for i, (f, text) in enumerate(zip(fcsts, groups)):
            change = f.get("fcstChange") or ("INITIAL" if i == 0 else None)
            if change is None and f.get("probability"):
                change = "PROB"
            wdir, wspd, wgst = f.get("wdir"), f.get("wspd"), f.get("wgst")
            if change == "BECMG":
                vis = _visibility_m(text, f.get("visib"), carried=prevailing_vis)
            else:
                vis = _visibility_m(text, f.get("visib"))
            if change in BASE_CHANGES:
                prevailing_vis = vis
            ceil = ceiling_ft(f.get("clouds"), f.get("vertVis"))
            clouds = f.get("clouds") or []
            rows.append({
                "station": st,
                "station_name": rec.get("name"),
                "latitude": float(rec["lat"]) if rec.get("lat") is not None else None,
                "longitude": float(rec["lon"]) if rec.get("lon") is not None else None,
                "issue_time": _iso(rec.get("issueTime")),
                "valid_from": _ts(rec.get("validTimeFrom")),
                "valid_to": _ts(rec.get("validTimeTo")),
                "period_index": i,
                "change_indicator": change,
                "probability": f.get("probability"),
                "period_from": _ts(f.get("timeFrom")),
                "period_to": _ts(f.get("timeTo")),
                "becmg_complete_at": _ts(f.get("timeBec")),
                "wind_dir_deg": int(wdir) if isinstance(wdir, (int, float)) else None,
                "wind_variable": wdir == "VRB" if wdir is not None else None,
                "wind_speed_kt": wspd,
                "wind_gust_kt": wgst,
                "wind_speed_kmh": round(float(wspd) * KT_TO_KMH, 1) if wspd is not None else None,
                "visibility_m": vis,
                "weather": f.get("wxString") or None,
                "cloud_layers": " ".join(f"{c.get('cover')}{int(c['base']):,}ft" + (c.get("type") or "")
                                         if c.get("base") is not None else str(c.get("cover"))
                                         for c in clouds) or None,
                "ceiling_ft": ceil,
                # TEMPO/PROB rows carry only what changes – a category from partial data would mislead
                "flight_category": derive_flight_category(ceil, vis) if change in BASE_CHANGES else None,
                "period_text": text,
                "raw_text": raw or None,
                "fetched_at": fetched_at,
            })
    pdf = pd.DataFrame(rows, columns=COLUMNS)
    if pdf.empty:
        return pdf
    for c in INT_COLUMNS:
        pdf[c] = pd.to_numeric(pdf[c], errors="coerce").round().astype("Int64")
    return pdf.sort_values(["station", "period_index"]).reset_index(drop=True)


def taf_at(pdf_taf, at):
    """Forecast valid at a point in time, per station (for the app: "TAF for the next hour"): the prevailing period
    (INITIAL/FM/BECMG row with period_from <= at < period_to) plus a text of the TEMPO/PROB groups active then."""
    at = pd.Timestamp(at)
    if pdf_taf is None or pdf_taf.empty:
        return pd.DataFrame(columns=COLUMNS + ["also_possible"])
    active = pdf_taf[(pdf_taf["period_from"] <= at) & (pdf_taf["period_to"] > at)]
    base = active[active["change_indicator"].isin(BASE_CHANGES)].sort_values("period_from")
    base = base.drop_duplicates("station", keep="last")
    temp = active[~active["change_indicator"].isin(BASE_CHANGES)]
    extra = temp.groupby("station")["period_text"].apply(lambda s: " | ".join(s.dropna())).rename("also_possible")
    return base.merge(extra, on="station", how="left").reset_index(drop=True)


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
