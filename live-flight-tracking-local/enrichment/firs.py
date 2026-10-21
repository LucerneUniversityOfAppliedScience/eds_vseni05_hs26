# Ported from the Databricks recipe (src_geo/06_silver_firs.py) – the pure pandas logic, without the Spark/pipeline part.
# Settings come from enrichment/config.py (CONF) instead of the pipeline configuration.
# Silver reference: Flight Information Regions (FIRs) as polygons – the basis for "which air traffic control region is
# the aircraft in?" and "does this flight cross closed or avoided airspace?" (07_silver_restricted_airspace.py,
# 08_gold_fir_traffic.py).
#
# What is a FIR? The world's airspace is divided into Flight Information Regions. Each one is run by one air
# navigation service provider and has a 4-letter ICAO code: the first one or two letters say the country
# (LS = Switzerland, UK = Ukraine, UU/UL/UW/... = Russia, OI = Iran). Airspace closures and conflict-zone warnings
# are published per FIR, which is why FIRs – and not country borders – are the right unit to explain detours.
#
# Source: VATSIM "VAT-Spy Client Data Update Project" on GitHub (github.com/vatsimnetwork/vatspy-data-project), file
# Boundaries.geojson (~2.2 MB, ~1,200 boundaries, updated every AIRAC cycle), plus VATSpy.dat for the FIR names and
# country names. raw.githubusercontent.com is on the Free Edition allow-list.
# LICENCE: Creative Commons Attribution-ShareAlike 4.0 (CC BY-SA 4.0). Attribution required:
#   "FIR boundaries: VATSIM VAT-Spy Data Project, CC BY-SA 4.0" – show it next to any map / dashboard that uses them.
#   Share-alike: if you publish a modified version of the boundaries themselves, it must be under CC BY-SA too.
#
# Read before you quote a number:
#   * These are boundaries of the VATSIM flight-simulation network, maintained by volunteers. They follow the real
#     FIRs closely (they are drawn from the national AIPs) but they are NOT an official source – never use them for
#     real flight planning.
#   * Boundaries.geojson mixes several levels: real FIRs (LSAS, UKBV, OIIX), sub-sectors ("UUWV-G1", "EDGG-..."),
#     a few aggregates without an ICAO code ("UKR" = all of Ukraine, "ADR", "NAT", "GUY") and some terminal areas
#     and upper areas that overlap their FIR (OEJN Jeddah terminal inside OEJD, OERD "from FL345").
#     We keep only ids that look like an ICAO code (4 letters, no suffix) – that drops sub-sectors and aggregates.
#     Some overlap remains (terminal areas, upper areas, Australian / Indian sectors). 08_gold_fir_traffic.py therefore
#     assigns a position to the SMALLEST boundary that contains it ("most specific wins") – see area_sq_deg.
#   * FIRs are 2-D here: real FIRs are often split vertically (lower FIR / upper UIR). We ignore altitude.
#   * Boundaries that cross the antimeridian (UHMM Magadan, PAZA Anchorage, NZZO Auckland Oceanic, KZAK Oakland
#     Oceanic) are split into two parts at +-180 degrees, as RFC 7946 recommends – no special handling needed.
#
# Region: unlike silver.countries, ALL FIRs of the world are kept (~420 rows, ~1.3 MB of GeoJSON – small) and
# in_region marks those overlapping the pipeline region (flights.region / flights.bbox, plus a margin). Why? A flight
# Paris -> Tokyo seen over Switzerland must still be checked against the Russian FIRs on its great circle
# (gold.route_restrictions). Per-aircraft lookups (gold.current_firs) only use the in_region FIRs.
# Coordinates are rounded to 4 decimals (~10 m). The geometry stays a GeoJSON string (MultiPolygon):
# Python reads it with json.loads, Databricks SQL with ST_GeomFromGeoJSON(geometry_geojson).
#
# Setting: geo.firs_interval (default "24 hours") – boundaries change at most every AIRAC cycle (28 days).
import json
import re

import pandas as pd
import requests
from enrichment.config import CONF

CATALOG = CONF.get("flights.catalog", "opensky")
SILVER = CONF.get("flights.silver_schema", "silver")
BASE_URL = "https://raw.githubusercontent.com/vatsimnetwork/vatspy-data-project/master/"
BOUNDARIES_URL = BASE_URL + "Boundaries.geojson"
NAMES_URL = BASE_URL + "VATSpy.dat"
USER_AGENT = "databricks-cookbook-flight-tracking/1.0 (teaching demo)"
ATTRIBUTION = "FIR boundaries: VATSIM VAT-Spy Data Project (github.com/vatsimnetwork/vatspy-data-project), CC BY-SA 4.0"
MARGIN_DEG = 2.0                     # FIRs are big: keep the neighbours of the region too
ICAO_ID = re.compile(r"^[A-Z]{4}$")  # a real FIR code; sub-sectors ("UUWV-G1") and aggregates ("UKR") are dropped
# The country comes from the ICAO prefix (first two letters, else the first letter: K = USA). Exceptions:
COUNTRY_OVERRIDES = {"UMKK": "Russia"}   # Kaliningrad has a Belarusian-looking prefix (UM) but is Russian

# lamin, lamax, lomin, lomax – same presets as src/01_bronze_flights_raw.py
REGIONS = {
    "switzerland": (45.6, 48.0, 5.8, 10.7),
    "alps": (43.5, 49.0, 5.0, 16.5),
    "europe": (35.0, 72.0, -25.0, 45.0),
    "east": (36.0, 48.0, 26.0, 50.0),
    "world": (-90.0, 90.0, -180.0, 180.0),
}

SCHEMA = ("fir_icao STRING, fir_name STRING, country STRING, vatsim_region STRING, oceanic BOOLEAN, "
          "in_region BOOLEAN, label_lat DOUBLE, label_lon DOUBLE, min_lon DOUBLE, max_lon DOUBLE, min_lat DOUBLE, "
          "max_lat DOUBLE, area_sq_deg DOUBLE, polygons INT, vertices INT, geometry_geojson STRING, source STRING")


def _cols(schema):
    return [c.split(" ")[0] for c in schema.split(", ")]


def region_bbox(region, bbox_text):
    """(lamin, lamax, lomin, lomax) of the pipeline region – a preset or the custom bbox."""
    region = (region or "switzerland").lower()
    if region == "custom":
        return tuple(float(x) for x in bbox_text.split(","))
    return REGIONS[region]


def _get(url, timeout):
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
        r.raise_for_status()
        return r
    except Exception as e:                       # the pipeline must never fail because of an enrichment source
        print(f"FIR download failed: {url} ({e})")
        return None


def fetch_boundaries(timeout=60):
    """Boundaries.geojson (dict) – or None if it cannot be downloaded."""
    r = _get(BOUNDARIES_URL, timeout)
    try:
        return r.json() if r is not None else None
    except ValueError as e:
        print(f"FIR boundaries are not valid JSON ({e})")
        return None


def fetch_names(timeout=60):
    """VATSpy.dat (text) – only used for names; None is fine (names stay NULL)."""
    r = _get(NAMES_URL, timeout)
    return r.text if r is not None else None


def parse_vatspy_dat(text):
    """VATSpy.dat -> (fir_names {boundary id: name}, country_names {ICAO prefix: country}).

    The file is a pipe-separated INI-like text: [Countries] "Name|ICAO prefix|suffix" and
    [FIRs] "ICAO|NAME|CALLSIGN PREFIX|FIR BOUNDARY". Several [FIRs] lines can point to the same boundary
    (one per controller callsign); the line whose ICAO equals the boundary id wins.
    """
    fir_names, countries, section = {}, {}, None
    for line in (text or "").splitlines():
        line = line.strip()
        if not line or line.startswith(";"):
            continue
        if line.startswith("["):
            section = line.strip("[]")
            continue
        parts = line.split("|")
        if section == "Countries" and len(parts) >= 2 and parts[1]:
            countries.setdefault(parts[1], parts[0])
        elif section == "FIRs" and len(parts) >= 4:
            icao, name, boundary = parts[0], parts[1], parts[3] or parts[0]
            if boundary not in fir_names or icao == boundary:
                fir_names[boundary] = name
    return fir_names, countries


def _ring_area(ring):
    """Shoelace area of a ring in square degrees (only used to order overlapping boundaries – not km²)."""
    a = 0.0
    for (x1, y1), (x2, y2) in zip(ring, ring[1:] + ring[:1]):
        a += x1 * y2 - x2 * y1
    return abs(a) / 2


def build_firs(geojson, bbox, names_text=None, margin=MARGIN_DEG):
    """Boundaries.geojson (+ VATSpy.dat text) -> one row per FIR of the world; in_region = overlaps bbox
    (lamin, lamax, lomin, lomax) plus margin."""
    cols = _cols(SCHEMA)
    if not geojson:
        return pd.DataFrame(columns=cols)
    fir_names, country_names = parse_vatspy_dat(names_text)
    lamin, lamax, lomin, lomax = bbox[0] - margin, bbox[1] + margin, bbox[2] - margin, bbox[3] + margin
    # 1. collect the polygon parts per FIR code. The same code can appear in several features (KZNY = New York
    #    domestic AND New York oceanic, CZQM, ...) – they are merged into one MultiPolygon, not dropped.
    parts_by_fir, props_by_fir = {}, {}
    for f in geojson.get("features", []):
        geom, props = f.get("geometry") or {}, f.get("properties") or {}
        fir = str(props.get("id") or "")
        if not ICAO_ID.match(fir):
            continue
        if geom.get("type") == "Polygon":
            parts = [geom["coordinates"]]
        elif geom.get("type") == "MultiPolygon":
            parts = geom["coordinates"]
        else:
            continue
        parts_by_fir.setdefault(fir, []).extend(parts)
        if fir not in props_by_fir or str(props_by_fir[fir].get("oceanic")) == "1":   # prefer the domestic label
            props_by_fir[fir] = props

    # 2. one row per FIR
    rows = []
    for fir, parts in parts_by_fir.items():
        props = props_by_fir[fir]
        kept, in_region = [], False
        for poly in parts:                       # poly = [exterior ring, hole, hole, ...]
            lons = [p[0] for p in poly[0]]
            lats = [p[1] for p in poly[0]]
            in_region |= min(lats) <= lamax and max(lats) >= lamin and min(lons) <= lomax and max(lons) >= lomin
            kept.append([[[round(x, 4), round(y, 4)] for x, y, *_ in ring] for ring in poly])
        lons = [p[0] for poly in kept for p in poly[0]]
        lats = [p[1] for poly in kept for p in poly[0]]
        area = sum(_ring_area(poly[0]) - sum(_ring_area(h) for h in poly[1:]) for poly in kept)
        rows.append({
            "fir_icao": fir,
            "fir_name": fir_names.get(fir),
            "country": COUNTRY_OVERRIDES.get(fir) or country_names.get(fir[:2]) or country_names.get(fir[:1]),
            "vatsim_region": props.get("region"),
            "oceanic": str(props.get("oceanic")) == "1",      # TRUE only if all parts are oceanic
            "in_region": bool(in_region),
            "label_lat": float(props["label_lat"]) if props.get("label_lat") else None,
            "label_lon": float(props["label_lon"]) if props.get("label_lon") else None,
            "min_lon": min(lons), "max_lon": max(lons), "min_lat": min(lats), "max_lat": max(lats),
            "area_sq_deg": round(area, 3),
            "polygons": len(kept),
            "vertices": sum(len(ring) for poly in kept for ring in poly),
            "geometry_geojson": json.dumps({"type": "MultiPolygon", "coordinates": kept}, separators=(",", ":")),
            "source": ATTRIBUTION,
        })
    return pd.DataFrame(rows, columns=cols).sort_values("fir_icao").reset_index(drop=True)
