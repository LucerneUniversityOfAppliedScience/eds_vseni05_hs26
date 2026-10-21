# Ported from the Databricks recipe (src_geo/03_silver_countries.py) – the pure pandas logic, without the Spark/pipeline part.
# Settings come from enrichment/config.py (CONF) instead of the pipeline configuration.
# Silver reference: country borders as polygons – the basis for "which country is the aircraft flying over?"
# (04_gold_country_overflights.py).
#
# Source: Natural Earth "Admin 0 – Countries" at scale 1:50m, as GeoJSON from the official Natural Earth GitHub
# repository (raw.githubusercontent.com is on the Free Edition allow-list). Natural Earth is public domain – no
# attribution required, "Made with Natural Earth" is appreciated. Why 1:50m? ~3 MB for the whole world and accurate
# to a few hundred metres – enough to tell Switzerland from its neighbours (1:110m is too coarse: no Liechtenstein,
# and borders are off by kilometres; the 1:10m files and datasets/geo-countries are ~14 MB).
#
# Only countries – and only those polygon parts – that overlap the pipeline's region (flights.region / flights.bbox,
# plus a margin) are kept: overseas territories like French Guiana or Réunion are dropped for a European region.
# Coordinates are rounded to 4 decimals (~10 m). The geometry stays a GeoJSON string: Python reads it with
# json.loads, Databricks SQL with ST_GeomFromGeoJSON(geometry_geojson).
#
# Country codes: Natural Earth sets ISO_A2 = -99 for a few countries (France, Norway, Kosovo, ...); the "_EH"
# columns fill them in. Disputed areas without a code (Northern Cyprus, Somaliland) keep the Natural Earth code ADM0_A3.
#
# Setting: geo.countries_interval (default "24 hours") – borders hardly change.
import json

import pandas as pd
import requests
from enrichment.config import CONF

CATALOG = CONF.get("flights.catalog", "opensky")
SILVER = CONF.get("flights.silver_schema", "silver")
URL = ("https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/"
       "ne_50m_admin_0_countries.geojson")
USER_AGENT = "databricks-cookbook-flight-tracking/1.0 (teaching demo)"
MARGIN_DEG = 1.0                                  # keep countries slightly outside the region too

# lamin, lamax, lomin, lomax – same presets as src/01_bronze_flights_raw.py
REGIONS = {
    "switzerland": (45.6, 48.0, 5.8, 10.7),
    "alps": (43.5, 49.0, 5.0, 16.5),
    "europe": (35.0, 72.0, -25.0, 45.0),
    "east": (36.0, 48.0, 26.0, 50.0),
    "world": (-90.0, 90.0, -180.0, 180.0),
}

SCHEMA = ("country_iso2 STRING, country_iso3 STRING, country_name STRING, continent STRING, "
          "min_lon DOUBLE, max_lon DOUBLE, min_lat DOUBLE, max_lat DOUBLE, polygons INT, vertices INT, "
          "geometry_geojson STRING")


def region_bbox(region, bbox_text):
    """(lamin, lamax, lomin, lomax) of the pipeline region – a preset or the custom bbox."""
    region = (region or "switzerland").lower()
    if region == "custom":
        return tuple(float(x) for x in bbox_text.split(","))
    return REGIONS[region]


def fetch_countries(timeout=60):
    """The Natural Earth GeoJSON (dict) – or None if it cannot be downloaded."""
    try:
        r = requests.get(URL, headers={"User-Agent": USER_AGENT}, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:                       # the pipeline must never fail because of an enrichment source
        print(f"Country borders download failed ({e}) – returning an empty table")
        return None


def _code(props, *keys):
    for k in keys:
        v = props.get(k)
        if v and v != "-99":
            return v
    return None


def build_countries(geojson, bbox, margin=MARGIN_DEG):
    """GeoJSON FeatureCollection -> one row per country overlapping bbox (lamin, lamax, lomin, lomax), pandas."""
    cols = [c.split(" ")[0] for c in SCHEMA.split(", ")]
    if not geojson:
        return pd.DataFrame(columns=cols)
    lamin, lamax, lomin, lomax = bbox[0] - margin, bbox[1] + margin, bbox[2] - margin, bbox[3] + margin
    rows = []
    for f in geojson.get("features", []):
        geom, props = f.get("geometry") or {}, f.get("properties") or {}
        if geom.get("type") == "Polygon":
            parts = [geom["coordinates"]]
        elif geom.get("type") == "MultiPolygon":
            parts = geom["coordinates"]
        else:
            continue
        kept = []
        for poly in parts:                       # poly = [exterior ring, hole, hole, ...]
            lons = [p[0] for p in poly[0]]
            lats = [p[1] for p in poly[0]]
            if min(lats) <= lamax and max(lats) >= lamin and min(lons) <= lomax and max(lons) >= lomin:
                kept.append([[[round(x, 4), round(y, 4)] for x, y, *_ in ring] for ring in poly])
        if not kept:
            continue
        lons = [p[0] for poly in kept for p in poly[0]]
        lats = [p[1] for poly in kept for p in poly[0]]
        rows.append({
            "country_iso2": _code(props, "ISO_A2_EH", "ISO_A2", "ADM0_A3"),
            "country_iso3": _code(props, "ISO_A3_EH", "ISO_A3", "ADM0_A3"),
            "country_name": props.get("NAME") or props.get("ADMIN"),
            "continent": props.get("CONTINENT"),
            "min_lon": min(lons), "max_lon": max(lons), "min_lat": min(lats), "max_lat": max(lats),
            "polygons": len(kept),
            "vertices": sum(len(ring) for poly in kept for ring in poly),
            "geometry_geojson": json.dumps({"type": "MultiPolygon", "coordinates": kept}, separators=(",", ":")),
        })
    return pd.DataFrame(rows, columns=cols)
