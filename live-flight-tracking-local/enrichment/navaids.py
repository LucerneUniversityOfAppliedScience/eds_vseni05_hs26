# Ported from the Databricks recipe (src_airport_ops/06_silver_navaids.py) – the pure pandas logic, without the Spark/pipeline part.
# Settings come from enrichment/config.py (CONF) instead of the pipeline configuration.
# Silver reference: radio navigation aids (navaids) in the region – one row per navaid.
#
# Before satellite navigation, aircraft flew from radio beacon to radio beacon, and many airways and arrival/departure
# routes are still anchored on them. A pilot tunes the frequency, checks the Morse identifier ("ZUE") and the cockpit
# shows the direction (VOR, NDB) and/or the distance (DME) to the station. Types in the data:
#   VOR      VHF omnidirectional range, 108–118 MHz: "which radial from the station am I on?"
#   DME      distance measuring equipment: slant distance to the station in nautical miles
#   VOR-DME  both on one site (the most common en-route navaid in Europe, e.g. ZUE Zurich East, KLO Kloten)
#   TACAN    military counterpart of VOR-DME;  VORTAC = VOR + TACAN (very common in the US)
#   NDB      non-directional beacon, 190–1750 kHz, the oldest type: the cockpit needle points to the station
#   NDB-DME  NDB with a co-located DME
# frequency_khz: VOR/DME/TACAN frequencies are in kHz too (110050 = 110.05 MHz) – frequency_label formats them.
#
# Source: OurAirports navaids.csv (https://ourairports.com/data/, mirrored on davidmegginson.github.io), Public
# Domain, ~11,000 navaids worldwide, maintained by volunteers – good for maps and teaching, NOT for navigation
# (decommissioned VORs may still be listed, see https://ourairports.com/help/data-dictionary.html).
# Static data: refreshed once a day (airport_ops.navaids_interval, default "24 hours"). Region = world keeps all
# ~11,000 rows (small: about 1.5 MB CSV).
import io

import pandas as pd
import requests
from enrichment.config import CONF

NAVAIDS_URL = "https://davidmegginson.github.io/ourairports-data/navaids.csv"
USER_AGENT = "databricks-cookbook-flight-tracking/1.0 (teaching demo)"
MHZ_TYPES = ("VOR", "VOR-DME", "VORTAC", "DME", "TACAN")          # frequency_khz is a VHF/UHF frequency

# lamin, lamax, lomin, lomax – identical to the presets of src/01_bronze_flights_raw.py
REGIONS = {
    "switzerland": (45.6, 48.0, 5.8, 10.7),
    "alps": (43.5, 49.0, 5.0, 16.5),
    "europe": (35.0, 72.0, -25.0, 45.0),
    "east": (36.0, 48.0, 26.0, 50.0),
    "world": (-90.0, 90.0, -180.0, 180.0),
}

SCHEMA = ("navaid_ident STRING, navaid_name STRING, navaid_type STRING, frequency_khz DOUBLE, frequency_label STRING, "
          "dme_channel STRING, latitude DOUBLE, longitude DOUBLE, elevation_ft DOUBLE, country STRING, "
          "associated_airport STRING, usage_type STRING, power STRING, magnetic_variation_deg DOUBLE, "
          "label STRING, ourairports_id BIGINT")
COLUMNS = [c.split()[0] for c in SCHEMA.split(", ")]


def region_bbox(region, bbox):
    """(lamin, lamax, lomin, lomax) of the pipeline region – custom regions use flights.bbox."""
    region = (region or "switzerland").lower()
    if region == "custom":
        return tuple(float(x) for x in bbox.split(","))
    return REGIONS[region]


def fetch_navaids_csv(timeout=60):
    """OurAirports navaids.csv as a raw pandas DataFrame, or None if the download fails (never raises).
    keep_default_na=False: the ident "NA" (and country code "NA" = Namibia) must not become NaN."""
    try:
        r = requests.get(NAVAIDS_URL, headers={"User-Agent": USER_AGENT}, timeout=timeout)
        r.raise_for_status()
        return pd.read_csv(io.StringIO(r.text), keep_default_na=False, na_values=[""])
    except Exception as e:
        print(f"Navaids download failed: {e}")
        return None


def frequency_label(navaid_type, khz):
    """110050 (VOR-DME) -> '110.05 MHz', 335 (NDB) -> '335 kHz'."""
    if khz is None or pd.isna(khz):
        return None
    if navaid_type in MHZ_TYPES:
        return f"{khz / 1000:.2f} MHz"
    return f"{khz:g} kHz"


def build_navaids(pdf_raw, bbox=None):
    """OurAirports navaids.csv (raw DataFrame) -> one row per navaid inside the bbox (None = whole world)."""
    if pdf_raw is None or pdf_raw.empty:
        return pd.DataFrame(columns=COLUMNS)
    n = pdf_raw.dropna(subset=["ident", "latitude_deg", "longitude_deg"])
    if bbox is not None:
        lamin, lamax, lomin, lomax = bbox
        n = n[n["latitude_deg"].between(lamin, lamax) & n["longitude_deg"].between(lomin, lomax)]
    out = pd.DataFrame({
        "navaid_ident": n["ident"].astype(str).str.strip(),
        "navaid_name": n["name"],
        "navaid_type": n["type"],
        "frequency_khz": pd.to_numeric(n["frequency_khz"], errors="coerce").astype(float),
        "dme_channel": n["dme_channel"],
        "latitude": n["latitude_deg"].astype(float),
        "longitude": n["longitude_deg"].astype(float),
        "elevation_ft": pd.to_numeric(n["elevation_ft"], errors="coerce").astype(float),
        "country": n["iso_country"],
        "associated_airport": n["associated_airport"],
        "usage_type": n["usageType"],                 # HI / LO = high / low airways, TERMINAL, BOTH, RNAV
        "power": n["power"],
        "magnetic_variation_deg": pd.to_numeric(n["magnetic_variation_deg"], errors="coerce").astype(float),
        "ourairports_id": n["id"].astype("int64"),
    })
    out["frequency_label"] = [frequency_label(t, f) for t, f in zip(out["navaid_type"], out["frequency_khz"])]
    out["label"] = (out["navaid_ident"] + " (" + out["navaid_name"].fillna("?").astype(str) + " "
                    + out["navaid_type"] + ")")                   # "ZUE (Zurich East VOR-DME)"
    return out[COLUMNS].sort_values(["country", "navaid_ident"]).reset_index(drop=True)


# ---------------------------------------------------------------------------------------------------------------
# Pipeline: a thin wrapper around fetch_navaids_csv() + build_navaids()
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
