# Ported from the Databricks recipe (src_aircraft/01_silver_aircraft_types.py) – the pure pandas logic, without the Spark/pipeline part.
# Settings come from enrichment/config.py (CONF) instead of the pipeline configuration.
# Silver: who is behind a transponder code? The OpenSky aircraft database maps every icao24 address to
# registration, manufacturer, model, ICAO type code (A320, B738, ...), operator and build year. The live positions
# only carry the icao24 – this table is the lookup for type, age and the fuel/CO2 estimate (03_gold_fuel_and_co2.py).
#
# One row per airframe (icao24), ~600,000 rows. The source is a ~108 MB CSV snapshot published roughly monthly, so the
# table refreshes only every aircraft.types_interval (default 24 hours) inside the continuous pipeline.
#
# Source: OpenSky Network aircraft database, https://opensky-network.org/datasets/metadata/
#   - crowdsourced and provided "as is" without guarantees (README in the bucket).
#   - OpenSky Terms of Use / Data License Agreement (https://opensky-network.org/about/terms-of-use): licensed solely
#     for non-profit research and non-profit education; commercial use needs a written license; do not redistribute
#     the data set; for publications (incl. web pages, public presentations) cite: M. Schäfer, M. Strohmeier,
#     V. Lenders, I. Martinovic, M. Wilhelm, "Bringing up OpenSky: A large-scale ADS-B sensor network for research",
#     IPSN 2014 – and https://opensky-network.org.
#   - only the "aircraft-database-complete-YYYY-MM.csv" files are complete (the aircraftDatabase-*.csv dumps are not).
# File format: comma separated, fields quoted with SINGLE quotes ('...'), a quote inside a field is doubled (''),
#   UTF-8 – but some rows were double-encoded upstream ("CÃ´te" instead of "Côte"), repaired below.
#
# Network: opensky-network.org/datasets/... redirects to s3.opensky-network.org, an S3-compatible server run by
# OpenSky itself (same IP as opensky-network.org, NOT AWS). Whether the Free Edition allow-list lets it through is
# unknown – on any error this table is created EMPTY (with schema) and a message is printed; the pipeline goes on.
# It is NOT reachable from Free Edition (blocked like the API). Way out: the producer machine copies the file into
# the private R2 bucket next to switzerland/ and world/ (producer/upload_aircraft_database.py, once a month) – this
# module reads the newest aircraft-database-complete-YYYY-MM.csv in aircraft.database_dir first (default
# /Volumes/opensky/raw/r2/reference). OpenSky terms: non-profit research/education, do not redistribute.
import io
import re
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from enrichment.config import CONF

CATALOG = CONF.get("flights.catalog", "opensky")
SILVER = CONF.get("flights.silver_schema", "silver")

BUCKET = "https://s3.opensky-network.org/data-samples"
FALLBACK_FILE = "metadata/aircraft-database-complete-2025-08.csv"     # newest file on 2026-10-18
USER_AGENT = "databricks-cookbook-flight-tracking/1.0 (teaching demo)"
DATABASE_DIR = CONF.get("aircraft.database_dir", f"/Volumes/{CATALOG}/raw/r2/reference")

# CSV column -> table column (only these are read: usecols keeps memory low)
COLUMNS = {
    "icao24": "icao24",
    "registration": "registration",
    "manufacturerName": "manufacturer",
    "model": "model",
    "typecode": "typecode",
    "icaoAircraftClass": "icao_aircraft_class",      # e.g. L2J = landplane, 2 engines, jet
    "operator": "operator",
    "operatorIcao": "operator_icao",
    "operatorCallsign": "operator_callsign",
    "owner": "owner",
    "built": "built",
    "firstFlightDate": "first_flight",
    "categoryDescription": "category",
    "country": "registration_country",
}
SCHEMA = ("icao24 STRING, registration STRING, manufacturer STRING, model STRING, typecode STRING, "
          "icao_aircraft_class STRING, operator STRING, operator_icao STRING, operator_callsign STRING, owner STRING, "
          "built_year INT, category STRING, registration_country STRING, source_file STRING")


def newest_database_file(listing_xml):
    """Pick the newest aircraft-database-complete-YYYY-MM.csv from an S3 bucket listing (XML)."""
    files = re.findall(r"<Key>(metadata/aircraft-database-complete-\d{4}-\d{2}\.csv)</Key>", listing_xml)
    return max(files) if files else None             # YYYY-MM sorts correctly as text


def fix_mojibake(s):
    """'CÃ´te' -> 'Côte': text that was UTF-8 encoded twice. Leaves everything else unchanged."""
    if isinstance(s, str) and ("Ã" in s or "â€" in s):
        try:
            return s.encode("cp1252").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            return s
    return s


def parse_aircraft_database(csv_bytes, source_file=None):
    """Raw CSV bytes -> cleaned pandas DataFrame with the table columns."""
    df = pd.read_csv(io.BytesIO(csv_bytes), usecols=list(COLUMNS), dtype=str, quotechar="'",
                     keep_default_na=False, na_values=[""], encoding="utf-8")
    df = df.rename(columns=COLUMNS)
    for c in df.columns:
        df[c] = df[c].str.strip().map(fix_mojibake).replace("", None)
    df["icao24"] = df["icao24"].str.lower()
    df = df[df["icao24"].str.fullmatch(r"[0-9a-f]{6}", na=False)]
    df["typecode"] = df["typecode"].str.upper()
    df["operator_icao"] = df["operator_icao"].str.upper()
    # built is a date ("2004-01-01"), first flight date as fallback; keep only plausible years.
    # Coverage is LOW for airliners (most rows of the 2025 file have neither) -> age is often unknown.
    year = pd.to_numeric(df["built"].fillna(df["first_flight"]).str[:4], errors="coerce")
    df["built_year"] = year.where(year.between(1903, datetime.now(timezone.utc).year)).astype("Int64")
    # rows without any useful information (only an icao24) are dropped
    useful = ["registration", "manufacturer", "model", "typecode", "operator", "owner"]
    df = df[df[useful].notna().any(axis=1)].drop_duplicates("icao24")
    df["source_file"] = source_file
    return df[["icao24", "registration", "manufacturer", "model", "typecode", "icao_aircraft_class", "operator",
               "operator_icao", "operator_callsign", "owner", "built_year", "category", "registration_country",
               "source_file"]]


def download_aircraft_database(timeout=300):
    """Find and download the newest complete database file. Returns (bytes, file name)."""
    headers = {"User-Agent": USER_AGENT}
    try:
        r = requests.get(BUCKET, params={"list-type": "2", "prefix": "metadata/"}, headers=headers, timeout=30)
        r.raise_for_status()
        key = newest_database_file(r.text) or FALLBACK_FILE
    except requests.RequestException as e:
        print(f"aircraft_types: bucket listing failed ({e}) – trying {FALLBACK_FILE}")
        key = FALLBACK_FILE
    t0 = time.time()
    r = requests.get(f"{BUCKET}/{key}", headers=headers, timeout=timeout)
    r.raise_for_status()
    print(f"aircraft_types: downloaded {key} ({len(r.content) / 1e6:.0f} MB) in {time.time() - t0:.0f} s")
    return r.content, key.split("/")[-1]
