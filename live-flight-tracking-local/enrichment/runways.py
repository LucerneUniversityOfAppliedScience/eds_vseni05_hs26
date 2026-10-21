# Ported from the Databricks recipe (src_airport_ops/02_silver_runways.py) – the pure pandas logic, without the Spark/pipeline part.
# Settings come from enrichment/config.py (CONF) instead of the pipeline configuration.
# Silver reference: runways of the airports in silver.airports – one row per runway END.
#
# A runway is named after its magnetic heading in tens of degrees: the concrete strip "14/32" in Zurich is runway 14
# when you land or take off towards ~140° and runway 32 in the opposite direction. To find out which runway is in
# use (03_gold_runway_in_use.py) we need each end separately: its threshold (where landing aircraft touch down,
# where departures start their take-off roll) and its TRUE heading (aircraft tracks and METAR winds are relative to
# true north, the runway name to magnetic north – in Switzerland about 3° apart).
#
# Source: OurAirports runways.csv (https://ourairports.com/data/), Public Domain. Columns le_* = low end
# (e.g. "14"), he_* = high end ("32"). Missing true headings are derived from the two threshold coordinates
# (initial great-circle bearing), as a last resort from the designator ("14" -> 140°, magnetic, flagged).
# Joined via OurAirports' airport_ident = silver.airports.airport_icao (identical for all but one airport in Europe).
# Static data: refreshed once a day (airport_ops.runways_interval, default "24 hours").
import numpy as np
import pandas as pd
from enrichment.config import CONF

RUNWAYS_URL = "https://davidmegginson.github.io/ourairports-data/runways.csv"

SCHEMA = ("airport_icao STRING, runway STRING, runway_end STRING, heading_true_deg DOUBLE, heading_source STRING, "
          "threshold_lat DOUBLE, threshold_lon DOUBLE, threshold_elevation_ft DOUBLE, displaced_threshold_ft DOUBLE, "
          "opposite_end STRING, opposite_lat DOUBLE, opposite_lon DOUBLE, length_ft DOUBLE, width_ft DOUBLE, "
          "surface STRING, surface_class STRING, lighted BOOLEAN, closed BOOLEAN")
COLUMNS = [c.split()[0] for c in SCHEMA.split(", ")]

PAVED = ("ASP", "CON", "PEM", "BIT", "TAR", "COP", "MAC", "PAV")       # prefixes of the free-text surface codes
UNPAVED = ("GRS", "GRA", "GRV", "GVL", "GRE", "DIRT", "TURF", "SAN", "CLA", "EAR", "SOIL", "COR", "LAT", "SNOW", "ICE")


def initial_bearing(lat1, lon1, lat2, lon2):
    """Initial great-circle bearing from point 1 to point 2 in degrees (0 = north, 90 = east). Works on arrays."""
    p1, p2, dl = np.radians(lat1), np.radians(lat2), np.radians(lon2 - lon1)
    x = np.sin(dl) * np.cos(p2)
    y = np.cos(p1) * np.sin(p2) - np.sin(p1) * np.cos(p2) * np.cos(dl)
    return (np.degrees(np.arctan2(x, y)) + 360) % 360


def surface_class(surface):
    s = str(surface or "").upper()
    if s.startswith(PAVED):
        return "paved"
    if s.startswith(UNPAVED) or "GRASS" in s or "GRAVEL" in s:
        return "unpaved"
    if s.startswith("WAT"):
        return "water"
    return "unknown"


def _designator_heading(ident):
    """'14' -> 140, '32L' -> 320, '7' -> 70; None for idents without a number (e.g. 'N', 'XX')."""
    digits = pd.Series(ident, dtype=object).astype(str).str.extract(r"^(\d{1,2})")[0]
    return pd.to_numeric(digits, errors="coerce") * 10


def build_runways(pdf_runways_raw, pdf_airports):
    """OurAirports runways.csv (as read with pandas) + silver.airports -> one row per runway end."""
    rw = pdf_runways_raw[pdf_runways_raw["airport_ident"].isin(pdf_airports["airport_icao"])].copy()
    for col in ("le_ident", "he_ident"):
        rw[col] = rw[col].fillna("").astype(str).str.strip()
    ends = []
    for this, other in (("le", "he"), ("he", "le")):
        e = pd.DataFrame({
            "airport_icao": rw["airport_ident"],
            "runway": rw["le_ident"] + "/" + rw["he_ident"],
            "runway_end": rw[f"{this}_ident"],
            "heading_true_deg": pd.to_numeric(rw[f"{this}_heading_degT"], errors="coerce"),
            "threshold_lat": rw[f"{this}_latitude_deg"], "threshold_lon": rw[f"{this}_longitude_deg"],
            "threshold_elevation_ft": rw[f"{this}_elevation_ft"],
            "displaced_threshold_ft": rw[f"{this}_displaced_threshold_ft"],
            "opposite_end": rw[f"{other}_ident"],
            "opposite_lat": rw[f"{other}_latitude_deg"], "opposite_lon": rw[f"{other}_longitude_deg"],
            "length_ft": rw["length_ft"], "width_ft": rw["width_ft"], "surface": rw["surface"],
            "lighted": rw["lighted"] == 1, "closed": rw["closed"] == 1,
        })
        ends.append(e)
    df = pd.concat(ends, ignore_index=True)
    df["runway_end"] = df["runway_end"].str.replace(r"^(\d)(?!\d)", r"0\1", regex=True)   # 7 -> 07
    # helicopter pads ("H1", "01H", "H-NORTH") and unnamed ends are not runways for airliners
    df = df[(df["runway_end"] != "") & (df["runway_end"] != "XX") & ~df["runway_end"].str.match(r"^(H|\d{2}H$)")]
    df["heading_source"] = np.where(df["heading_true_deg"].notna(), "ourairports", None)
    # 1) derive the true heading from the two thresholds: bearing from this end towards the opposite end
    derived = initial_bearing(df["threshold_lat"], df["threshold_lon"], df["opposite_lat"], df["opposite_lon"])
    use = df["heading_true_deg"].isna() & derived.notna()
    df.loc[use, "heading_true_deg"] = derived[use].round(1)
    df.loc[use, "heading_source"] = "coordinates"
    # 2) last resort: the designator (magnetic heading / 10) – a few degrees off, flagged as such
    designator = _designator_heading(df["runway_end"].tolist())
    designator.index = df.index
    use = df["heading_true_deg"].isna() & designator.notna()
    df.loc[use, "heading_true_deg"] = designator[use]
    df.loc[use, "heading_source"] = "designator"
    df = df[df["heading_true_deg"].notna()].copy()
    df["surface_class"] = df["surface"].map(surface_class)
    return df[COLUMNS].sort_values(["airport_icao", "runway", "runway_end"]).reset_index(drop=True)


# ---------------------------------------------------------------------------------------------------------------
# Pipeline: a thin wrapper around build_runways()
# ---------------------------------------------------------------------------------------------------------------
CATALOG = CONF.get("flights.catalog", "opensky")
SILVER = CONF.get("flights.silver_schema", "silver")
