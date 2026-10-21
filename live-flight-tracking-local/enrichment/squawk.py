# Ported from the Databricks recipe (src_geo/05_gold_squawk_alerts.py) – the pure pandas logic, without the Spark/pipeline part.
# Settings come from enrichment/config.py (CONF) instead of the pipeline configuration.
# Gold: emergency squawks. The squawk is the 4-digit transponder code a pilot sets; three codes are reserved
# worldwide for emergencies and make the aircraft light up on every radar screen:
#   7700 general emergency   7600 radio failure (lost communications)   7500 unlawful interference (hijack)
# Plus a few codes that are only informational – they explain why an aircraft shows up with a "generic" code:
#   7000 VFR conspicuity code (most of Europe)   2000 entering an SSR area without an assigned code
#   1200 VFR conspicuity code (USA/Canada)       7400 lost link of an unmanned aircraft (ICAO, since 2020)
# Code 1000 (Mode S conspicuity, used by hundreds of airliners over Europe at any time) is deliberately not listed.
#
# Real emergencies are rare – most days this table holds only info rows. A single snapshot with 7700 can also be a
# typo while the pilot dials another code (e.g. 7100 -> 7700 passes 7700 for a second), so `snapshots` and the
# duration help to separate real cases from glitches.
#
# One row per aircraft and code over the last geo.squawk_lookback_hours (default 6): first/last seen, last position.
# Setting: geo.squawk_interval (default "1 minute") – cheap: the filter on the squawk runs in Spark first.
import pandas as pd
from enrichment.config import CONF

CATALOG = CONF.get("flights.catalog", "opensky")
SILVER = CONF.get("flights.silver_schema", "silver")
GOLD = CONF.get("flights.gold_schema", "gold")
LOOKBACK_HOURS = int(CONF.get("geo.squawk_lookback_hours", "6"))

# code -> (severity, meaning)
SQUAWKS = {
    "7500": ("critical", "unlawful interference (hijack)"),
    "7700": ("critical", "general emergency"),
    "7600": ("warning", "radio failure (lost communications)"),
    "7400": ("warning", "unmanned aircraft: lost control link"),
    "7000": ("info", "VFR conspicuity code (Europe)"),
    "2000": ("info", "no code assigned (entering SSR area)"),
    "1200": ("info", "VFR conspicuity code (USA/Canada)"),
}
SEVERITY_ORDER = {"critical": 1, "warning": 2, "info": 3}

SCHEMA = ("icao24 STRING, callsign STRING, squawk STRING, severity STRING, meaning STRING, first_seen TIMESTAMP, "
          "last_seen TIMESTAMP, duration_minutes DOUBLE, snapshots BIGINT, latitude DOUBLE, longitude DOUBLE, "
          "altitude_ft DOUBLE, on_ground BOOLEAN, origin_country STRING")


def squawk_alerts(pdf_flights, since=None):
    """silver.flights rows (any subset) -> one alert row per aircraft and notable squawk, most severe first."""
    cols = [c.split(" ")[0] for c in SCHEMA.split(", ")]
    d = pdf_flights[pdf_flights["squawk"].isin(list(SQUAWKS))]
    if since is not None:
        d = d[pd.to_datetime(d["snapshot_time"]) >= since]
    if d.empty:
        return pd.DataFrame(columns=cols)
    d = d.sort_values("snapshot_time")
    g = d.groupby(["icao24", "squawk"])
    out = g.last()[["callsign", "latitude", "longitude", "altitude_ft", "on_ground", "origin_country"]]
    out["callsign"] = g["callsign"].agg(lambda s: s.dropna().iloc[-1] if s.notna().any() else None)
    out["first_seen"] = g["snapshot_time"].min()
    out["last_seen"] = g["snapshot_time"].max()
    out["snapshots"] = g.size()
    out = out.reset_index()
    out["duration_minutes"] = (pd.to_datetime(out["last_seen"]) - pd.to_datetime(out["first_seen"])).dt.total_seconds() / 60
    out["severity"] = out["squawk"].map(lambda c: SQUAWKS[c][0])
    out["meaning"] = out["squawk"].map(lambda c: SQUAWKS[c][1])
    out["_order"] = out["severity"].map(SEVERITY_ORDER)
    return out.sort_values(["_order", "last_seen"], ascending=[True, False])[cols].reset_index(drop=True)
