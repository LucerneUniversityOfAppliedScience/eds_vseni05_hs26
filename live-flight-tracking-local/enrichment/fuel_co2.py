# Ported from the Databricks recipe (src_aircraft/03_gold_fuel_and_co2.py) – the pure pandas logic, without the Spark/pipeline part.
# Settings come from enrichment/config.py (CONF) instead of the pipeline configuration.
# Gold: how much fuel do the aircraft above us burn – and how much CO2 is that? An ESTIMATE for teaching, computed
# with OpenAP, an open aircraft performance model from TU Delft (https://openap.dev, PyPI "openap", LGPL-3.0).
#
#   gold.fuel_burn        one row per aircraft and snapshot of the last 60 minutes: fuel flow and CO2 in kg/h,
#                         plus the fuel / CO2 of the time slice the snapshot stands for (kg)
#   gold.co2_per_minute   CO2 per minute, airline and aircraft type – the time series for the dashboard
#
# How the estimate works (read this before you quote any number!)
#   * Fuel flow = OpenAP FuelFlow(ac=typecode).enroute(mass, tas, alt, vs): from drag (clean configuration, no flaps,
#     no gear) and the climb/descent angle, OpenAP derives the thrust and from the thrust the fuel flow of the
#     default engine. Units: mass kg, tas knots, alt ft, vs ft/min -> kg/s. CO2 = 3.16 kg per kg of jet fuel
#     (Emission.co2, IATA factor).
#   * Mass is UNKNOWN (no payload, no fuel on board in ADS-B). Assumption: half way between empty weight and maximum
#     take-off weight: mass = OEW + 0.5 * (MTOW - OEW), from openap.prop.aircraft(). For an A320 that is ~60 t –
#     the value OpenAP's own examples use. Real masses vary by roughly ±20 %, fuel flow follows mass roughly linearly.
#   * TAS ≈ ground speed: ADS-B gives the speed over ground; the speed through the air differs by the wind component
#     (up to ±150 km/h at cruise altitude). gold.flights_with_weather has an est_airspeed_kmh, but only for the latest
#     position – we keep ground speed for the whole hour so that every snapshot is treated the same way.
#   * Climb / cruise / descent are NOT separate models: the vertical rate goes into enroute(); climbing needs more
#     thrust, descending close to idle. Taxiing, take-off roll and approach with flaps/gear are not modelled:
#     aircraft on the ground or slower than 100 kt are skipped (fuel = NULL).
#   * Types: OpenAP knows ~37 types natively (A320 family, B737/B777/B787, E-Jets, ...) and maps ~22 more to similar
#     types ("synonyms", e.g. CRJ2 -> E145, B733 -> B734). Business jets, turboprops (except the ATR synonym),
#     helicopters and light aircraft are not covered -> fuel = NULL, openap_type = NULL.
#   * Integration over time: every snapshot stands for the time until the same aircraft's next snapshot (typically
#     the polling interval of 10-15 s). Gaps are capped at 2 × the typical snapshot interval – an aircraft that is
#     not seen for 3 minutes is not assumed to have flown (and burned) the whole time. fuel_kg = kg/h × seconds / 3600.
#
# Refreshes every aircraft.co2_interval (default 5 minutes). Needs the pip package openap (environment dependency
# "openap==2.4"); without it the tables are created empty and a message is printed.
import warnings

import numpy as np
import pandas as pd
from enrichment.config import CONF

CATALOG = CONF.get("flights.catalog", "opensky")
SILVER = CONF.get("flights.silver_schema", "silver")
GOLD = CONF.get("flights.gold_schema", "gold")
INTERVAL = CONF.get("aircraft.co2_interval", "5 minutes")

WORLD = CONF.get("flights.region", "switzerland").lower() == "world"   # ~10,000 aircraft: smaller windows
WINDOW_MINUTES = int(CONF.get("aircraft.co2_window_minutes" + ("_world" if WORLD else ""), "10" if WORLD else "60"))
MASS_FRACTION = 0.5           # mass = OEW + MASS_FRACTION * (MTOW - OEW)
MIN_SPEED_KT = 100            # slower = taxiing / landing roll / helicopter: not modelled
# OpenAP synonyms that change the size class too much (A310 -> A318, Learjet 45 -> Gulfstream G650): skip them
SKIP_SYNONYMS = {"A310", "LJ45"}
KMH_TO_KT = 1 / 1.852
MS_TO_FPM = 196.850

BURN_SCHEMA = ("snapshot_time TIMESTAMP, minute TIMESTAMP, icao24 STRING, callsign STRING, airline_icao STRING, "
               "typecode STRING, openap_type STRING, altitude_ft DOUBLE, speed_kt DOUBLE, vertical_rate_fpm DOUBLE, "
               "flight_phase STRING, assumed_mass_kg DOUBLE, fuel_kg_per_h DOUBLE, co2_kg_per_h DOUBLE, "
               "interval_s DOUBLE, fuel_kg DOUBLE, co2_kg DOUBLE")
MINUTE_SCHEMA = ("minute TIMESTAMP, airline STRING, typecode STRING, aircraft_count INT, fuel_kg DOUBLE, "
                 "co2_kg DOUBLE")


def openap_models(typecodes):
    """typecode -> (FuelFlow model, OpenAP type actually used, assumed mass kg); unsupported types are left out."""
    from openap import FuelFlow, prop
    native = {t.upper() for t in prop.available_aircraft()}
    known = {t.upper() for t in prop.available_aircraft(use_synonym=True)}
    models = {}
    for tc in sorted({t for t in typecodes if isinstance(t, str)}):
        if tc not in known or tc in SKIP_SYNONYMS:
            continue
        try:
            with warnings.catch_warnings():               # OpenAP warns "using synonym ..." – expected here
                warnings.simplefilter("ignore")
                ac = prop.aircraft(tc, use_synonym=True)
                ff = FuelFlow(tc, use_synonym=True)
            used = tc if tc in native else f"{tc}->{_synonym_of(tc)}"      # e.g. "CRJ2->E145"
            mass = ac["oew"] + MASS_FRACTION * (ac["mtow"] - ac["oew"])
            models[tc] = (ff, used, float(mass))
        except Exception as e:                             # broken model data for a type: just skip the type
            print(f"fuel_burn: OpenAP model for {tc} not usable ({e})")
    return models


def _synonym_of(tc):
    from openap import prop
    syn = prop.aircraft_synonym
    hit = syn.loc[syn["orig"] == tc.lower(), "new"]
    return hit.iloc[0].upper() if len(hit) else tc


def snapshot_intervals(pdf):
    """Seconds each row stands for: until the aircraft's next snapshot, capped at 2 × the typical interval."""
    times = pd.Series(pdf["snapshot_time"].unique()).sort_values()
    typical = times.diff().dt.total_seconds().median() if len(times) > 1 else 15.0
    typical = typical if typical and not np.isnan(typical) else 15.0
    s = pdf.sort_values(["icao24", "snapshot_time"])
    dt = (s.groupby("icao24")["snapshot_time"].shift(-1) - s["snapshot_time"]).dt.total_seconds()
    # last snapshot of an aircraft: assume one typical interval
    return dt.reindex(pdf.index).fillna(typical).clip(upper=2 * typical), typical


def estimate_fuel_burn(pdf_flights, pdf_types):
    """silver.flights rows (last hour) + aircraft_types (icao24, typecode) -> one row per aircraft and snapshot."""
    df = pdf_flights.merge(pdf_types[["icao24", "typecode"]], on="icao24", how="left")
    df["snapshot_time"] = pd.to_datetime(df["snapshot_time"], utc=True)
    df["minute"] = df["snapshot_time"].dt.floor("min")
    df["speed_kt"] = df["speed_kmh"] * KMH_TO_KT
    df["vertical_rate_fpm"] = (df["vertical_rate"].fillna(0) * MS_TO_FPM).round()
    df["interval_s"], _ = snapshot_intervals(df)
    df["openap_type"] = None
    df["assumed_mass_kg"] = np.nan
    df["fuel_kg_per_h"] = np.nan

    models = openap_models(df["typecode"].dropna().unique())
    flying = (~df["on_ground"].fillna(False).astype(bool)) & (df["speed_kt"] >= MIN_SPEED_KT) \
        & df["altitude_ft"].notna()
    for tc, (ff, used, mass) in models.items():
        rows = flying & (df["typecode"] == tc)
        if not rows.any():
            continue
        sub = df.loc[rows]
        with warnings.catch_warnings(), np.errstate(all="ignore"):
            warnings.simplefilter("ignore")
            kg_s = ff.enroute(mass=np.full(len(sub), mass), tas=sub["speed_kt"].to_numpy(float),
                              alt=sub["altitude_ft"].clip(lower=0).to_numpy(float),
                              vs=sub["vertical_rate_fpm"].to_numpy(float))
        df.loc[rows, "fuel_kg_per_h"] = np.asarray(kg_s, dtype=float) * 3600
        df.loc[rows, "openap_type"] = used
        df.loc[rows, "assumed_mass_kg"] = mass
    df["fuel_kg_per_h"] = df["fuel_kg_per_h"].astype(float).round(1)
    df["co2_kg_per_h"] = (df["fuel_kg_per_h"] * 3.16).round(1)          # = openap Emission.co2 (3160 g per kg)
    df["fuel_kg"] = (df["fuel_kg_per_h"] * df["interval_s"] / 3600).round(3)
    df["co2_kg"] = (df["co2_kg_per_h"] * df["interval_s"] / 3600).round(3)
    df["speed_kt"] = df["speed_kt"].round()
    return df[[c.split()[0] for c in BURN_SCHEMA.split(", ")]].sort_values(["snapshot_time", "icao24"])


def co2_per_minute(pdf_burn, pdf_airlines):
    """Sum the fuel / CO2 slices per minute, airline and type. Only aircraft with an estimate are counted."""
    df = pdf_burn[pdf_burn["co2_kg"].notna()].merge(pdf_airlines, on="airline_icao", how="left")
    df["airline"] = df["airline_name"].fillna(df["airline_icao"]).fillna("unknown / private")
    out = (df.groupby(["minute", "airline", "typecode"], as_index=False)
             .agg(aircraft_count=("icao24", "nunique"), fuel_kg=("fuel_kg", "sum"), co2_kg=("co2_kg", "sum")))
    out["fuel_kg"] = out["fuel_kg"].round(2)
    out["co2_kg"] = out["co2_kg"].round(2)
    return out.sort_values(["minute", "co2_kg"], ascending=[True, False])


def _empty(schema):
    return pd.DataFrame(columns=[c.split()[0] for c in schema.split(", ")])
