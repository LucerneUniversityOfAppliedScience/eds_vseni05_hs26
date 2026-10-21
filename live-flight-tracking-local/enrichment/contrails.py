# Ported from the Databricks recipe (src_weather/03_gold_contrails.py) – the pure pandas logic, without the Spark/pipeline part.
# Settings come from enrichment/config.py (CONF) instead of the pipeline configuration.
# Gold: which aircraft are producing contrails right now – and which of them persist?
#
# Why this matters: CO2 calculators only count the fuel burnt. But the white lines behind aircraft can spread into
# "contrail cirrus". For 2018, Lee et al. (2021) estimate their effective radiative forcing at 57.4 mW/m², that of
# aviation's CO2 at 34.3 mW/m². Over Japan, 2.2 % of the flights caused 80 % of the contrail energy forcing
# (Teoh et al. 2020): the ones flying through cold AND ice-supersaturated air.
#
# The physics in three steps, per airborne aircraft above contrails.min_altitude_ft (default 20,000 ft):
# 1. Ambient air at the aircraft: its barometric altitude gives the pressure (ICAO standard atmosphere). Temperature
#    and relative humidity are interpolated (linear in ln p) between the two pressure levels of
#    silver.weather_at_flights that bracket the aircraft, in its 1° grid cell.
# 2. Does a contrail FORM? Schmidt-Appleman criterion (Schumann 1996): the hot, moist exhaust mixes with the cold
#    ambient air; in a vapour-pressure/temperature diagram the mixture moves along a straight line with slope
#        G = EI_H2O * cp * p / (eps * Q * (1 - eta))      [Pa/K]
#    Constants as in pycontrails 0.64 (core/fuel.py JetA, physics/constants.py, models/sac.py): EI_H2O 1.23 kg water
#    per kg Jet A-1, Q 43.13 MJ/kg, cp 1004 J/kg/K (dry air, c_pd), eps = R_d / R_v = 0.622, eta 0.3 (pycontrails' default engine
#    efficiency). If the line crosses the saturation curve over LIQUID water, droplets form and freeze -> contrail.
#      T_LM: the line is tangent to the saturation curve (d e_w/dT = G) – the threshold for water-saturated air,
#            i.e. the warmest possible threshold (column sac_threshold_max_c);
#      T_LC: threshold for the actual humidity U (0..1, over water): e_w(T_LM) - G (T_LM - T_LC) = U e_w(T_LC)
#            (column sac_threshold_c).
#    contrail_forms = ambient temperature < T_LC. With these constants at 250 hPa: T_LM = -41.9 °C,
#    T_LC = -51.3 °C for dry air (computed by sac_threshold_temps below).
# 3. Does it PERSIST? Only if the ambient air is supersaturated with respect to ICE (relative humidity over ice
#    RHi >= 100 %) – the "persistent contrail region" of pycontrails (models/pcr.py). The weather comes from DWD's
#    ICON (Open-Meteo, models=icon_eu / icon_global), whose relative humidity is defined with respect to liquid water
#    (DWD ICON Database Reference v2.5.6), so it is converted: RHi = RHw * e_w(T) / e_i(T) (saturation pressures after
#    Murphy & Koop 2005). At -50 °C ice saturates at 62 % RHw.
#
# Limitations (tell your students):
# - Six pressure levels (850 … 200 hPa) and a 1° grid (~110 km): layers thinner than the level spacing are not
#   resolved. Above 200 hPa (38,662 ft in the standard atmosphere) the 200 hPa values are used (flag
#   weather_extrapolated).
# - Other models define humidity differently (ECMWF IFS: over ice below -23 °C); contrails.rh_reference = water | ice
#   must match the model in weather.py.
# - eta depends on the engine; EI_H2O and Q are for Jet A-1 (not SAF/hydrogen).
# - Only grid cells in silver.weather_at_flights (top weather.max_cells) are evaluated – the rest gets a reason.
#
# References: Schumann, U. (1996): On conditions for contrail formation from aircraft exhausts. Meteorol. Z. 5, 4–23.
# Murphy, D. M. & Koop, T. (2005): Review of the vapour pressures of ice and supercooled water. QJRMS 131, 1539–1565.
# Lee, D. S. et al. (2021): The contribution of global aviation to anthropogenic climate forcing for 2000 to 2018.
# Atmos. Environ. 244, 117834. Teoh, R. et al. (2020): Mitigating the climate forcing of aircraft contrails by small-
# scale diversions and technology adoption. Environ. Sci. Technol. 54, 2941–2950. ICAO Doc 7488 (standard atmosphere).
# DWD (2026): Database Reference for the Global and Regional ICON and ICON-EPS Forecasting System, v2.5.6.
# Weather data by Open-Meteo.com (CC BY 4.0).
#
# All logic is in plain pandas/numpy functions (compute_contrails, summarize_contrails), the @dp functions only read
# and write tables – so the same code runs in an app or a notebook.
import numpy as np
import pandas as pd
from enrichment.config import CONF

CATALOG = CONF.get("flights.catalog", "opensky")
SILVER = CONF.get("flights.silver_schema", "silver")
GOLD = CONF.get("flights.gold_schema", "gold")
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
GRID = float(CONF.get("weather.grid_degrees", "1.0"))
WORLD = CONF.get("flights.region", "switzerland").lower() == "world"   # ~13,000 aircraft: refresh less often
INTERVAL = CONF.get("contrails.interval" + ("_world" if WORLD else ""), "5 minutes" if WORLD else "1 minute")
ETA = float(CONF.get("contrails.propulsion_efficiency", "0.3"))
MIN_ALT_FT = float(CONF.get("contrails.min_altitude_ft", "20000"))
RH_REFERENCE = CONF.get("contrails.rh_reference", "water").lower()

EI_H2O = 1.23      # kg water vapour per kg Jet A-1 (pycontrails JetA)
Q_FUEL = 43.13e6   # J/kg, specific combustion heat of Jet A-1 (pycontrails JetA)
CP = 1004.0        # J/kg/K, isobaric heat capacity of air
EPS = 0.622        # molar mass water / dry air
BANDS = [(20000, 25000, "20-25k ft"), (25000, 30000, "25-30k ft"), (30000, 35000, "30-35k ft"),
         (35000, 40000, "35-40k ft"), (40000, 1e9, "40k+ ft")]


# ---------------------------------------------------------------------------------------------------- physics
def e_sat_water_pa(t_k):
    """Saturation vapour pressure over (supercooled) liquid water in Pa, Murphy & Koop (2005) eq. 10, 123-332 K."""
    t = np.asarray(t_k, dtype=float)
    return np.exp(54.842763 - 6763.22 / t - 4.210 * np.log(t) + 0.000367 * t
                  + np.tanh(0.0415 * (t - 218.8)) * (53.878 - 1331.22 / t - 9.44523 * np.log(t) + 0.014025 * t))


def e_sat_ice_pa(t_k):
    """Saturation vapour pressure over ice in Pa, Murphy & Koop (2005) eq. 7, valid above 110 K."""
    t = np.asarray(t_k, dtype=float)
    return np.exp(9.550426 - 5723.265 / t + 3.53068 * np.log(t) - 0.00728332 * t)


def isa_pressure_hpa(altitude_m):
    """Pressure of the ICAO standard atmosphere at a (barometric/pressure) altitude in m – troposphere and the
    isothermal layer above 11 km. OpenSky's baro_altitude is exactly such a pressure altitude."""
    h = np.asarray(altitude_m, dtype=float)
    tropo = 1013.25 * (1 - 2.25577e-5 * np.minimum(h, 11000)) ** 5.25588
    return np.where(h <= 11000, tropo, 226.32 * np.exp(-(h - 11000) / 6341.62))


def rh_water_to_ice(rh_water_pct, temp_c):
    """Relative humidity over water -> over ice at the same vapour pressure (RHi = RHw * e_w / e_i)."""
    t_k = np.asarray(temp_c, dtype=float) + 273.15
    return np.asarray(rh_water_pct, dtype=float) * e_sat_water_pa(t_k) / e_sat_ice_pa(t_k)


def sac_slope_pa_per_k(pressure_hpa, eta=0.3):
    """Slope G of the exhaust mixing line (Schumann 1996)."""
    return EI_H2O * CP * np.asarray(pressure_hpa, dtype=float) * 100 / (EPS * Q_FUEL * (1 - eta))


def _bisect(f, lo, hi, n=60):
    """Vectorised bisection for a root of an increasing function f between lo and hi (arrays)."""
    lo, hi = np.array(lo, dtype=float), np.array(hi, dtype=float)
    for _ in range(n):
        mid = (lo + hi) / 2
        pos = f(mid) > 0
        hi, lo = np.where(pos, mid, hi), np.where(pos, lo, mid)
    return (lo + hi) / 2


def sac_threshold_temps(pressure_hpa, rh_water_pct, eta=0.3):
    """Schmidt-Appleman threshold temperatures in °C: (t_lm for water-saturated air = the warmest possible threshold,
    t_lc for the given relative humidity over water, <= t_lm).
    A contrail forms when the ambient temperature is below t_lc."""
    g = np.atleast_1d(sac_slope_pa_per_k(pressure_hpa, eta))
    u = np.clip(np.nan_to_num(np.atleast_1d(np.asarray(rh_water_pct, dtype=float)), nan=0.0) / 100, 0, 1)
    u = np.broadcast_to(u, g.shape)
    slope = lambda t: (e_sat_water_pa(t + 0.01) - e_sat_water_pa(t - 0.01)) / 0.02     # d e_w / dT
    t_lm = _bisect(lambda t: slope(t) - g, np.full(g.shape, 180.0), np.full(g.shape, 300.0))
    e_lm = e_sat_water_pa(t_lm)
    # f(T) = e_w(T_LM) - G (T_LM - T) - U e_w(T) is increasing in T below T_LM and >= 0 at T_LM
    t_lc = _bisect(lambda t: e_lm - g * (t_lm - t) - u * e_sat_water_pa(t), t_lm - 60, t_lm)
    t_lc = np.where(u >= 1, t_lm, t_lc)
    return t_lm - 273.15, t_lc - 273.15


# ---------------------------------------------------------------------------------------------- the model
def snap(values, grid=1.0):
    """Grid cell coordinate exactly like silver.weather_at_flights (round to the grid, then to 2 decimals)."""
    return np.round(np.round(np.asarray(values, dtype=float) / grid) * grid, 2)


def ambient_at_aircraft(cell_lat, cell_lon, pressure_hpa, weather):
    """Temperature (°C) and relative humidity (%) at each aircraft, interpolated linearly in ln(p) between the
    bracketing pressure levels of its grid cell. Returns (temp_c, rh_pct, extrapolated, observed_at) arrays."""
    w = weather[weather["level_hpa"] > 0]
    n = len(pressure_hpa)
    if w.empty:
        return np.full(n, np.nan), np.full(n, np.nan), np.zeros(n, bool), np.full(n, pd.NaT)
    levels = np.sort(w["level_hpa"].unique()).astype(float)                       # ascending: 200 … 850
    w = w.assign(cell_lat=snap(w["cell_lat"], 0.01), cell_lon=snap(w["cell_lon"], 0.01))
    temp = w.pivot_table(index=["cell_lat", "cell_lon"], columns="level_hpa", values="level_temp_c")
    rh = w.pivot_table(index=["cell_lat", "cell_lon"], columns="level_hpa", values="level_rh_pct")
    obs = w.groupby(["cell_lat", "cell_lon"])["observed_at"].max()
    key = pd.MultiIndex.from_arrays([np.asarray(cell_lat, float), np.asarray(cell_lon, float)])
    T = temp.reindex(index=key, columns=levels).to_numpy(float)                   # aircraft x levels
    R = rh.reindex(index=key, columns=levels).to_numpy(float) if not rh.empty else np.full(T.shape, np.nan)
    if len(levels) == 1:
        return T[:, 0], R[:, 0], np.asarray(pressure_hpa) < levels[0], obs.reindex(key).to_numpy()
    xl = np.log(levels)
    p = np.asarray(pressure_hpa, dtype=float)
    x = np.clip(np.log(p), xl[0], xl[-1])                                         # above 200 hPa: 200 hPa values
    i = np.clip(np.searchsorted(xl, x, side="right") - 1, 0, len(xl) - 2)
    wt = (x - xl[i]) / (xl[i + 1] - xl[i])
    rows = np.arange(n)
    t_c = T[rows, i] * (1 - wt) + T[rows, i + 1] * wt
    rh_pct = R[rows, i] * (1 - wt) + R[rows, i + 1] * wt
    return t_c, rh_pct, p < levels[0], obs.reindex(key).to_numpy()


def compute_contrails(positions, weather, grid_degrees=1.0, eta=0.3, min_altitude_ft=20000, rh_reference="water"):
    """One row per aircraft of `positions` (shaped like gold.current_positions) with ambient conditions,
    Schmidt-Appleman thresholds and the contrail verdict. `weather` is shaped like silver.weather_at_flights
    (cell_lat, cell_lon, level_hpa, level_temp_c, level_rh_pct, observed_at)."""
    out = positions[[c for c in ["snapshot_time", "icao24", "callsign", "airline_name", "latitude", "longitude",
                                 "altitude_ft", "altitude_m", "flight_phase", "on_ground"] if c in positions]].copy()
    out = out.reset_index(drop=True)
    alt_ft = pd.to_numeric(out["altitude_ft"], errors="coerce").to_numpy(float)
    alt_m = pd.to_numeric(out.get("altitude_m", pd.Series(alt_ft / 3.28084)), errors="coerce").to_numpy(float)
    on_ground = out.get("on_ground", pd.Series(False, index=out.index)).fillna(False).astype(bool).to_numpy()
    relevant = ~on_ground & (alt_ft >= min_altitude_ft)

    p = np.where(relevant, isa_pressure_hpa(np.nan_to_num(alt_m)), np.nan)
    t_c, rh_in, extrap, observed = ambient_at_aircraft(snap(out["latitude"], grid_degrees),
                                                       snap(out["longitude"], grid_degrees), np.nan_to_num(p, nan=1000),
                                                       weather if "level_rh_pct" in weather else
                                                       weather.assign(level_rh_pct=np.nan))
    if rh_reference == "ice":           # model humidity already over ice below 0 °C -> derive the value over water
        rh_ice = rh_in
        rh_w = rh_in * e_sat_ice_pa(t_c + 273.15) / e_sat_water_pa(t_c + 273.15)
    else:
        rh_w = rh_in
        rh_ice = rh_water_to_ice(rh_in, t_c)
    has_wx = relevant & ~np.isnan(t_c) & ~np.isnan(rh_w)
    t_lm, t_lc = sac_threshold_temps(np.where(has_wx, p, 250.0), np.where(has_wx, rh_w, 0.0), eta)

    nan = lambda a: np.where(has_wx, a, np.nan)
    out["pressure_hpa"] = np.round(np.where(relevant, p, np.nan), 1)
    out["ambient_temp_c"] = np.round(nan(t_c), 1)
    out["rh_water_pct"] = np.round(nan(rh_w), 1)
    out["rh_ice_pct"] = np.round(nan(rh_ice), 1)
    out["sac_threshold_max_c"] = np.round(nan(t_lm), 1)
    out["sac_threshold_c"] = np.round(nan(t_lc), 1)
    out["sac_margin_k"] = np.round(nan(t_lc - t_c), 1)           # > 0: colder than the threshold -> contrail forms
    out["rhi_margin_pct"] = np.round(nan(rh_ice - 100), 1)       # >= 0: ice-supersaturated -> contrail persists
    forms = has_wx & (t_c < t_lc)
    persistent = forms & (rh_ice >= 100)
    out["contrail_forms"] = pd.array(np.where(has_wx | ~relevant, forms, None), dtype="boolean")
    out["contrail_persistent"] = pd.array(np.where(has_wx | ~relevant, persistent, None), dtype="boolean")
    out["contrail_class"] = np.select([~relevant, ~has_wx, persistent, forms],
                                      ["not evaluated", "unknown", "persistent", "short-lived"], "none")
    reason = np.select(
        [on_ground | np.isnan(alt_ft), ~relevant, ~has_wx, persistent, forms],
        ["on ground / no altitude", f"below {min_altitude_ft:,.0f} ft",
         "no weather for this grid cell (weather.max_cells)",
         "forms in ice-supersaturated air: persistent contrail region, can spread into contrail cirrus",
         "forms, but the air is not ice-supersaturated: short-lived"],
        "too warm for a contrail")
    out["reason"] = np.where(has_wx & extrap, np.char.add(reason.astype(str), " (above 200 hPa: 200 hPa weather used)"),
                             reason)
    out["weather_extrapolated"] = has_wx & extrap
    out["weather_observed_at"] = np.where(has_wx, observed, pd.NaT)
    out["weather_observed_at"] = pd.to_datetime(out["weather_observed_at"], utc=True)
    return out.drop(columns=[c for c in ["altitude_m", "on_ground"] if c in out])


def summarize_contrails(contrails, min_altitude_ft=20000):
    """Share of airborne aircraft forming / persistent contrails per altitude band, plus a total row."""
    air = contrails[contrails["reason"] != "on ground / no altitude"]
    alt = air["altitude_ft"].astype(float)
    bands = [(-1e9, min_altitude_ft, f"below {min_altitude_ft / 1000:.0f}k ft")] + BANDS
    rows = []

    def row(order, label, part):
        ev = part[part["contrail_forms"].notna() & (part["contrail_class"] != "not evaluated")]
        n_ev, n_f, n_p = len(ev), int(ev["contrail_forms"].sum()), int(ev["contrail_persistent"].sum())
        rows.append({"band_order": order, "altitude_band": label, "airborne_aircraft": len(part),
                     "evaluated_aircraft": n_ev, "forming": n_f, "persistent": n_p,
                     "forming_pct": round(100 * n_f / n_ev, 1) if n_ev else None,
                     "persistent_pct": round(100 * n_p / n_ev, 1) if n_ev else None,
                     "persistent_pct_of_airborne": round(100 * n_p / len(part), 1) if len(part) else None,
                     "avg_ambient_temp_c": round(ev["ambient_temp_c"].mean(), 1) if n_ev else None,
                     "avg_sac_threshold_c": round(ev["sac_threshold_c"].mean(), 1) if n_ev else None,
                     "avg_rh_ice_pct": round(ev["rh_ice_pct"].mean(), 1) if n_ev else None})

    for k, (lo, hi, label) in enumerate(bands):
        row(k, label, air[(alt >= lo) & (alt < hi)])
    row(len(bands), "all airborne", air)
    out = pd.DataFrame(rows)
    out.insert(0, "snapshot_time", contrails["snapshot_time"].max() if len(contrails) else pd.NaT)
    return out


# ----------------------------------------------------------------------------------------------- pipeline
CONTRAILS_SCHEMA = (
    "snapshot_time TIMESTAMP, icao24 STRING, callsign STRING, airline_name STRING, latitude DOUBLE, longitude DOUBLE, "
    "altitude_ft DOUBLE, flight_phase STRING, pressure_hpa DOUBLE, ambient_temp_c DOUBLE, rh_water_pct DOUBLE, "
    "rh_ice_pct DOUBLE, sac_threshold_max_c DOUBLE, sac_threshold_c DOUBLE, sac_margin_k DOUBLE, "
    "rhi_margin_pct DOUBLE, contrail_forms BOOLEAN, contrail_persistent BOOLEAN, contrail_class STRING, "
    "reason STRING, weather_extrapolated BOOLEAN, weather_observed_at TIMESTAMP")
SUMMARY_SCHEMA = (
    "snapshot_time TIMESTAMP, band_order INT, altitude_band STRING, airborne_aircraft INT, evaluated_aircraft INT, "
    "forming INT, persistent INT, forming_pct DOUBLE, persistent_pct DOUBLE, persistent_pct_of_airborne DOUBLE, "
    "avg_ambient_temp_c DOUBLE, avg_sac_threshold_c DOUBLE, avg_rh_ice_pct DOUBLE")
