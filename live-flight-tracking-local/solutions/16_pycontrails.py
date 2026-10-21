import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium", app_title="16 – Contrails with pycontrails")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 16 – Contrails with pycontrails: SAC, ISSR and persistent contrail regions

    In notebook 09 you checked the **Schmidt-Appleman criterion** (SAC) by hand, with the contrail module of the
    app. Here you use [pycontrails](https://py.contrails.org) (open source, Apache 2.0) on a **3D grid over
    Switzerland** for every hour of your data, and then put your observed cruise positions into that grid.

    | model | question | condition |
    |---|---|---|
    | `SAC` | does a contrail **form**? | air colder than the Schmidt-Appleman threshold |
    | `ISSR` | is the air **ice-supersaturated**? | relative humidity over ice > 100 % and colder than 0 °C |
    | `PCR` | **persistent contrail region** | SAC **and** ISSR – a contrail forms *and* does not sublimate right away |

    (Conditions as implemented in `pycontrails.models.issr` and `pycontrails.models.pcr`, version 0.64.)

    Result: the share of **flight kilometres** that ran through persistent contrail regions – per airline and type.

    pycontrails is an optional dependency: `uv sync --group contrails` (once).
    """)
    return


@app.cell
def _(mo):
    import sys

    import numpy as np
    import pandas as pd
    import requests

    sys.path.insert(0, str(mo.notebook_dir().parent))
    import flightlab as fl
    from enrichment import contrails as cm                # the hand-made physics of notebook 09

    def check(ok, good, bad):
        return mo.callout(mo.md(("✅ " + good) if ok else ("❌ " + bad)), kind="success" if ok else "danger")

    con = fl.snapshot()
    fl.ensure(con, "aircraft_types")                     # tables the app computes – here if you only ran collector.py
    span = con.execute("SELECT MIN(snapshot_time), MAX(snapshot_time) FROM silver.flights "
                       "WHERE region = 'switzerland'").fetchone()
    mo.md(f"your Swiss data: **{span[0]:%Y-%m-%d %H:%M} – {span[1]:%Y-%m-%d %H:%M} UTC**")
    return check, cm, con, np, pd, requests, span


@app.cell
def _(mo):
    try:
        import pycontrails
        from pycontrails import GeoVectorDataset, MetDataset
        from pycontrails.models.pcr import PCR
        from pycontrails.physics import thermo

        pc_error = None
        _msg = mo.callout(mo.md(f"✅ pycontrails {pycontrails.__version__} is installed"), kind="success")
    except ImportError as _e:
        GeoVectorDataset = MetDataset = PCR = thermo = None
        pc_error = str(_e)
        _msg = mo.callout(mo.md(f"❌ pycontrails is missing (`{_e}`). Install the optional group once in the "
                                f"terminal: `uv sync --group contrails` – then restart this notebook."), kind="danger")
    _msg
    return GeoVectorDataset, MetDataset, PCR, pc_error, thermo


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## The weather: a grid over Switzerland from Open-Meteo

    The cell below loads hourly temperature, relative humidity and wind on **5 pressure levels** (400 … 150 hPa =
    23,574 … 44,647 ft in the ICAO standard atmosphere) for a **1° grid** (lat 45–48, lon 5–11, 28 points) and your
    data window (at most the last 7 days) – one request, **model DWD ICON-EU** (`models=icon_eu`). The answer is a
    long table `om` (one row per point, level and hour). Windows older than 90 days go to the
    [historical forecast API](https://open-meteo.com/en/docs/historical-forecast-api) (ICON from 2022-11-24).

    **Why one fixed model?** Weather models define relative humidity differently – DWD's ICON
    [with respect to water](https://www.dwd.de/DWD/forschung/nwv/fepub/icon_database_main.pdf) (Database Reference
    v2.5.6, pressure-level fields), ECMWF's IFS
    [with respect to ice below −23 °C](https://confluence.ecmwf.int/spaces/UER/pages/51711725/Relative+humidity+pressure+level).
    Open-Meteo's default ("best match") picks a model per location, and its
    [documentation](https://open-meteo.com/en/docs) does not say which definition you get. For ice supersaturation
    that is the whole question – so we fix the model and use its documented definition.

    Weather data by [Open-Meteo.com](https://open-meteo.com) (CC BY 4.0), model ICON-EU by DWD.
    """)
    return


@app.cell
def _(mo, pd, requests, span):
    LATS, LONS, LEVELS = [45, 46, 47, 48], [5, 6, 7, 8, 9, 10, 11], [400, 300, 250, 200, 150]

    def load_open_meteo(start, end):
        """Hourly weather on LEVELS for the LATS x LONS grid -> long DataFrame (UTC)."""
        points = [(la, lo) for la in LATS for lo in LONS]
        old = start < pd.Timestamp.now() - pd.Timedelta(days=90)
        url = ("https://historical-forecast-api.open-meteo.com/v1/forecast" if old
               else "https://api.open-meteo.com/v1/forecast")
        r = requests.get(url, timeout=60, params={
            "latitude": ",".join(str(p[0]) for p in points), "longitude": ",".join(str(p[1]) for p in points),
            "hourly": ",".join(f"{v}_{lvl}hPa" for lvl in LEVELS
                               for v in ("temperature", "relative_humidity", "wind_speed", "wind_direction")),
            "wind_speed_unit": "ms", "start_date": f"{start:%Y-%m-%d}", "end_date": f"{end:%Y-%m-%d}",
            "timezone": "GMT", "models": "icon_eu"})          # RH with respect to water (DWD) – see the text above
        r.raise_for_status()
        parts = []
        for (la, lo), loc in zip(points, r.json()):       # use OUR grid coordinates, not the snapped ones returned
            h = loc["hourly"]
            for lvl in LEVELS:
                parts.append(pd.DataFrame({
                    "time": pd.to_datetime(h["time"]), "latitude": float(la), "longitude": float(lo),
                    "level": float(lvl), "t_c": h[f"temperature_{lvl}hPa"], "rh_pct": h[f"relative_humidity_{lvl}hPa"],
                    "ws": h[f"wind_speed_{lvl}hPa"], "wd": h[f"wind_direction_{lvl}hPa"]}))
        return pd.concat(parts, ignore_index=True).dropna()

    _start = max(pd.Timestamp(span[0]), pd.Timestamp(span[1]) - pd.Timedelta(days=7))
    try:
        with mo.status.spinner(title="Open-Meteo: 28 points × 5 levels ..."):
            om = load_open_meteo(_start, pd.Timestamp(span[1]))
        _view = mo.md(f"`om`: **{len(om):,} rows** – {om.time.nunique()} hours × 28 points × {len(LEVELS)} levels")
    except Exception as _e:
        om = pd.DataFrame(columns=["time", "latitude", "longitude", "level", "t_c", "rh_pct", "ws", "wd"])
        _view = mo.callout(mo.md(f"❌ Open-Meteo is not reachable (`{_e}`) – rerun this cell later."), kind="danger")
    _view
    return LATS, LEVELS, LONS, om


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 16.1 – Build a pycontrails `MetDataset`

    pycontrails wants the weather as an xarray dataset with the dimensions **`longitude`, `latitude`, `level`
    (hPa), `time`** and variables in SI units with CF names. Convert `om`:

    - `air_temperature` in **K** = `t_c + 273.15`
    - `specific_humidity` (kg water per kg air): ICON's relative humidity is **over liquid water**, so
      `q = rh_pct / 100 × thermo.q_sat_liquid(T, p)` with `T` in K and `p` in **Pa** (`level × 100`)
    - `eastward_wind` = `−ws × sin(wd)`, `northward_wind` = `−ws × cos(wd)` (`wd` in radians; it is the direction
      the wind blows **from**)

    Then `met = MetDataset(df.set_index(["longitude", "latitude", "level", "time"])[columns].to_xarray())`.

    /// details | Hint
    Work on a copy: `_df = om.copy()`, and pass numpy arrays to `thermo`:
    `thermo.q_sat_liquid(_df["air_temperature"].to_numpy(), _df["level"].to_numpy() * 100)`. `np.deg2rad` for the
    wind direction.
    ///
    """)
    return


@app.cell
def _(MetDataset, mo, np, om, pc_error, thermo):
    mo.stop(pc_error is not None, mo.callout(mo.md("❌ pycontrails is missing – see above."), kind="danger"))

    # >>> TODO 16.1: met = MetDataset with air_temperature, specific_humidity, eastward_wind, northward_wind
    # ?? met = None
    _df = om.copy()
    _df["air_temperature"] = _df["t_c"] + 273.15
    _df["specific_humidity"] = _df["rh_pct"] / 100 * thermo.q_sat_liquid(_df["air_temperature"].to_numpy(),
                                                                        _df["level"].to_numpy() * 100)
    _wd = np.deg2rad(_df["wd"])
    _df["eastward_wind"] = -_df["ws"] * np.sin(_wd)
    _df["northward_wind"] = -_df["ws"] * np.cos(_wd)
    met = MetDataset(_df.set_index(["longitude", "latitude", "level", "time"])
                     [["air_temperature", "specific_humidity", "eastward_wind", "northward_wind"]].to_xarray())
    # <<< TODO 16.1
    return (met,)


@app.cell(hide_code=True)
def _(MetDataset, check, met):
    import math

    try:
        _vars = {"air_temperature", "specific_humidity", "eastward_wind", "northward_wind"}
        _ok = isinstance(met, MetDataset) and _vars <= set(met.data.data_vars)
        _t = met.data["air_temperature"] if _ok else None
        _ok = _ok and 190 < float(_t.min()) and float(_t.max()) < 270 and float(met.data["specific_humidity"].max()) < 0.01
        if _ok:
            _u = met.data["eastward_wind"].sel(level=250).mean().item()
            _v = met.data["northward_wind"].sel(level=250).mean().item()
            _msg = (f"MetDataset: {met.data.sizes['longitude']} × {met.data.sizes['latitude']} points, "
                    f"{met.data.sizes['level']} levels, {met.data.sizes['time']} hours – mean wind at 250 hPa "
                    f"**{3.6 * (_u ** 2 + _v ** 2) ** 0.5:.0f} km/h** blowing towards "
                    f"{['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW'][round(math.degrees(math.atan2(_u, _v)) % 360 / 45) % 8]} "
                    f"(u = {_u:+.0f}, v = {_v:+.0f} m/s; mean of the u and v components)")
        else:
            _msg = ""
        _out = check(_ok, _msg, "Expected a MetDataset with air_temperature (K), specific_humidity (kg/kg), "
                                "eastward_wind and northward_wind (m/s).")
    except Exception as _e:
        _out = check(False, "", f"Expected a MetDataset with the four variables ({_e}).")
    _out
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Sanity check: is the humidity plausible?

    Clear air cannot be arbitrarily ice-supersaturated: at a certain humidity, ice crystals form on their own
    (homogeneous nucleation). ECMWF's model uses the limit of Kärcher & Lohmann (2002),
    **RHi_homo = 2.583 − T / 207.8** (T in K; [IFS documentation Cy50r1, Part IV, eq. 7.23](https://www.ecmwf.int/sites/default/files/elibrary/062026/81734-ifs-documentation-cy50r1-part-4-physical-processes.pdf)) –
    147 % at −42 °C, 152 % at −52 °C, 157 % at −62 °C. The table converts the humidity to **over ice** twice:

    - `rhi_icon_pct`: ICON-EU, over water as DWD defines it (what we use)
    - `rhi_bestmatch_pct`: Open-Meteo's default model mix, **if** you read it as over water – values above the limit
      (`bestmatch_above_limit_pct`) are physically impossible, so that reading must be wrong for at least part of it.
    """)
    return


@app.cell
def _(LATS, LEVELS, LONS, cm, mo, np, om, pd, requests):
    def _homo_pct(t_c):
        return 100 * (2.583 - (np.asarray(t_c, float) + 273.15) / 207.8)

    _s = om.assign(rhi_icon_pct=cm.rh_water_to_ice(om["rh_pct"], om["t_c"]))
    try:                                                   # the same grid and hours with the default model choice
        _pts = [(la, lo) for la in LATS for lo in LONS]
        _r = requests.get("https://api.open-meteo.com/v1/forecast", timeout=60, params={
            "latitude": ",".join(str(p[0]) for p in _pts), "longitude": ",".join(str(p[1]) for p in _pts),
            "hourly": ",".join(f"{v}_{lvl}hPa" for lvl in LEVELS for v in ("temperature", "relative_humidity")),
            "start_date": f"{om.time.min():%Y-%m-%d}", "end_date": f"{om.time.max():%Y-%m-%d}", "timezone": "GMT"})
        _r.raise_for_status()
        _bm = pd.concat([pd.DataFrame({"time": pd.to_datetime(_loc["hourly"]["time"]), "level": float(_lvl),
                                       "t_c": _loc["hourly"][f"temperature_{_lvl}hPa"],
                                       "rh_pct": _loc["hourly"][f"relative_humidity_{_lvl}hPa"]})
                         for _loc in _r.json() for _lvl in LEVELS], ignore_index=True).dropna()
        _bm = _bm.assign(rhi=cm.rh_water_to_ice(_bm["rh_pct"], _bm["t_c"]), homo=_homo_pct(_bm["t_c"]))
        _bm_stats = _bm.groupby("level").agg(
            rhi_bestmatch_max_pct=("rhi", "max"),
            bestmatch_above_limit_pct=("rhi", lambda x: 100 * (x > _bm.loc[x.index, "homo"]).mean()))
    except Exception as _e:                               # the comparison is optional – show why it is missing
        print(f"default-model comparison skipped: {_e}")
        _bm_stats = pd.DataFrame()
    _s = _s.assign(homo=_homo_pct(_s["t_c"]))
    sanity = (_s.groupby("level")
              .agg(temp_c=("t_c", "mean"), rh_max_pct=("rh_pct", "max"),
                   rhi_icon_max_pct=("rhi_icon_pct", "max"),
                   icon_issr_pct=("rhi_icon_pct", lambda x: 100 * (x > 100).mean()),
                   icon_above_limit_pct=("rhi_icon_pct", lambda x: 100 * (x > _s.loc[x.index, "homo"]).mean()))
              .join(_bm_stats).round(1).sort_index(ascending=False).reset_index())
    sanity
    return (sanity,)


@app.cell
def _(met):
    met_used, pcr_params = met, {"rhi_threshold": 1.0}       # pycontrails' default: RHi > 100 %
    return met_used, pcr_params


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 16.2 – SAC, ISSR and PCR on the whole grid

    `PCR(met_used, params=pcr_params).eval()` without an argument evaluates every grid point and hour and returns a
    MetDataset with three new 0/1 variables: `sac`, `issr` and `pcr`. Store it in `grid`.

    Then compute `grid_share`: per **level** the share in % of grid points × hours with `sac`, `issr` and `pcr`
    (columns `level`, `sac_pct`, `issr_pct`, `pcr_pct`).

    /// details | Hint
    `grid.data` is an xarray Dataset: `grid.data[["sac", "issr", "pcr"]].mean(dim=["longitude", "latitude", "time"])`
    leaves one value per level; `.to_dataframe()`, `* 100`, `.reset_index()` and `rename(columns=...)`.
    ///
    """)
    return


@app.cell
def _(PCR, met_used, pcr_params, pd):
    # >>> TODO 16.2: grid = PCR on the grid; grid_share = level, sac_pct, issr_pct, pcr_pct
    # ?? grid = None
    # ?? grid_share = pd.DataFrame({"level": [250.0], "sac_pct": [0.0], "issr_pct": [0.0], "pcr_pct": [0.0]})
    grid = PCR(met_used, params=pcr_params).eval()
    grid_share = ((grid.data[["sac", "issr", "pcr"]].mean(dim=["longitude", "latitude", "time"])
                   .to_dataframe()[["sac", "issr", "pcr"]] * 100).round(1).reset_index()
                  .rename(columns={"sac": "sac_pct", "issr": "issr_pct", "pcr": "pcr_pct"}))
    # <<< TODO 16.2
    _ = pd
    return grid, grid_share


@app.cell(hide_code=True)
def _(check, grid, grid_share, mo):
    _out = []
    try:
        _cols = {"level", "sac_pct", "issr_pct", "pcr_pct"}
        _ok = (_cols <= set(grid_share.columns) and len(grid_share) == 5
               and (grid_share["pcr_pct"] <= grid_share[["sac_pct", "issr_pct"]].min(axis=1) + 0.1).all())
        _g = grid_share.set_index("level") if _ok else None
        _out.append(check(
            _ok,
            f"at 250 hPa (~34,000 ft): contrails form in **{_g.loc[250, 'sac_pct']:.0f} %**, the air is "
            f"ice-supersaturated in **{_g.loc[250, 'issr_pct']:.0f} %**, persistent contrail region in "
            f"**{_g.loc[250, 'pcr_pct']:.0f} %** of grid points × hours. At 400 hPa it is too warm to form one "
            f"({_g.loc[400, 'sac_pct']:.0f} %)" if _ok else "",
            "Expected one row per level (5) with sac_pct, issr_pct and pcr_pct – pcr can never exceed sac or issr."))
        if _ok:
            import plotly.express as px

            _pcr = grid.data["pcr"].mean(dim=["longitude", "latitude"]).transpose("level", "time") * 100
            _out.append(px.imshow(_pcr.values, x=_pcr["time"].values, y=[f"{int(lv)} hPa" for lv in _pcr["level"].values],
                                  aspect="auto", height=280, color_continuous_scale="Blues", zmin=0, zmax=100,
                                  labels={"color": "% PCR", "x": "UTC"}))
    except Exception as _e:
        _out.append(check(False, "", f"Expected grid (MetDataset) and grid_share ({_e})."))
    mo.vstack(_out)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## The observed cruise positions

    Every position of the Swiss feed **above 25,000 ft** inside the weather grid and window. `km` = distance until
    the aircraft's next snapshot (speed × time, at most 70 s – a gap in the data does not count as flown distance).
    """)
    return


@app.cell
def _(con, mo, om):
    cruise = con.execute("""
        WITH c AS (
            SELECT f.snapshot_time, f.icao24, f.callsign, f.latitude, f.longitude, f.altitude_m, f.altitude_ft,
                   f.speed_kmh, COALESCE(a.airline_name, 'unknown / private') AS airline,
                   COALESCE(t.typecode, '?') AS typecode,
                   LEAST(epoch(LEAD(f.snapshot_time) OVER (PARTITION BY f.icao24 ORDER BY f.snapshot_time))
                         - epoch(f.snapshot_time), 70) AS dt_s
            FROM silver.flights f
            LEFT JOIN silver.airlines a USING (airline_icao)
            LEFT JOIN silver.aircraft_types t USING (icao24)
            WHERE f.region = 'switzerland' AND NOT f.on_ground AND f.altitude_ft > 25000)
        SELECT * EXCLUDE (dt_s), speed_kmh * COALESCE(dt_s, 35) / 3600 AS km
        FROM c
        WHERE snapshot_time BETWEEN ? AND ? AND latitude BETWEEN 45 AND 48 AND longitude BETWEEN 5 AND 11
        ORDER BY snapshot_time""", [om["time"].min(), om["time"].max()]).df()
    mo.md(f"`cruise`: **{len(cruise):,} positions** of {cruise.icao24.nunique():,} aircraft, "
          f"{cruise.km.sum():,.0f} flight-km")
    return (cruise,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 16.3 – Sample the model at the aircraft

    A `GeoVectorDataset` is a set of points in space and time. Build one from `cruise` (`longitude`, `latitude`,
    `altitude` = `altitude_m`, `time` = `snapshot_time`) and evaluate `PCR(met_used, params=pcr_params)` on it:
    pycontrails **interpolates** the weather in longitude, latitude, pressure and time to every point (the altitude
    is converted to pressure with the standard atmosphere, `pycontrails.physics.units.m_to_pl`; OpenSky's
    `baro_altitude` is a barometric altitude).

    Store in `pos` a copy of `cruise` plus the columns `air_temperature`, `specific_humidity`, `air_pressure`, `sac`,
    `issr` and `pcr` of the result.

    /// details | Hint
    ```python
    _vec = GeoVectorDataset(longitude=cruise["longitude"].to_numpy(), latitude=..., altitude=...,
                            time=cruise["snapshot_time"].to_numpy())
    _res = PCR(met_used, params=pcr_params).eval(_vec)
    pos = cruise.copy()
    pos["pcr"] = _res["pcr"]          # same order as the input
    ```
    ///
    """)
    return


@app.cell
def _(GeoVectorDataset, PCR, cruise, met_used, np, pcr_params):
    # >>> TODO 16.3: pos = cruise + air_temperature, specific_humidity, air_pressure, sac, issr, pcr at each position
    # ?? pos = cruise.assign(sac=np.nan, issr=np.nan, pcr=np.nan)
    _vec = GeoVectorDataset(longitude=cruise["longitude"].to_numpy(float), latitude=cruise["latitude"].to_numpy(float),
                            altitude=cruise["altitude_m"].to_numpy(float), time=cruise["snapshot_time"].to_numpy())
    _res = PCR(met_used, params=pcr_params).eval(_vec)
    pos = cruise.copy()
    for _c in ["air_temperature", "specific_humidity", "air_pressure", "sac", "issr", "pcr"]:
        pos[_c] = _res[_c]
    # <<< TODO 16.3
    _ = np
    return (pos,)


@app.cell(hide_code=True)
def _(check, pos):
    try:
        _ok = ({"sac", "issr", "pcr"} <= set(pos.columns) and len(pos) > 100 and pos["pcr"].notna().mean() > 0.95
               and (pos["pcr"].fillna(0) <= pos["sac"].fillna(0)).all())
        _out = check(_ok, f"{len(pos):,} cruise positions: **{pos.sac.mean() * 100:.0f} %** form a contrail, "
                          f"**{pos.pcr.mean() * 100:.0f} %** in a persistent contrail region "
                          f"({pos.pcr.isna().mean() * 100:.1f} % outside the grid)" if _ok else "",
                     "Expected cruise plus sac, issr and pcr (0/1) for (almost) every position.")
    except Exception as _e:
        _out = check(False, "", f"Expected pos with sac, issr and pcr ({_e}).")
    _out
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Hand-made (09) vs. pycontrails – on the same weather

    The cell below runs the functions of notebook 09 (`cm.sac_threshold_temps`, `cm.rh_water_to_ice`) on the
    temperature and humidity pycontrails interpolated to each position. Differences come from the physics
    constants and the saturation vapour pressure formula (09: Murphy & Koop 2005; pycontrails: Sonntag 1994, see
    `pycontrails.physics.thermo`) – not from the weather. In 09 itself the weather was different too: nearest 1° cell and one of three levels, no interpolation.
    """)
    return


@app.cell
def _(cm, mo, pcr_params, pd, pos, thermo):
    _need = {"air_temperature", "specific_humidity", "air_pressure", "sac", "pcr"}
    mo.stop(not _need <= set(pos.columns) or pos["pcr"].isna().all(),
            mo.md("*finish TODO 16.3 first (with the columns air_temperature, specific_humidity, air_pressure)*"))
    _p = pos.dropna(subset=list(_need))
    _t_c = _p["air_temperature"].to_numpy() - 273.15
    _rhw = 100 * thermo.rh(_p["specific_humidity"].to_numpy(), _p["air_temperature"].to_numpy(),
                           _p["air_pressure"].to_numpy())
    _, _t_lc = cm.sac_threshold_temps(_p["air_pressure"].to_numpy() / 100, _rhw)
    _forms_09 = _t_c < _t_lc
    _pers_09 = _forms_09 & (cm.rh_water_to_ice(_rhw, _t_c) > 100 * pcr_params["rhi_threshold"])
    compare_09 = pd.DataFrame({
        "": ["contrail forms (SAC)", "persistent (PCR)"],
        "09 hand-made %": [100 * _forms_09.mean(), 100 * _pers_09.mean()],
        "pycontrails %": [100 * _p["sac"].mean(), 100 * _p["pcr"].mean()],
        "same verdict %": [100 * (_forms_09 == (_p["sac"] == 1)).mean(), 100 * (_pers_09 == (_p["pcr"] == 1)).mean()],
    }).round(1)
    compare_09
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 16.4 – Persistent contrail kilometres per airline

    Per `airline` in `pos`: total `km`, `pcr_km` (km where `pcr == 1`), number of aircraft, and
    `pcr_share_pct` = pcr_km / km × 100. Only airlines with **at least 1,000 km**, highest share first. Store it in
    `by_airline` (columns `airline`, `km`, `pcr_km`, `aircraft`, `pcr_share_pct`).

    /// details | Hint
    `pos.assign(pcr_km=pos["km"] * pos["pcr"].fillna(0)).groupby("airline").agg(km=("km", "sum"), ...)`,
    then `.query("km >= 1000")`.
    ///
    """)
    return


@app.cell
def _(pd, pos):
    # >>> TODO 16.4: by_airline = airline, km, pcr_km, aircraft, pcr_share_pct (>= 1000 km, highest share first)
    # ?? by_airline = pd.DataFrame({"airline": ["TODO"], "km": [0.0], "pcr_km": [0.0], "aircraft": [0], "pcr_share_pct": [0.0]})
    by_airline = (pos.assign(pcr_km=pos["km"] * pos["pcr"].fillna(0))
                  .groupby("airline")
                  .agg(km=("km", "sum"), pcr_km=("pcr_km", "sum"), aircraft=("icao24", "nunique"))
                  .query("km >= 1000")
                  .assign(pcr_share_pct=lambda d: (100 * d["pcr_km"] / d["km"]).round(1))
                  .sort_values("pcr_share_pct", ascending=False)
                  .reset_index())
    # <<< TODO 16.4
    _ = pd
    return (by_airline,)


@app.cell(hide_code=True)
def _(by_airline, check, mo, pos):
    _out = []
    try:
        _cols = {"airline", "km", "pcr_km", "aircraft", "pcr_share_pct"}
        _ok = (_cols <= set(by_airline.columns) and len(by_airline) > 3 and by_airline["km"].min() >= 1000
               and by_airline["pcr_share_pct"].is_monotonic_decreasing and by_airline["km"].sum() > 0)
        _total = 100 * (pos["km"] * pos["pcr"].fillna(0)).sum() / pos["km"].sum() if "pcr" in pos else 0
        _out.append(check(
            _ok,
            f"**{_total:.0f} % of all cruise flight-km** ran through persistent contrail regions. "
            f"{len(by_airline)} airlines – highest: {by_airline.airline.iloc[0]} ({by_airline.pcr_share_pct.iloc[0]:.0f} %), "
            f"lowest: {by_airline.airline.iloc[-1]} ({by_airline.pcr_share_pct.iloc[-1]:.0f} %)" if _ok else "",
            "Expected airline, km, pcr_km, aircraft and pcr_share_pct (>= 1,000 km), highest share first."))
        if _ok:
            import plotly.express as px_airline

            _top = by_airline.nlargest(15, "km").sort_values("pcr_share_pct")
            _out.append(px_airline.bar(_top, x="pcr_share_pct", y="airline", orientation="h", height=420,
                                       hover_data=["km", "aircraft"],
                                       labels={"pcr_share_pct": "% of flight-km in persistent contrail regions",
                                               "airline": ""}, title="the 15 airlines with the most km"))
    except Exception as _e:
        _out.append(check(False, "", f"Expected by_airline ({_e})."))
    mo.vstack(_out)
    return


@app.cell
def _(mo, pos):
    _ok = "pcr" in pos and pos["pcr"].notna().any()
    _hours = sorted(pos["snapshot_time"].dt.floor("h").astype(str).unique()) if _ok else []
    _busy = pos["snapshot_time"].dt.floor("h").astype(str).value_counts().idxmax() if _ok else None
    hour_pick = mo.ui.dropdown(options=_hours, value=_busy, label="Hour (UTC) on the map")
    hour_pick
    return (hour_pick,)


@app.cell
def _(hour_pick, mo, pd, pos):
    mo.stop(not hour_pick.value, mo.md("*finish TODO 16.3 to see the map*"))
    import plotly.express as px_map

    _h = pos[pos["snapshot_time"].dt.floor("h") == pd.Timestamp(hour_pick.value)]
    _h = _h.assign(contrail=_h["pcr"].map({1.0: "persistent region", 0.0: "no persistent contrail"}).fillna("outside grid"))
    _by_type = (pos.assign(pcr_km=pos["km"] * pos["pcr"].fillna(0)).groupby("typecode")
                .agg(km=("km", "sum"), pcr_km=("pcr_km", "sum")).query("km >= 1000")
                .assign(pcr_share_pct=lambda d: (100 * d.pcr_km / d.km).round(1)).nlargest(10, "km").round(0))
    mo.vstack([
        px_map.scatter_map(_h, lat="latitude", lon="longitude", color="contrail", hover_name="callsign",
                           hover_data=["airline", "typecode", "altitude_ft"], zoom=6, height=480,
                           map_style="carto-darkmatter",
                           color_discrete_map={"persistent region": "white", "no persistent contrail": "#3b82f6",
                                               "outside grid": "grey"}),
        mo.md("**By aircraft type** (the 10 types with the most km):"), _by_type.reset_index()])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## What if: 2,000 ft higher or lower?

    The cell below evaluates the model again at every persistent-contrail position **2,000 ft (610 m) higher and
    lower** – how many of those kilometres could a small level change have avoided? Over Japan,
    [Teoh et al. (2020)](https://doi.org/10.1021/acs.est.9b05608) estimated that diverting 1.7 % of the flights
    could reduce the contrail energy forcing by up to 59 %. Mind the resolution: our levels are 50 hPa apart
    (about 1.2–1.6 km), thinner humid layers are invisible to this grid.
    """)
    return


@app.cell
def _(GeoVectorDataset, PCR, met_used, mo, pcr_params, pos):
    mo.stop("pcr" not in pos or not (pos["pcr"] == 1).any(), mo.md("*needs TODO 16.3 (and some PCR positions)*"))
    _p = pos[pos["pcr"] == 1]
    _alt = {}
    for _dz in (-610.0, 610.0):
        _v = GeoVectorDataset(longitude=_p["longitude"].to_numpy(float), latitude=_p["latitude"].to_numpy(float),
                              altitude=_p["altitude_m"].to_numpy(float) + _dz, time=_p["snapshot_time"].to_numpy())
        _alt[_dz] = PCR(met_used, params=pcr_params).eval(_v)["pcr"]
    _avoid = (_alt[-610.0] == 0) | (_alt[610.0] == 0)
    mo.md(f"Of **{_p['km'].sum():,.0f} persistent-contrail km**, **{100 * _p['km'][_avoid].sum() / _p['km'].sum():.0f} %** "
          f"would be outside a persistent region 2,000 ft lower or higher "
          f"(lower: {100 * _p['km'][_alt[-610.0] == 0].sum() / _p['km'].sum():.0f} %, "
          f"higher: {100 * _p['km'][_alt[610.0] == 0].sum() / _p['km'].sum():.0f} %).")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Out of scope: CoCiP – how warm does a contrail make it?

    PCR tells you *where* a contrail would persist, not whether it **warms or cools** (by day it also reflects
    sunlight). pycontrails' **CoCiP** model follows every contrail segment over its life (default maximum age 20 h,
    `CocipParams.max_age`) and computes its energy forcing – but it needs weather that we did not load: **vertical
    velocity** and **cloud ice** on pressure levels plus **top-of-atmosphere shortwave and longwave radiation**
    (`Cocip.met_variables`, `Cocip.rad_variables`). pycontrails has data loaders for this, e.g.
    `pycontrails.datalib.ecmwf.ERA5` (Copernicus Climate Data Store, account needed) and
    `pycontrails.datalib.gfs.GFSForecast` (NOAA GFS).
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Think about it

    - Look at the sanity table: what would the share of persistent-contrail km be with the default model mix read
      as "over water"? What does that say about any contrail number without a stated humidity definition?
    - **Contrail avoidance**: if 2,000 ft avoid a large share of persistent contrail km, what could stop airlines
      from doing it every day? Collect arguments – and look for sources for each.
    - **CO₂ vs. non-CO₂**: an extra 1 % of fuel to avoid a strongly warming contrail – which numbers would you need
      to decide whether that is a good trade?
    - Night flights: at night a contrail cannot reflect sunlight ([Stuber et al. 2006](https://doi.org/10.1038/nature04877)).
      Which of your PCR positions were at night?

    **Next:** [17 – Detecting disruptions](17_disruptions.py)
    """)
    return


if __name__ == "__main__":
    app.run()
