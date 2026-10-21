import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium", app_title="09 – Weather and contrails")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 09 – Weather at flight level, and the contrails CO₂ calculators leave out

    In the ICAO standard atmosphere it is −54 °C at 35,000 ft; the real temperature and the wind at that height come
    from a weather model (your data will show how strong the wind is). Wind changes the flight time (and the fuel), and
    cold, humid air decides whether an aircraft leaves a **contrail** – white lines that can spread into contrail
    cirrus. For 2018, [Lee et al. (2021)](https://doi.org/10.1016/j.atmosenv.2020.117834) estimate the effective
    radiative forcing of contrail cirrus at 57.4 mW m⁻², that of aviation’s CO₂ at 34.3 mW m⁻². A pure CO₂ figure leaves
    it out; some calculators add the non-CO₂ effects as a multiplier (myclimate: RFI 3 since 2023,
    [press release](https://www.myclimate.org/fileadmin/user_upload/myclimate_-_home/01_Information/02_News_Press/03_Press_releases/Documents/231127_Pressemitteilung_Neuer_Flugrechner_AT.pdf)).

    [Open-Meteo](https://open-meteo.com) (free, no key) has hourly weather on **pressure levels** – also for the past:
    `past_days` up to 92 or `start_date`/`end_date` on the [forecast API](https://open-meteo.com/en/docs), older dates on
    the [historical forecast API](https://open-meteo.com/en/docs/historical-forecast-api) (DWD ICON from 2022-11-24).
    So we can attach the weather to **every** position of your history, not only the current ones.

    | pressure level | altitude in the ICAO standard atmosphere |
    |---|---|
    | 300 hPa | 30,065 ft (9,164 m) |
    | 250 hPa | 33,999 ft (10,363 m) |
    | 200 hPa | 38,662 ft (11,784 m) |
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
    from enrichment import contrails as cm                # the physics (Schmidt-Appleman) of the app

    def check(ok, good, bad):
        return mo.callout(mo.md(("✅ " + good) if ok else ("❌ " + bad)), kind="success" if ok else "danger")

    con = fl.snapshot()
    span = con.execute("SELECT MIN(snapshot_time), MAX(snapshot_time) FROM silver.flights").fetchone()
    mo.md(f"your data: **{span[0]:%Y-%m-%d %H:%M} – {span[1]:%Y-%m-%d %H:%M} UTC**")
    return check, cm, con, np, pd, requests, span


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 9.1 – Past weather for one place

    Write `weather_history(lat, lon, start_date, end_date)`: call
    `https://api.open-meteo.com/v1/forecast` with `latitude`, `longitude`, `start_date`, `end_date` (YYYY-MM-DD),
    `models=icon_eu` and `hourly` = the variables `temperature_{L}hPa`, `relative_humidity_{L}hPa`,
    `wind_speed_{L}hPa`, `wind_direction_{L}hPa` for L in 300, 250, 200 (comma-separated).

    **Why `models=icon_eu`?** Weather models define relative humidity differently: DWD's ICON
    [with respect to water](https://www.dwd.de/DWD/forschung/nwv/fepub/icon_database_main.pdf) (Database Reference,
    pressure-level fields), ECMWF's IFS
    [with respect to ice below −23 °C](https://confluence.ecmwf.int/spaces/UER/pages/51711725/Relative+humidity+pressure+level).
    Without `models`, Open-Meteo picks a model per location ("best match") and its documentation does not say which
    definition you get. For contrails that difference is everything – so pick one model and know its definition.

    The answer is **wide**: one list per variable. Turn it into a **long** DataFrame with one row per hour and level:
    `time` (datetime, UTC), `level_hpa`, `temp_c`, `rh_pct`, `wind_kmh`, `wind_dir`.

    /// details | Hint
    `h = r.json()["hourly"]`; for each level build a DataFrame `{"time": pd.to_datetime(h["time"]), "level_hpa": L,
    "temp_c": h[f"temperature_{L}hPa"], ...}` and `pd.concat` the three.
    ///
    """)
    return


@app.cell
def _(pd, requests):
    LEVELS = [300, 250, 200]

    # >>> TODO 9.1: weather_history(lat, lon, start_date, end_date) -> long DataFrame (time, level_hpa, temp_c, rh_pct, wind_kmh, wind_dir)
    # ?? def weather_history(lat, lon, start_date, end_date):
    # ??     return pd.DataFrame()
    def weather_history(lat, lon, start_date, end_date):
        hourly = ",".join(f"{v}_{lvl}hPa" for lvl in LEVELS
                          for v in ("temperature", "relative_humidity", "wind_speed", "wind_direction"))
        r = requests.get("https://api.open-meteo.com/v1/forecast", timeout=30, params={
            "latitude": lat, "longitude": lon, "start_date": start_date, "end_date": end_date, "hourly": hourly,
            "models": "icon_eu"})                       # RH with respect to water (DWD) – see the text above
        r.raise_for_status()
        h = r.json()["hourly"]
        return pd.concat([pd.DataFrame({
            "time": pd.to_datetime(h["time"]), "level_hpa": lvl, "temp_c": h[f"temperature_{lvl}hPa"],
            "rh_pct": h[f"relative_humidity_{lvl}hPa"], "wind_kmh": h[f"wind_speed_{lvl}hPa"],
            "wind_dir": h[f"wind_direction_{lvl}hPa"]}) for lvl in LEVELS], ignore_index=True)
    # <<< TODO 9.1
    return LEVELS, weather_history


@app.cell(hide_code=True)
def _(check, span, weather_history):
    _w = weather_history(47.0, 8.0, f"{span[0]:%Y-%m-%d}", f"{span[1]:%Y-%m-%d}")
    _cols = {"time", "level_hpa", "temp_c", "rh_pct", "wind_kmh", "wind_dir"}
    _ok = _cols <= set(_w.columns) and len(_w) >= 72 and set(_w.get("level_hpa", [])) == {300, 250, 200}
    check(_ok, f"{len(_w)} rows (hours × 3 levels) over central Switzerland – at 250 hPa between "
               f"{_w[_w.level_hpa == 250].temp_c.min():.0f} and {_w[_w.level_hpa == 250].temp_c.max():.0f} °C"
          if _ok else "", f"Expected a long table with the columns {sorted(_cols)}, 24 hours × 3 levels per day.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Weather for every cell aircraft cruised through

    The cell below rounds the cruise positions to a **1° grid** (≈ 111 × 75 km), takes the 12 busiest cells and loads
    their history with your function – into the table `weather_history` of your snapshot. 12 calls, well within
    Open-Meteo's free 10,000 per day.
    """)
    return


@app.cell
def _(con, mo, pd, span, weather_history):
    _cells = con.execute("""
        SELECT ROUND(latitude) AS cell_lat, ROUND(longitude) AS cell_lon, COUNT(*) AS n FROM silver.flights
        WHERE NOT on_ground AND altitude_ft >= 20000 GROUP BY ALL ORDER BY n DESC LIMIT 12""").df()
    _parts = []
    with mo.status.progress_bar(total=len(_cells), title="weather history") as _bar:
        for _c in _cells.itertuples():
            _parts.append(weather_history(_c.cell_lat, _c.cell_lon, f"{span[0]:%Y-%m-%d}", f"{span[1]:%Y-%m-%d}")
                          .assign(cell_lat=_c.cell_lat, cell_lon=_c.cell_lon))
            _bar.update()
    history = pd.concat(_parts, ignore_index=True)
    if {"time", "level_hpa"} <= set(history.columns):
        con.execute("CREATE OR REPLACE TABLE weather_history AS SELECT * FROM history")
    else:                                            # TODO 9.1 not done yet: an empty table with the right columns
        con.execute("CREATE OR REPLACE TABLE weather_history (time TIMESTAMP, level_hpa INTEGER, temp_c DOUBLE, "
                    "rh_pct DOUBLE, wind_kmh DOUBLE, wind_dir DOUBLE, cell_lat DOUBLE, cell_lon DOUBLE)")
    mo.md(f"`weather_history`: **{len(history):,} rows** for {len(_cells)} cells")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 9.2 – Attach the weather to every cruise position

    Write the SQL for `with_wind`: every position of `silver.flights` **above 20,000 ft and in the air**, joined to
    `weather_history` on

    - the **hour**: `date_trunc('hour', snapshot_time)` = `time`
    - the **cell**: `ROUND(latitude)` = `cell_lat`, `ROUND(longitude)` = `cell_lon`
    - the **level**: below 32,000 ft → 300, below 37,000 ft → 250, else 200

    and add `headwind_kmh` = `wind_kmh * COS(RADIANS(wind_dir - heading))` (positive = headwind: the wind blows
    *from* the direction the aircraft flies *to*).
    """)
    return


@app.cell
def _(con, mo):
    # >>> TODO 9.2: with_wind = cruise positions + weather of their hour, cell and level + headwind_kmh
    # ?? with_wind = mo.sql("SELECT * FROM silver.flights LIMIT 10", engine=con)
    with_wind = mo.sql(
        """
        WITH cruise AS (
            SELECT *, date_trunc('hour', snapshot_time) AS hour, ROUND(latitude) AS cell_lat,
                   ROUND(longitude) AS cell_lon,
                   CASE WHEN altitude_ft < 32000 THEN 300 WHEN altitude_ft < 37000 THEN 250 ELSE 200 END AS level_hpa
            FROM silver.flights WHERE NOT on_ground AND altitude_ft >= 20000)
        SELECT c.snapshot_time, c.icao24, c.callsign, c.altitude_ft, c.altitude_m, c.speed_kmh, c.heading,
               c.level_hpa, w.temp_c, w.rh_pct, w.wind_kmh, w.wind_dir,
               w.wind_kmh * COS(RADIANS(w.wind_dir - c.heading)) AS headwind_kmh
        FROM cruise c
        JOIN weather_history w
          ON w.time = c.hour AND w.cell_lat = c.cell_lat AND w.cell_lon = c.cell_lon AND w.level_hpa = c.level_hpa
        """,
        engine=con,
    )
    # <<< TODO 9.2
    return (with_wind,)


@app.cell(hide_code=True)
def _(check, mo, with_wind):
    _ok = {"temp_c", "headwind_kmh", "level_hpa"} <= set(with_wind.columns) and len(with_wind) > 100
    _out = [check(_ok, f"{len(with_wind):,} cruise positions with weather – mean headwind "
                       f"{with_wind.headwind_kmh.mean():+.0f} km/h, coldest {with_wind.temp_c.min():.0f} °C" if _ok
                  else "", "Expected cruise positions joined with weather_history plus headwind_kmh.")]
    if _ok:
        import plotly.express as px_wind
        _out.append(px_wind.histogram(with_wind, x="headwind_kmh", nbins=60, height=300,
                                 labels={"headwind_kmh": "headwind (+) / tailwind (−) in km/h"}))
    mo.vstack(_out)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 9.3 – Who leaves a contrail – and does it persist?

    The **Schmidt-Appleman criterion**: the hot, humid exhaust mixes with the cold air. Below a threshold temperature
    `t_lc` (which depends on pressure and humidity) water condenses and freezes – a contrail **forms**. It **persists**
    only when the air is **ice-supersaturated** (relative humidity over ice ≥ 100 %) – the "persistent contrail region"
    of [pycontrails](https://py.contrails.org/api/pycontrails.models.pcr.html) is exactly this intersection; in drier
    air the ice crystals sublimate and the contrail is short-lived.

    The physics is given (`cm` = the contrail module of the app). Add three columns to `ct`:

    - `forms` = `temp_c < t_lc` with `_, t_lc = cm.sac_threshold_temps(pressure, rh_pct)` and
      `pressure = cm.isa_pressure_hpa(altitude_m)`
    - `rh_ice` = `cm.rh_water_to_ice(rh_pct, temp_c)` – ICON's humidity is over water; the function converts it with
      the saturation pressures of [Murphy & Koop (2005)](https://doi.org/10.1256/qj.04.94): `RHi = RHw · e_w(T) / e_i(T)`
    - `persistent` = `forms` and `rh_ice >= 100`

    All functions take whole columns (numpy arrays) – no loop needed.
    """)
    return


@app.cell
def _(cm, np, with_wind):
    ct = with_wind.copy()

    # >>> TODO 9.3: ct["forms"], ct["rh_ice"], ct["persistent"]
    # ?? pass
    _pressure = cm.isa_pressure_hpa(ct["altitude_m"].to_numpy(float))
    _, _t_lc = cm.sac_threshold_temps(_pressure, ct["rh_pct"].to_numpy(float))
    ct["forms"] = ct["temp_c"].to_numpy(float) < _t_lc
    ct["rh_ice"] = cm.rh_water_to_ice(ct["rh_pct"].to_numpy(float), ct["temp_c"].to_numpy(float))
    ct["persistent"] = ct["forms"] & (ct["rh_ice"] >= 100)
    # <<< TODO 9.3
    _ = np
    return (ct,)


@app.cell(hide_code=True)
def _(check, ct, mo):
    _ok = {"forms", "persistent", "rh_ice"} <= set(ct.columns) and len(ct) > 0
    _out = [check(_ok and (ct["persistent"] <= ct["forms"]).all(),
                  f"{ct.forms.mean() * 100:.0f} % of the cruise positions form a contrail, "
                  f"{ct.persistent.mean() * 100:.0f} % persist" if _ok else "",
                  "Add forms, rh_ice and persistent (persistent only where forms is True).")]
    if _ok:
        import plotly.express as px_contrails
        _hourly = (ct.assign(hour=ct.snapshot_time.dt.floor("h")).groupby("hour")
                   .agg(forming=("forms", "mean"), persistent=("persistent", "mean")).reset_index())
        _hourly[["forming", "persistent"]] *= 100
        _out.append(px_contrails.line(_hourly, x="hour", y=["forming", "persistent"], height=320,
                            labels={"value": "% of cruise positions", "hour": "", "variable": ""}))
    mo.vstack(_out)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Think about it

    - At which hours and altitudes do persistent contrails occur? At night contrails only trap heat, by day they also
      reflect sunlight: [Stuber et al. (2006)](https://doi.org/10.1038/nature04877) found night flights (25 % of the
      traffic over south-east England) responsible for up to 80 % of the contrail warming there.
    - Persistent contrails are concentrated: over Japan, 2.2 % of the flights caused 80 % of the contrail energy
      forcing ([Teoh et al. 2020](https://doi.org/10.1021/acs.est.9b05608)). What could an airline do with that
      knowledge? (The same paper: diverting 1.7 % of the flights cut it by up to 59 %.)
    - The headwind distribution: is the mean zero over Switzerland? If not, from which direction does the wind blow
      at cruise level in your data?
    - `weather_history` covers your backfill too – that is the payoff of storing history: analyses you did not plan
      when you started collecting.

    **Next:** [10 – Analyses](10_analysis.py). Notebook [16](16_pycontrails.py) does the contrail part with
    pycontrails on a whole grid. The app (`uv run python app/app.py`) puts all of it on one map.
    """)
    return


if __name__ == "__main__":
    app.run()
