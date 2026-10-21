import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium", app_title="13 – Ground weather at Kloten")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 13 – Ground weather at Kloten: does the weather move the traffic?

    MeteoSwiss runs an automatic weather station **at Zurich airport** (Zürich / Kloten, `KLO`, 426 m, 47.480 N /
    8.536 E – about 2.6 km north-north-west of the airport reference point in
    [OurAirports](https://ourairports.com/data/);
    [station list](https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/ogd-smn_meta_stations.csv)). Since May 2025
    MeteoSwiss publishes its measurements as **open data**
    ([press release](https://www.swisstopo.admin.ch/en/weather-and-climate-data-freely-available)): plain file
    download without a key, licence CC BY 4.0, attribution "Source: MeteoSwiss"
    ([terms](https://opendatadocs.meteoswiss.ch/general/terms-of-use)). One CSV row every **10 minutes** with
    temperature, wind, gusts, precipitation, pressure and more.

    In this notebook you load the station into DuckDB, put it next to the departures and arrivals at LSZH, compare it
    with the pilots' weather report (**METAR**) and with the model weather of Open-Meteo (notebook 09), and look at
    which way the aircraft take off.

    | file | content |
    |---|---|
    | `ogd-smn_klo_t_recent.csv` | 10-minute values from 1 January until yesterday (~6 MB, rebuilt twice a day) |
    | `ogd-smn_klo_t_now.csv` | the latest 10-minute values (updated every 10 minutes) |

    Format: `;` as separator, timestamps in **UTC** as `dd.mm.yyyy HH:MM` (column `reference_timestamp`), one column
    per parameter code (e.g. `tre200s0` = air temperature 2 m above ground). For 10-minute values the timestamp is
    the **end** of the interval: `10:10` = 10:00–10:10
    ([MeteoSwiss download docs](https://opendatadocs.meteoswiss.ch/general/download)). The docs say `now` starts
    "yesterday 12 UTC"; the file we downloaded on 20.10.2026 started at 00:00 UTC – check it yourself.
    """)
    return


@app.cell
def _(mo):
    import sys
    from urllib.parse import urlencode

    import pandas as pd
    import plotly.express as px

    sys.path.insert(0, str(mo.notebook_dir().parent))
    import flightlab as fl
    from enrichment import metar

    def check(ok, good, bad):
        return mo.callout(mo.md(("✅ " + good) if ok else ("❌ " + bad)), kind="success" if ok else "danger")

    _base = "https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn"
    with mo.status.spinner(title="downloading the Kloten station (MeteoSwiss, ~6 MB once a day) ..."):
        klo_recent = fl.cached(f"{_base}/klo/ogd-smn_klo_t_recent.csv", "meteoswiss_klo_t_recent.csv", 1)
        klo_now = fl.cached(f"{_base}/klo/ogd-smn_klo_t_now.csv", "meteoswiss_klo_t_now.csv", 10 / 1440)
        klo_params = fl.cached(f"{_base}/ogd-smn_meta_parameters.csv", "meteoswiss_meta_parameters.csv", 30)

    con = fl.snapshot()
    con.execute("CREATE SCHEMA IF NOT EXISTS weather")
    span = con.execute("SELECT MIN(snapshot_time), MAX(snapshot_time) FROM silver.flights "
                       "WHERE region = 'switzerland'").fetchone()
    mo.md(f"your Swiss flight data: **{span[0]:%d.%m. %H:%M} – {span[1]:%d.%m. %H:%M} UTC** · station files in "
          f"`cache/`")
    return check, con, fl, klo_now, klo_params, klo_recent, metar, pd, px, span, urlencode


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### The parameters we use

    The codes are explained in a metadata file. MeteoSwiss writes its CSV files in **Windows-1252**, not UTF-8
    ([docs](https://opendatadocs.meteoswiss.ch/general/download)); DuckDB's `latin-1` decodes the `°` and `²` of
    this file the same way.
    """)
    return


@app.cell
def _(con, klo_params):
    codes = {"tre200s0": "temp_c", "ure200s0": "rh_pct", "dkl010z0": "wind_dir_deg", "fu3010z0": "wind_kmh",
             "fu3010z1": "gust_kmh", "rre150z0": "precip_mm", "pp0qnhs0": "qnh_hpa", "gre000z0": "radiation_wm2"}
    con.execute(f"""
        SELECT parameter_shortname AS code, parameter_description_en AS description, parameter_unit AS unit
        FROM read_csv('{klo_params}', sep = ';', encoding = 'latin-1')
        WHERE parameter_shortname IN ({", ".join(f"'{c}'" for c in codes)})""").df()
    return (codes,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 13.1 – Load the station into DuckDB

    Create the table **`weather.kloten_10min`** from **both** files (`klo_recent` and `klo_now`, paths in the setup
    cell) with one row per 10 minutes and readable column names:

    `time_utc` (TIMESTAMP), `temp_c`, `rh_pct`, `wind_dir_deg`, `wind_kmh`, `gust_kmh`, `precip_mm`, `qnh_hpa`,
    `radiation_wm2` (the mapping code → name is the dict `codes` above).

    /// details | Hint
    `read_csv` takes a list of files. Two traps: DuckDB does not recognise `20.10.2026 14:00` as a timestamp (the
    column stays VARCHAR) – give the format; and it guesses the column types from a **sample** of rows – a column
    that is empty or whole-numbered in the sample gets the wrong type. `sample_size = -1` reads all rows first.
    ```sql
    CREATE OR REPLACE TABLE weather.kloten_10min AS
    SELECT DISTINCT reference_timestamp AS time_utc, tre200s0 AS temp_c, ...
    FROM read_csv(['<recent>', '<now>'], sep = ';', sample_size = -1, timestampformat = '%d.%m.%Y %H:%M')
    ```
    `DISTINCT` because the two files may overlap around midnight.
    ///
    """)
    return


@app.cell
def _(codes, con, klo_now, klo_recent):
    # TODO 13.1: weather.kloten_10min from both CSV files (time_utc + the 8 parameters with readable names)
    con.execute("CREATE OR REPLACE TABLE weather.kloten_10min (time_utc TIMESTAMP, temp_c DOUBLE, rh_pct DOUBLE, wind_dir_deg DOUBLE, wind_kmh DOUBLE, gust_kmh DOUBLE, precip_mm DOUBLE, qnh_hpa DOUBLE, radiation_wm2 DOUBLE)")
    kloten_rows = con.execute("SELECT COUNT(*) FROM weather.kloten_10min").fetchone()[0]
    return (kloten_rows,)


@app.cell(hide_code=True)
def _(check, con, kloten_rows):
    try:
        _s = con.execute("""SELECT MIN(time_utc), MAX(time_utc), COUNT(*) - COUNT(DISTINCT time_utc),
                                   typeof(ANY_VALUE(time_utc)), MAX(gust_kmh), SUM(precip_mm)
                            FROM weather.kloten_10min""").fetchone()
        _ok = kloten_rows > 1000 and _s[2] == 0 and _s[3] == "TIMESTAMP" and _s[1].date() >= _s[0].date()
        _msg = (f"{kloten_rows:,} ten-minute rows from {_s[0]:%d.%m.%Y} to {_s[1]:%d.%m.%Y %H:%M} UTC – strongest gust "
                f"{_s[4]:.0f} km/h, {_s[5]:.0f} mm precipitation this year" if _ok else "")
    except Exception as _e:
        _ok, _msg = False, ""
    check(_ok, _msg, "Expected weather.kloten_10min with one row per 10 minutes since 1 January (both files), "
                     "time_utc as TIMESTAMP and no duplicate times.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 13.2 – Traffic and weather per 10 minutes

    Count per **10 minutes (UTC)** the **departures** and **arrivals** at LSZH (47.4581, 8.5481, airport reference point from
    [OurAirports](https://ourairports.com/data/)) as in notebook 10:
    different aircraft within **15 km**, **below 6,000 ft**, `flight_phase = 'climbing'` (departure) or
    `'descending'` (arrival). Put the station's `wind_kmh`, `gust_kmh`, `precip_mm` and `wind_dir_deg` of the same
    10 minutes next to it. Store the result in `traffic_wx` with the columns `t_utc`, `snapshots`, `departures`,
    `arrivals`, `wind_kmh`, `gust_kmh`, `precip_mm`, `wind_dir_deg`.

    **A zero must be a real zero.** If the collector was not running, there are no snapshots – and no aircraft. So
    also count the snapshots per 10 minutes (`snapshots`), keep only 10 minutes with **at least 5 snapshots**, and
    only the operating hours **06:00–22:59 Swiss time** (the night curfew is not weather either).

    /// details | Hint
    Three CTEs: the snapshots per bucket (the "spine"), the movements per bucket, the weather – then
    `spine JOIN weather LEFT JOIN movements` and `COALESCE(departures, 0)`.
    `time_bucket(INTERVAL 10 MINUTE, snapshot_time)` gives the **start** of each 10 minutes (10:00 = 10:00–10:10),
    the station stamps the **end** (10:10 = 10:00–10:10) – so join on `w.time_utc = s.t_utc + INTERVAL 10 MINUTE`.
    ///
    """)
    return


@app.cell
def _(con, kloten_rows, mo):
    kloten_rows                                          # rerun after 13.1
    # TODO 13.2: traffic_wx = departures / arrivals at LSZH per 10 minutes with snapshots and station weather
    traffic_wx = mo.sql("SELECT TIMESTAMP '2026-10-20 12:00' AS t_utc, 0 AS snapshots, 0 AS departures, 0 AS arrivals, 0.0 AS wind_kmh, 0.0 AS gust_kmh, 0.0 AS precip_mm, 0.0 AS wind_dir_deg", engine=con)
    return (traffic_wx,)


@app.cell(hide_code=True)
def _(check, mo, pd, px, traffic_wx):
    _cols = {"t_utc", "snapshots", "departures", "arrivals", "wind_kmh", "gust_kmh", "precip_mm", "wind_dir_deg"}
    _out = []
    try:
        _df = traffic_wx.copy()
        _ok = (_cols <= set(_df.columns) and len(_df) > 30 and _df["departures"].sum() > 20
               and _df["snapshots"].min() >= 5 and _df["wind_kmh"].notna().mean() > 0.9)
    except Exception:
        _ok = False
    if _ok:
        _df["t_utc"] = pd.to_datetime(_df["t_utc"])
        _corr = _df[["departures", "arrivals"]].corrwith(_df["gust_kmh"])
        _out.append(check(True, f"{len(_df)} ten-minute slots with data, {int(_df.departures.sum())} departures and "
                                f"{int(_df.arrivals.sum())} arrivals – correlation with the gusts: departures "
                                f"{_corr['departures']:+.2f}, arrivals {_corr['arrivals']:+.2f}; max gust "
                                f"{_df.gust_kmh.max():.0f} km/h, rain {_df.precip_mm.sum():.1f} mm", ""))
        _out_win = _df[(_df.t_utc >= "2026-10-20 12:15") & (_df.t_utc < "2026-10-20 13:00")]
        _day = _df[(_df.t_utc >= "2026-10-20 08:00") & (_df.t_utc < "2026-10-20 12:00")]
        if len(_out_win) >= 3 and len(_day) >= 10:
            _out.append(mo.md(
                f"**Counter-example – the Skyguide outage on Tuesday afternoon, 20.10.:** a fault in the system that "
                f"builds the controllers' radar picture; for a while no aircraft took off or landed at Zurich "
                f"([20 Minuten](https://www.20min.ch/story/schweiz-technische-stoerung-bei-skyguide-luftraum-"
                f"kapazitaeten-reduziert-103652678) – the article gives no times; the window 14:15–15:00 Swiss time "
                f"is where the departures drop in *our* data). {_out_win.departures.mean():.1f} departures per 10 min "
                f"against {_day.departures.mean():.1f} in the morning – with a wind of {_out_win.wind_kmh.mean():.0f} "
                f"km/h (morning {_day.wind_kmh.mean():.0f}) and {_out_win.precip_mm.sum():.1f} mm rain. The drop is "
                f"the outage, not weather."))
        _long = _df.melt(id_vars="t_utc", value_vars=["departures", "arrivals"], var_name="movement",
                         value_name="aircraft")
        _fig = px.bar(_long, x="t_utc", y="aircraft", color="movement", height=340, barmode="group",
                      labels={"t_utc": "UTC", "aircraft": "aircraft per 10 min"})
        _fig.add_scatter(x=_df.t_utc, y=_df.gust_kmh, name="gust km/h", mode="lines", line_color="black")
        _fig.add_vrect(x0="2026-10-20 12:15", x1="2026-10-20 13:00", fillcolor="red", opacity=0.1, line_width=0,
                       annotation_text="Skyguide outage")
        _out.append(_fig)
    else:
        _out.append(check(False, "", f"Expected the columns {sorted(_cols)}: one row per 10 minutes with >= 5 "
                                     "snapshots (06:00–22:59 Swiss time), departures / arrivals at LSZH and the "
                                     "station's weather."))
    mo.vstack(_out)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 13.3 – Station or METAR: do they agree?

    Pilots do not read MeteoSwiss CSV files – they get the **METAR**, a coded airport weather report. LSZH issues one
    every 30 minutes, at :20 and :50 (all 336 reports of 13.–20.10.2026 in aviationweather.gov), for example
    `METAR LSZH 201350Z 04008KT 9999 FEW016 13/09 Q1010`: wind from 040° (true) at 8 knots, visibility 10 km or
    more, few clouds (1–2 oktas) with the base at 1,600 ft, 13 °C, dew point 9 °C, QNH 1010 hPa. The wind is the mean
    of the **10 minutes before** the report, in steps of 10° and 1 kt
    ([WMO-No. 782, 2025](https://wmo.int/media/news/aviation-news-2025-08-28-wmo-publishes-update-wmo-no-782-aerodrome-reports-and-forecasts-users),
    sections 2, 3, 6;
    [WMO definition](https://codes.wmo.int/common/quantity-kind/aerodromeMeanWindDirection)). The app's `silver.metar` keeps only
    the latest report; the cell below loads the **last 72 hours** for LSZH from aviationweather.gov and parses them
    with the app's `enrichment/metar.py` into `weather.metar_lszh`.

    Join the METARs (`report_type = 'METAR'`) with the station on the **same time** (both stamp the end of the
    10 minutes) and store in `metar_vs_station`:
    `observed_at`, `metar_temp_c`, `station_temp_c`, `metar_wind_kmh`, `station_wind_kmh`, `metar_dir_deg`,
    `station_dir_deg` and **`dir_diff_deg`** – the difference of the wind directions between −180 and +180.

    /// details | Hint
    Directions are circular: 350° and 10° are only 20° apart, not 340°.
    `((a - b + 540) % 360) - 180` folds every difference into −180 … +180. A METAR with variable wind (`VRB`) has
    no direction (NULL) – that is fine.
    ///
    """)
    return


@app.cell
def _(con, fl, metar, pd, urlencode):
    import json

    _url = metar.METAR_URL + "?" + urlencode({"ids": "LSZH", "format": "json", "hours": 72})
    _records = json.loads(fl.cached(_url, "metar_lszh_72h.json", 30 / 1440).read_text() or "[]")
    metars = pd.concat([metar.parse_metars([_r]) for _r in _records] or [pd.DataFrame(columns=metar.COLUMNS)],
                       ignore_index=True)                # parse_metars keeps the latest per station -> one at a time
    metars["observed_at"] = pd.to_datetime(metars["observed_at"], utc=True).dt.tz_localize(None)
    con.execute("CREATE OR REPLACE TABLE weather.metar_lszh AS SELECT * FROM metars")
    metars[["observed_at", "report_type", "wind_dir_deg", "wind_speed_kmh", "temperature_c", "qnh_hpa",
            "visibility_m", "raw_text"]].head(5)
    return


@app.cell
def _(con, kloten_rows, mo):
    kloten_rows                                          # rerun after 13.1
    # TODO 13.3: metar_vs_station = METAR LSZH next to the station at the same time, with dir_diff_deg (-180..180)
    metar_vs_station = mo.sql("SELECT TIMESTAMP '2026-10-20 12:20' AS observed_at, 0.0 AS metar_temp_c, 0.0 AS station_temp_c, 0.0 AS metar_wind_kmh, 0.0 AS station_wind_kmh, 0 AS metar_dir_deg, 0 AS station_dir_deg, 999 AS dir_diff_deg", engine=con)
    return (metar_vs_station,)


@app.cell(hide_code=True)
def _(check, metar_vs_station):
    _cols = {"observed_at", "metar_temp_c", "station_temp_c", "metar_wind_kmh", "station_wind_kmh", "metar_dir_deg",
             "station_dir_deg", "dir_diff_deg"}
    try:
        _m = metar_vs_station
        _d = _m["dir_diff_deg"].dropna()
        _dt = (_m.metar_temp_c - _m.station_temp_c).abs().mean()
        _ok = (_cols <= set(_m.columns) and len(_m) >= 20 and len(_d) > 0 and _d.between(-180, 180).all()
               and _dt < 2)
        _msg = (f"{len(_m)} METARs matched – temperature differs by {_dt:.1f} °C on average, wind speed by "
                f"{(_m.metar_wind_kmh - _m.station_wind_kmh).mean():+.1f} km/h (METAR in whole knots), wind "
                f"direction by {_d.abs().median():.0f}° (median; METAR in steps of 10°). Both are 10-minute means "
                f"at the airport – they should roughly agree (we do not know whether the METAR uses the same "
                f"anemometer as the MeteoSwiss station)." if _ok else "")
    except Exception:
        _ok, _msg = False, ""
    check(_ok, _msg, f"Expected METAR and station side by side ({sorted(_cols)}), at least 20 matched reports and "
                     "dir_diff_deg between -180 and 180.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### And the model? Open-Meteo at the same place

    Notebook 09 used [Open-Meteo](https://open-meteo.com) – a **weather model** on a grid, not a measurement. The cell
    below loads its hourly 2 m temperature and 10 m wind at the station's position for your days and puts all three
    sources side by side (full hours only, where all three exist).
    """)
    return


@app.cell
def _(con, fl, kloten_rows, metar_vs_station, mo, pd, px, span, urlencode):
    import json as js

    kloten_rows
    _q = {"latitude": 47.480, "longitude": 8.536, "hourly": "temperature_2m,wind_speed_10m,wind_direction_10m",
          "start_date": f"{span[0]:%Y-%m-%d}", "end_date": f"{span[1]:%Y-%m-%d}", "timezone": "GMT"}
    try:
        _p = fl.cached("https://api.open-meteo.com/v1/forecast?" + urlencode(_q),
                        f"openmeteo_klo_{_q['start_date']}_{_q['end_date']}.json", 1 / 24)
        _h = js.loads(_p.read_text())["hourly"]
        openmeteo = pd.DataFrame({"time_utc": pd.to_datetime(_h["time"]), "model_temp_c": _h["temperature_2m"],
                                  "model_wind_kmh": _h["wind_speed_10m"], "model_dir_deg": _h["wind_direction_10m"]})
        _three = con.execute("""
            SELECT o.time_utc, w.temp_c AS station, o.model_temp_c AS model, m.metar_temp_c AS metar
            FROM openmeteo o JOIN weather.kloten_10min w USING (time_utc)
            LEFT JOIN metar_vs_station m ON m.observed_at = o.time_utc - INTERVAL 10 MINUTE   -- METAR at :50
            ORDER BY 1""").df()
        _err = (_three.model - _three.station).abs().mean()
        _out = mo.vstack([
            mo.md(f"Open-Meteo vs. station: **{_err:.1f} °C** mean absolute difference over {len(_three)} hours "
                  f"(METAR vs. station above: "
                  f"{(metar_vs_station.metar_temp_c - metar_vs_station.station_temp_c).abs().mean():.1f} °C). "
                  f"A model gives one value per grid cell – 1 to 11 km wide, depending on the model Open-Meteo "
                  f"picks ([model list](https://open-meteo.com/en/docs))."),
            px.line(_three.melt(id_vars="time_utc", var_name="source", value_name="temp_c"), x="time_utc",
                    y="temp_c", color="source", height=300, labels={"time_utc": "UTC", "temp_c": "°C"})])
    except Exception as _e:
        openmeteo = pd.DataFrame()
        _out = mo.md(f"*Open-Meteo not reachable ({type(_e).__name__}) – skip this comparison.*")
    _out
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 13.4 – Which way do they take off?

    Aircraft take off **into the wind** whenever possible
    ([Flughafen Zürich](https://www.flughafen-zuerich.ch/de/unternehmen/verantwortung/laerm-und-schallschutz/flugbetrieb/betriebskonzepte)):
    a headwind lets the aircraft reach lift-off speed at a lower ground speed, so the take-off run is shorter
    ([FAA, Pilot's Handbook of Aeronautical Knowledge, ch. 11](https://www.faa.gov/sites/faa.gov/files/regulations_policies/handbooks_manuals/aviation/phak/13_phak_ch11.pdf)).
    Zurich has three runways, each usable in both directions: **10/28** (true heading ≈ 96° / 276°), **16/34**
    (≈ 155° / 335°) and **14/32** (≈ 137° / 317°) – computed from the threshold coordinates in
    [OurAirports](https://ourairports.com/airports/LSZH/runways.html). The number is the **magnetic** direction in tens
    of degrees ([SKYbrary](https://skybrary.aero/articles/runway-designators)); with a magnetic declination of
    ≈ 3.7° E at Zurich today ([NOAA, WMM-2025](https://www.ngdc.noaa.gov/geomag/calculators/magcalc.shtml)) it is only
    a rough guide to the true heading. Wind from the north-east to east (the **bise**,
    [MeteoSwiss](https://www.meteoswiss.admin.ch/home/weather/wetterbegriffe/wind.html)) blows in the face of departures
    to the east and north – and in the back of departures to the west. Zurich then lets some aircraft take off on
    **10** (to the east); with a strong westerly wind, take-off is on 32 and landing on 28
    ([Flughafen Zürich](https://www.flughafen-zuerich.ch/de/unternehmen/verantwortung/laerm-und-schallschutz/flugbetrieb/betriebskonzepte)).

    Build `departures` with one row per departure: the **first** position of each aircraft within **8 km** of LSZH,
    **below 4,000 ft**, `flight_phase = 'climbing'` (per aircraft and hour – the same aircraft can leave twice a day)
    with `icao24`, `callsign`, `snapshot_time`, `heading`, the station's `wind_dir_deg` and `wind_kmh` of that
    10 minutes, and

    - `headwind_kmh` = `wind_kmh * cos(wind_dir_deg − heading)` (positive = headwind, negative = tailwind)
    - `runway` = `'16'` (heading 120–200), `'28'` (250–300), `'32'` (300–340), else `'other'`

    Our `heading` is OpenSky's `true_track` – the direction over the ground in degrees true, like the station's wind
    direction. The bins are rough: `'32'` also catches departures on 34 (335°), `'16'` those on 14 (137°), and
    departures on 10 (bise) end up in `'other'`.

    /// details | Hint
    `QUALIFY ROW_NUMBER() OVER (PARTITION BY icao24, date_trunc('hour', snapshot_time) ORDER BY snapshot_time) = 1`
    keeps the first position. The wind direction is where the wind comes **from** – so a heading equal to the
    wind direction means flying straight into it: `COS(RADIANS(wind_dir_deg - heading)) = 1`. The station's
    10 minutes for a position: `time_bucket(INTERVAL 10 MINUTE, snapshot_time) + INTERVAL 10 MINUTE` (end stamp).
    ///
    """)
    return


@app.cell
def _(con, kloten_rows, mo):
    kloten_rows                                          # rerun after 13.1
    # TODO 13.4: departures = first climbing position per aircraft and hour near LSZH, with wind, headwind_kmh, runway
    departures = mo.sql("SELECT 'abc123' AS icao24, 'TODO' AS callsign, TIMESTAMP '2026-10-20 12:00' AS snapshot_time, 0.0 AS heading, 0.0 AS wind_dir_deg, 0.0 AS wind_kmh, 99.0 AS headwind_kmh, 'other' AS runway", engine=con)
    return (departures,)


@app.cell(hide_code=True)
def _(check, departures, mo, pd, px):
    import numpy as np

    _cols = {"icao24", "snapshot_time", "heading", "wind_dir_deg", "wind_kmh", "headwind_kmh", "runway"}
    _out = []
    try:
        _d = departures.dropna(subset=["heading", "wind_dir_deg", "wind_kmh"]).copy()
        _expect = _d.wind_kmh * np.cos(np.radians(_d.wind_dir_deg - _d.heading))
        _ok = (_cols <= set(_d.columns) and len(_d) > 30 and (_d.headwind_kmh - _expect).abs().max() < 1
               and set(_d.runway) <= {"16", "28", "32", "other"} and _d.runway.isin(["16", "28", "32"]).mean() > 0.8)
    except Exception:
        _ok = False
    if _ok:
        _top = _d.runway.value_counts()
        _tail = (_d.headwind_kmh < 0).mean() * 100
        _out.append(check(True, f"{len(_d)} departures – runway {_top.index[0]} {_top.iloc[0]}×, "
                                f"{', '.join(f'{k} {v}×' for k, v in _top.iloc[1:].items())}; "
                                f"{_tail:.0f} % took off with a tailwind component "
                                f"(mean {_d.headwind_kmh.mean():+.1f} km/h)", ""))
        _d["hour_local"] = (pd.to_datetime(_d.snapshot_time).dt.tz_localize("UTC").dt.tz_convert("Europe/Zurich")
                            .dt.tz_localize(None).dt.floor("h"))
        _per_hour = _d.groupby(["hour_local", "runway"]).size().reset_index(name="departures")
        _wind = _d.groupby("hour_local").agg(east_share=("wind_dir_deg", lambda s: ((s >= 0) & (s <= 120)).mean()))
        _fig = px.bar(_per_hour, x="hour_local", y="departures", color="runway", height=340,
                      labels={"hour_local": "hour (Swiss time)", "departures": "departures per hour"})
        _fig.add_scatter(x=_wind.index, y=_wind.east_share * _per_hour.departures.max(), mode="lines+markers",
                         name="share of north-easterly wind (scaled)", line_color="black")
        _out += [_fig, mo.md("The runways change at **fixed local times**: on weekdays take-offs on 28 or 16 from "
                             "07:00 to 21:00, on 32/34 in the evening and on 32/34 or 28 before 07:00 (other hours on "
                             "weekends) – [Flughafen Zürich, operating concepts](https://www.flughafen-zuerich.ch/de/"
                             "unternehmen/verantwortung/laerm-und-schallschutz/flugbetrieb/betriebskonzepte). "
                             "So a tailwind component is often accepted. ICAO's rule of thumb for choosing a runway "
                             "for noise reasons: tailwind up to 5 kt (≈ 9 km/h) including gusts, crosswind up to "
                             "15 kt (PANS-ATM, Doc 4444, as cited by [IFALPA](https://ifalpa.org/media/4306/"
                             "25pos28-tailwind-operations.pdf)); we did not find Zurich's own limit. Bise and "
                             "strong westerly wind change the concept (see the text above).")]
    else:
        _out.append(check(False, "", f"Expected one row per departure with {sorted(_cols)}, headwind_kmh = "
                                     "wind_kmh · cos(wind_dir_deg − heading) and runway 16 / 28 / 32 / other."))
    mo.vstack(_out)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Think about it

    - Traffic hardly reacts to the weather in your data – is that because the weather was calm? Which events would
      you expect to show up: thunderstorms in summer, snow (de-icing!), fog (low visibility – that is in the METAR,
      `visibility_m`, not in the station file), strong bise?
    - During the Skyguide outage the departures in our data dropped to almost zero – while the METARs reported
      visibility of 10 km or more, no rain and a wind of 8–9 kt (`METAR LSZH 201220Z 04009KT 9999 FEW016 ...`).
      What else can empty the sky without any weather? (strikes, slot restrictions, the night curfew ...)
    - The METAR rounds the wind to 10° and whole knots; the station gives 10-minute means in 1° and 0.1 km/h (see
      the metadata file). When does that rounding matter for a pilot – and when for your analysis?
    - Compare your runway counts with the app's `gold.runway_in_use` (`enrichment/runway_in_use.py`), which matches
      the tracks to the real runway ends and computes head- and crosswind from the METAR.

    Data: MeteoSwiss open data (CC BY 4.0, source: MeteoSwiss), METAR from aviationweather.gov (US National Weather
    Service, [public domain](https://www.weather.gov/disclaimer)), Open-Meteo
    ([CC BY 4.0](https://open-meteo.com/en/licence)), runways from OurAirports.

    **Next:** [14 – Our sample vs. the official numbers](14_eurocontrol.py)
    """)
    return


if __name__ == "__main__":
    app.run()
