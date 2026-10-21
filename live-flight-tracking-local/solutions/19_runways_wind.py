import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium", app_title="19 – Runways and wind")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 19 – Runways and wind: which way does Zurich fly today?

    Zurich has three runways, each usable in two directions: **10/28**, **14/32** and **16/34**. The number is the
    direction in tens of degrees – runway 14 means landing or taking off towards ~140°. By the rule it is the
    **magnetic** direction ([FAA AIM 2-3-3](https://www.faa.gov/air_traffic/publications/atpubs/aim_html/chap2_section_3.html));
    the true directions at Zurich are 96°, 137° and 155° ([OurAirports](https://davidmegginson.github.io/ourairports-data/runways.csv)),
    and with today's declination of +3.7° ([NOAA WMM-2025 calculator](https://www.ngdc.noaa.gov/geomag/calculators/magcalc.shtml))
    the magnetic ones are about 92°, 133° and 151° – the names are only approximate.

    Which direction is used is not only a question of wind. Zurich has **operating concepts**, set in the operating
    regulation and shaped by noise rules and German restrictions on approaches over southern Germany
    ([Flughafen Zürich: Betriebskonzepte](https://www.flughafen-zuerich.ch/de/unternehmen/verantwortung/laerm-und-schallschutz/flugbetrieb/betriebskonzepte);
    the restrictions are a German ordinance, the 213th DVO, after the treaty failed in 2003 –
    [Federal Supreme Court 1A.246/2003](https://servat.unibe.ch/dfr/bger/2004/040331_1A-246-2003.html)):

    | concept | landings | departures | when (Mon–Fri; Sat, Sun and Baden-Württemberg holidays) |
    |---|---|---|---|
    | **north** (normal) | 14 or 16 (from the north) | 28, 16 | 07:00–21:00; 09:00–20:00 |
    | **east** | 28 (from the east) | 32, 34 | 21:00–23:30; 20:00–23:30 – with west wind also during the day |
    | **south** | 34 (from the south) | 32, 34, 28 | 06:00–07:00; 06:00–09:00 – before 07:00 / 09:00 aircraft may cross Germany only at FL120 or above |
    | **bise** | 14 or 16 (evenings: 34) | mainly 10, heavy long-haul aircraft 16 | north-east wind ("Bise") |

    Sources: the Flughafen Zürich page above; departures in the bise concept:
    [environmental report of the operating-regulation change 2014/2017](https://www.bazl.admin.ch/dam/de/sd-web/8QxLSsaLDvmJ/Beilage%203%20UVB.pdf),
    section 2.4. The south approach to 34 follows the extended centre line over Opfikon, Wallisellen, Dübendorf and
    the eastern edge of the city of Zurich (our computation with the runway threshold and the swisstopo communes).

    Aircraft take off and land **into the wind** when they can; tailwind is avoided because it can destabilise
    landings ([Flughafen Zürich: Flugrouten](https://www.flughafen-zuerich.ch/de/unternehmen/verantwortung/laerm-und-schallschutz/flugbetrieb/flugrouten)),
    and it makes the landing distance longer ([FAA Pilot's Handbook, ch. 11](https://www.faa.gov/sites/faa.gov/files/13_phak_ch11.pdf)).
    So who wins, the wind or the concept? You find out from the positions and the METAR.
    """)
    return


@app.cell
def _(mo):
    import json
    import sys

    sys.path.insert(0, str(mo.notebook_dir().parent))
    import numpy as np
    import pandas as pd
    import plotly.express as px

    import flightlab as fl
    from enrichment import jobs, metar, runway_in_use

    def check(ok, good, bad):
        return mo.callout(mo.md(("✅ " + good) if ok else ("❌ " + bad)), kind="success" if ok else "danger")

    con = fl.snapshot()
    fl.ensure(con, "runways", "metar")                     # tables the app computes – here if you only ran collector.py
    if not con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'runways'").fetchone()[0]:
        jobs.run_job(con, "runways", log=lambda *_: None)          # OurAirports runways.csv, public domain
    runways_zrh = con.execute("""SELECT runway_end, runway, heading_true_deg, threshold_lat, threshold_lon, length_ft
                                 FROM silver.runways WHERE airport_icao = 'LSZH' ORDER BY runway_end""").df()
    runways_zrh
    return check, con, fl, json, metar, np, pd, px, runway_in_use, runways_zrh


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 19.1 – Which runway did each aircraft use?

    Near the airport, an aircraft flies along the runway: its **heading** (track over ground) is the runway
    direction. Take every position within **8 km** of LSZH (47.4581, 8.5481), **below 3,000 ft above the airport**
    (elevation 1,417 ft) and `climbing` (departure) or `descending` (arrival). Match its heading with the nearest
    runway direction **100, 280, 140, 320, 160, 340** (the runway numbers × 10 – a simplification, the true
    directions in `runways_zrh` differ by up to 5°) – only if the difference is at most **±20°**. Then one row per
    aircraft, movement and local hour with the most frequent runway end (`MODE`).

    Store it in `movements` with the columns `hour_local`, `icao24`, `callsign`, `movement` (`arrival` /
    `departure`) and `runway_end` (`'10'`, `'28'`, ...).

    /// details | Hint
    The angle difference across north: `ABS((heading - nominal + 540) % 360 - 180)`. All six candidates with a
    `CROSS JOIN` of a `VALUES` table, the nearest one with
    `QUALIFY ROW_NUMBER() OVER (PARTITION BY icao24, snapshot_time ORDER BY diff) = 1`.
    ///
    """)
    return


@app.cell
def _(con, mo):
    # >>> TODO 19.1: movements = hour_local, icao24, callsign, movement, runway_end (nearest of the six directions, ±20°)
    # ?? movements = mo.sql("SELECT TIMESTAMP '2026-10-20 12:00' AS hour_local, 'todo' AS icao24, 'TODO' AS callsign, 'arrival' AS movement, '00' AS runway_end", engine=con)
    movements = mo.sql(
        """
        WITH rw(runway_end, nominal) AS (VALUES ('10', 100), ('28', 280), ('14', 140), ('32', 320), ('16', 160), ('34', 340)),
        p AS (
            SELECT snapshot_time, icao24, callsign, heading,
                   date_trunc('hour', timezone('Europe/Zurich', snapshot_time::TIMESTAMPTZ)) AS hour_local,
                   CASE WHEN flight_phase = 'descending' THEN 'arrival' ELSE 'departure' END AS movement
            FROM silver.flights
            WHERE region = 'switzerland' AND flight_phase IN ('climbing', 'descending') AND heading IS NOT NULL
              AND altitude_ft < 1417 + 3000
              AND 2 * 6371 * ASIN(SQRT(POWER(SIN(RADIANS(latitude - 47.4581) / 2), 2)
                  + COS(RADIANS(47.4581)) * COS(RADIANS(latitude)) * POWER(SIN(RADIANS(longitude - 8.5481) / 2), 2))) <= 8),
        m AS (
            SELECT p.*, rw.runway_end, ABS((p.heading - rw.nominal + 540) % 360 - 180) AS diff
            FROM p CROSS JOIN rw
            QUALIFY ROW_NUMBER() OVER (PARTITION BY p.icao24, p.snapshot_time ORDER BY diff) = 1)
        SELECT hour_local, icao24, ANY_VALUE(callsign) AS callsign, movement, MODE(runway_end) AS runway_end,
               COUNT(*) AS positions
        FROM m
        WHERE diff <= 20
        GROUP BY hour_local, icao24, movement
        ORDER BY hour_local, movement
        """,
        engine=con,
    )
    # <<< TODO 19.1
    return (movements,)


@app.cell(hide_code=True)
def _(check, movements):
    try:
        _ok_cols = {"hour_local", "icao24", "movement", "runway_end"} <= set(movements.columns)
        _arr = movements[movements["movement"] == "arrival"] if _ok_cols else movements.iloc[0:0]
        _ok = (_ok_cols and len(movements) > 50 and set(movements["runway_end"]) <= {"10", "28", "14", "32", "16", "34"}
               and set(movements["movement"]) == {"arrival", "departure"})
        _top = _arr["runway_end"].value_counts() if _ok else None
        _msg = (f"{len(movements)} movements – landings per runway: "
                + ", ".join(f"{k}: {v}" for k, v in _top.items()) if _ok else "")
    except Exception as _e:
        _ok, _msg = False, ""
    check(_ok, _msg, "Expected one row per aircraft, movement and hour with runway_end in 10, 28, 14, 32, 16, 34 "
                     "and both movements (arrival / departure).")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 19.2 – Runways per hour and the operating concept

    Per **local hour**: the number of arrivals, the **landing runway** (most frequent `runway_end` of the
    arrivals), the number of departures and the **departure mix** as text (e.g. `28: 25, 16: 3`, most first). Then
    name the concept from the table at the top:

    - landing 28 → `east`, landing 34 → `south` (a simplification: with bise in the evening Zurich also lands on 34),
    - landing 14 or 16 → `bise` if the most frequent departure runway is 10, otherwise `north`,
    - no arrivals → `none`.

    Store it in `per_hour` with the columns `hour_local`, `arrivals`, `landing_runway`, `departures`,
    `departure_mix`, `concept`.

    /// details | Hint
    `movements[movements.movement == "arrival"].groupby("hour_local")["runway_end"].agg(lambda s: s.mode().iloc[0])`
    for the landing runway; `", ".join(f"{k}: {v}" for k, v in s.value_counts().items())` for the mix. A small
    function `concept(row)` with `if/elif` and `per_hour.apply(concept, axis=1)`.
    ///
    """)
    return


@app.cell
def _(movements, pd):
    # >>> TODO 19.2: per_hour = hour_local, arrivals, landing_runway, departures, departure_mix, concept
    # ?? per_hour = pd.DataFrame(columns=["hour_local", "arrivals", "landing_runway", "departures", "departure_mix", "concept"])
    _arr = movements[movements["movement"] == "arrival"].groupby("hour_local")["runway_end"]
    _dep = movements[movements["movement"] == "departure"].groupby("hour_local")["runway_end"]
    per_hour = pd.DataFrame({
        "arrivals": _arr.size(), "landing_runway": _arr.agg(lambda s: s.mode().iloc[0]),
        "departures": _dep.size(), "departure_mix": _dep.agg(lambda s: ", ".join(f"{k}: {v}" for k, v in s.value_counts().items())),
        "top_departure": _dep.agg(lambda s: s.mode().iloc[0])}).rename_axis("hour_local").reset_index()
    per_hour[["arrivals", "departures"]] = per_hour[["arrivals", "departures"]].fillna(0).astype(int)

    def _concept(r):
        if pd.isna(r["landing_runway"]):
            return "none"
        if r["landing_runway"] == "28":
            return "east"
        if r["landing_runway"] == "34":
            return "south"
        if r["landing_runway"] in ("14", "16"):
            return "bise" if r["top_departure"] == "10" else "north"
        return "other"

    per_hour["concept"] = per_hour.apply(_concept, axis=1)
    per_hour = per_hour.drop(columns="top_departure").sort_values("hour_local").reset_index(drop=True)
    # <<< TODO 19.2
    per_hour
    return (per_hour,)


@app.cell(hide_code=True)
def _(check, mo, per_hour, px):
    try:
        _ok = ({"hour_local", "arrivals", "landing_runway", "departures", "departure_mix", "concept"} <= set(per_hour.columns)
               and len(per_hour) >= 5 and "north" in set(per_hour["concept"])
               and set(per_hour["concept"]) <= {"north", "east", "south", "bise", "none", "other"})
        _c = per_hour.groupby("concept")["hour_local"].agg(["size", "min", "max"]) if _ok else None
        _txt = "; ".join(f"**{k}** {r['size']} h (first {r['min']:%d.%m. %H:00}, last {r['max']:%d.%m. %H:00})"
                         for k, r in _c.iterrows()) if _ok else ""
        _out = [check(_ok, f"Concepts in your data: {_txt}",
                      "Expected per hour the landing runway, the departure mix and a concept "
                      "(north / east / south / bise / none).")]
        if _ok:
            _out.append(px.bar(per_hour, x="hour_local", y="arrivals", color="concept", height=300,
                               hover_data=["landing_runway", "departure_mix"],
                               color_discrete_map={"north": "#4c78a8", "east": "#f58518", "south": "#e45756",
                                                   "bise": "#72b7b2", "none": "#bab0ac", "other": "#bab0ac"},
                               labels={"hour_local": "Swiss time", "arrivals": "landings per hour"}))
    except Exception as _e:
        _out = [check(False, "", f"per_hour could not be evaluated: {type(_e).__name__}: {_e}")]
    mo.vstack(_out)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## The wind: METAR history

    `silver.metar` only holds the **latest** report per airport (the app refreshes it every 15 minutes). For the
    past we ask the [aviationweather.gov Data API](https://aviationweather.gov/data/api/) (Aviation Weather Center
    of the US National Weather Service, no key needed) for the reports of the last **168 hours** (one week). The API
    keeps up to 30 days, but returns at most 400 entries per request and asks for at most 100 requests per minute
    and a custom User-Agent (API page, "Using the API"). At Zurich (two reports per hour) 400 entries are about
    8 days. One request, cached in `cache/` for 30 minutes; `enrichment.metar.parse_metars` turns each report into
    a row.

    The METAR wind direction is where the wind **comes from**, relative to **true** north
    ([FAA Pilot's Handbook, ch. 13](https://www.faa.gov/sites/faa.gov/files/15_phak_ch13.pdf)) – like our true runway
    headings (`silver.runways.heading_true_deg`), not like the magnetic runway names.
    """)
    return


@app.cell
def _(con, fl, json, metar, pd):
    try:
        _path = fl.cached("https://aviationweather.gov/api/data/metar?ids=LSZH&format=json&hours=168",
                           "metar_LSZH_168h.json", max_age_days=1 / 48)
        _records = json.loads(_path.read_text() or "[]")
        wx = pd.concat([metar.parse_metars([_r]) for _r in _records], ignore_index=True)
        _source = f"aviationweather.gov, {len(wx)} reports"
    except Exception as _e:                                     # offline: only the latest report of the app
        wx = con.execute("SELECT * FROM silver.metar WHERE station = 'LSZH'").df()
        _source = f"only silver.metar ({type(_e).__name__})"
    wx["observed_at"] = pd.to_datetime(wx["observed_at"], utc=True)
    wx["hour_local"] = wx["observed_at"].dt.tz_convert("Europe/Zurich").dt.tz_localize(None).dt.floor("h")
    wx = wx.sort_values("observed_at").reset_index(drop=True)
    wx[["observed_at", "hour_local", "wind_dir_deg", "wind_speed_kt", "wind_gust_kt", "wind_variable", "raw_text"]].tail(8)
    return (wx,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 19.3 – Headwind on the landing runway

    With the angle `a = wind direction − runway heading` (both true, in degrees):

    - **headwind** = `speed × cos(a)` – negative means **tailwind**,
    - **crosswind** = `speed × sin(a)` – positive from the right.

    Take the **last METAR of every local hour** from `wx`, join it with `per_hour` (landing runway) and the true
    heading of that runway end from `runways_zrh`, and compute `headwind_kt` and `crosswind_kt`. Store it in `wind`
    with the columns `hour_local`, `concept`, `landing_runway`, `wind_dir_deg`, `wind_speed_kt`, `headwind_kt`,
    `crosswind_kt`. Variable wind (`VRB`, no direction) gives NaN – that is fine.

    /// details | Hint
    `wx.groupby("hour_local").last()` (or `drop_duplicates("hour_local", keep="last")`), two `merge`s,
    `np.radians(...)`. Cast `wind_dir_deg` / `wind_speed_kt` with `.astype(float)` – they are nullable integers.
    ///
    """)
    return


@app.cell
def _(np, per_hour, runways_zrh, wx):
    # >>> TODO 19.3: wind = per hour: concept, landing_runway, wind_dir_deg, wind_speed_kt, headwind_kt, crosswind_kt
    # ?? wind = per_hour.assign(wind_dir_deg=0.0, wind_speed_kt=0.0, headwind_kt=0.0, crosswind_kt=0.0)
    _w = wx.drop_duplicates("hour_local", keep="last")[["hour_local", "wind_dir_deg", "wind_speed_kt", "wind_gust_kt"]]
    wind = (per_hour[per_hour["concept"] != "none"][["hour_local", "concept", "landing_runway"]]
            .merge(_w, on="hour_local", how="left")
            .merge(runways_zrh[["runway_end", "heading_true_deg"]], left_on="landing_runway", right_on="runway_end",
                   how="left").drop(columns="runway_end"))
    _a = np.radians(wind["wind_dir_deg"].astype(float) - wind["heading_true_deg"])
    wind["headwind_kt"] = (wind["wind_speed_kt"].astype(float) * np.cos(_a)).round(1)
    wind["crosswind_kt"] = (wind["wind_speed_kt"].astype(float) * np.sin(_a)).round(1)
    # <<< TODO 19.3
    wind
    return (wind,)


@app.cell(hide_code=True)
def _(check, mo, np, px, runway_in_use, runways_zrh, wind):
    try:
        _w = wind.dropna(subset=["headwind_kt"])
        _hdg = _w["landing_runway"].map(runways_zrh.set_index("runway_end")["heading_true_deg"])
        _hw, _cw = runway_in_use.wind_components(_w["wind_dir_deg"].astype(float), _w["wind_speed_kt"].astype(float), _hdg)
        _ok = (len(_w) >= 3 and np.allclose(_w["headwind_kt"], _hw, atol=0.2) and np.allclose(_w["crosswind_kt"], _cw, atol=0.2))
        _tail = _w[_w["headwind_kt"] < -0.5]
        _out = [check(_ok, f"{len(_w)} hours with a wind direction – the landing runway had a **tailwind** in "
                           f"{len(_tail)} of them (max {(-_w['headwind_kt']).max():.0f} kt), a headwind in "
                           f"{int((_w['headwind_kt'] > 0.5).sum())}" if _ok else "",
                      "Expected headwind = speed × cos(wind direction − true runway heading) and crosswind = "
                      "speed × sin(...) per hour (same numbers as `runway_in_use.wind_components`).")]
        if _ok:
            _out.append(px.bar(_w, x="hour_local", y="headwind_kt", color="concept", height=280,
                               hover_data=["landing_runway", "wind_dir_deg", "wind_speed_kt", "crosswind_kt"],
                               labels={"hour_local": "Swiss time", "headwind_kt": "headwind on the landing runway (kt)"}))
    except Exception as _e:
        _out = [check(False, "", f"wind could not be evaluated: {type(_e).__name__}: {_e}")]
    mo.vstack(_out)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 19.4 – Compare with the finished module

    `enrichment/runway_in_use.py` does the same more carefully: true runway headings, **distance from the extended
    centerline** (a cone of ~4.6°) instead of only the heading – that is how it separates 14 (137°) from 16 (155°),
    which are only 18° apart – and the threshold position (arrivals *before* it, departures *after* it). In the app
    it only looks at the last 15 minutes (`LOOKBACK`); below we call `match_runway_movements` on all positions
    near Zurich and take the most frequent landing runway per hour (`module_hour`).

    Merge `per_hour` with `module_hour` on `hour_local` (only hours where both have a landing runway) and add a
    column `same` (your landing runway equals the module's). Store it in `agreement`.
    """)
    return


@app.cell
def _(con, pd, runway_in_use):
    _f = con.execute("""
        SELECT snapshot_time, icao24, callsign, latitude, longitude, on_ground, altitude_ft, heading, vertical_rate
        FROM silver.flights
        WHERE region = 'switzerland' AND NOT on_ground AND altitude_ft < 12000
          AND latitude BETWEEN 47.2 AND 47.75 AND longitude BETWEEN 8.15 AND 8.95""").df()
    _ap = con.execute("SELECT * FROM silver.airports WHERE airport_icao = 'LSZH'").df()
    _rw = con.execute("SELECT * FROM silver.runways WHERE airport_icao = 'LSZH'").df()
    _m = runway_in_use.match_runway_movements(_f, _ap, _rw, None)
    if len(_m):
        _m["hour_local"] = (pd.to_datetime(_m["snapshot_time"]).dt.tz_localize("UTC").dt.tz_convert("Europe/Zurich")
                            .dt.tz_localize(None).dt.floor("h"))
        # one runway per aircraft and hour: the match closest to the threshold (as the module does)
        _best = _m.sort_values("along_km", key=abs).drop_duplicates(["icao24", "movement", "hour_local"])
        module_hour = (_best[_best["movement"] == "arrivals"].groupby("hour_local")["runway_end"]
                       .agg(lambda s: s.mode().iloc[0]).rename("module_landing").reset_index())
    else:
        module_hour = pd.DataFrame(columns=["hour_local", "module_landing"])
    module_hour
    return (module_hour,)


@app.cell
def _(module_hour, pd, per_hour):
    # >>> TODO 19.4: agreement = per_hour merged with module_hour (inner) + column same (landing runway equal)
    # ?? agreement = pd.DataFrame(columns=["hour_local", "landing_runway", "module_landing", "same"])
    agreement = per_hour.merge(module_hour, on="hour_local").dropna(subset=["landing_runway", "module_landing"])
    agreement["same"] = agreement["landing_runway"] == agreement["module_landing"]
    # <<< TODO 19.4
    agreement[["hour_local", "concept", "landing_runway", "module_landing", "same"]] if len(agreement) else pd.DataFrame()
    return (agreement,)


@app.cell(hide_code=True)
def _(agreement, check):
    try:
        _ok = {"landing_runway", "module_landing", "same"} <= set(agreement.columns) and len(agreement) >= 5
        _share = float(agreement["same"].mean()) if _ok else 0.0
        _diff = agreement[~agreement["same"]] if _ok else agreement
        _ok = _ok and _share >= 0.8
        _msg = (f"same landing runway as the module in {_share:.0%} of {len(agreement)} hours"
                + (" – different: " + ", ".join(f"{r['hour_local']:%d.%m. %H:00} ({r['landing_runway']} vs "
                                                   f"{r['module_landing']})" for _, r in _diff.iterrows())
                   if len(_diff) else "") if _ok else "")
    except Exception as _e:
        _ok, _msg, _share = False, "", 0.0
    check(_ok, _msg, f"Expected the hours of per_hour and module_hour side by side with a column same – and mostly "
                     f"the same runway (now {_share:.0%}).")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Think about it

    - Which concepts did you find, and at which hours did Zurich switch? Compare with the rules: south approach in
      the early morning, east approach in the evening.
    - Count the hours with a **tailwind** on the landing runway. With a north-east wind, landings on 14 get mostly
      crosswind – the problem are the departures on 28, which get a tailwind. Compute the tailwind on 28 per hour:
      at what value did Zurich switch to the bise concept? (The switch follows the ICAO rules on tail- and
      crosswind, says the environmental report above, section 2.4 – which limit is that?)
    - The south approach (34) brings aircraft low over Opfikon, Wallisellen, Dübendorf and the east of Zurich. With
      the runway per movement and an estimate of the people living under the approach path, how would you measure
      *who* is affected by which concept?
    - Where do your simple runway (nearest of six headings) and the module disagree – 14 versus 16? Why is the
      heading alone not enough there?

    **Next:** [20 – Dashboard](20_dashboard.py)
    """)
    return


if __name__ == "__main__":
    app.run()
