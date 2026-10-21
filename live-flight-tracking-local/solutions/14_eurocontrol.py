import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium", app_title="14 – Our sample vs. the official numbers")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 14 – Our sample vs. the official numbers (EUROCONTROL)

    Our data is a **sample**: what the crowdsourced receivers of the OpenSky Network
    ([OpenSky Report 2025](https://arxiv.org/abs/2505.06254)) picked up, every ~35 seconds, inside a box around
    Switzerland. EUROCONTROL counts the **IFR** flights per airport – the columns we use come from its Network
    Manager, a second set from the airport operators
    ([dataset definition](https://ansperformance.eu/reference/dataset/airport-traffic/)). For IFR traffic that is
    close to a **census**. In this notebook you check your sample against the census:

    - how far has traffic at Zurich, Geneva and Basel recovered since 2019?
    - do *your* departures and arrivals at Zurich match the official count?
    - is the order of magnitude of our CO₂ estimate (notebook 08) right?
    - where does Zurich stand among Europe's airports?

    Data: [EUROCONTROL Aviation Intelligence Portal](https://ansperformance.eu/data/) – daily traffic per airport
    (IFR arrivals + departures) and CO₂ per state, as CSV. Source: EUROCONTROL. Free for non-commercial use with
    attribution, no modification without permission ([disclaimer](https://ansperformance.eu/about/disclaimer/)).
    On 20.10.2026 the 2026 traffic file ended on **30.09.2026**; the CO₂ file is updated monthly
    ([CO₂ dataset](https://ansperformance.eu/reference/dataset/emissions/)).
    """)
    return


@app.cell
def _(mo):
    import sys

    sys.path.insert(0, str(mo.notebook_dir().parent))
    import flightlab as fl
    from enrichment import fuel_co2, jobs

    def check(ok, good, bad):
        return mo.callout(mo.md(("✅ " + good) if ok else ("❌ " + bad)), kind="success" if ok else "danger")

    EC = "https://www.eurocontrol.int/performance/data/download/csv/"
    con = fl.snapshot()
    fl.ensure(con, "aircraft_types")                     # tables the app computes – here if you only ran collector.py
    _out = []
    try:
        with mo.status.spinner(title="downloading EUROCONTROL airport traffic 2026 + 2019 (~14 MB, once) ..."):
            _p26 = fl.cached(EC + "airport_traffic_2026.csv", "eurocontrol_airport_traffic_2026.csv", max_age_days=7)
            _p19 = fl.cached(EC + "airport_traffic_2019.csv", "eurocontrol_airport_traffic_2019.csv", max_age_days=365)
            _pco2 = fl.cached(EC + "co2_emmissions_by_state_2026.csv",      # sic: "emmissions" in the official URL
                               "eurocontrol_co2_by_state_2026.csv", max_age_days=7)
        con.execute(f"""CREATE OR REPLACE TABLE apt_traffic AS
            SELECT YEAR, CAST(MONTH_NUM AS INTEGER) AS MONTH_NUM, FLT_DATE, APT_ICAO, APT_NAME, STATE_NAME,
                   FLT_DEP_1, FLT_ARR_1, FLT_TOT_1
            FROM read_csv(['{_p19}', '{_p26}'], union_by_name = true)""")
        con.execute(f"CREATE OR REPLACE TABLE co2_state AS SELECT * FROM read_csv('{_pco2}')")
    except Exception as _e:                    # no internet: empty tables, the checks below show red hints
        con.execute("CREATE OR REPLACE TABLE apt_traffic (YEAR BIGINT, MONTH_NUM INTEGER, FLT_DATE DATE, "
                    "APT_ICAO VARCHAR, APT_NAME VARCHAR, STATE_NAME VARCHAR, FLT_DEP_1 BIGINT, FLT_ARR_1 BIGINT, "
                    "FLT_TOT_1 BIGINT)")
        con.execute("CREATE OR REPLACE TABLE co2_state (FLIGHT_MONTH DATE, YEAR BIGINT, MONTH BIGINT, "
                    "STATE_NAME VARCHAR, STATE_CODE VARCHAR, CO2_QTY_TONNES DOUBLE, TF BIGINT, NOTE BOOLEAN)")
        _out.append(mo.callout(mo.md(f"❌ EUROCONTROL download failed: `{_e}`"), kind="danger"))
    _n, _last = con.execute("SELECT COUNT(*), MAX(FLT_DATE) FROM apt_traffic").fetchone()
    _span = con.execute("SELECT MIN(snapshot_time), MAX(snapshot_time) FROM silver.flights "
                        "WHERE region = 'switzerland'").fetchone()
    _out.append(mo.md(f"`apt_traffic`: **{_n:,} rows** (airport × day, 2019 and 2026, last day {_last}) · "
                      f"your data: {_span[0]:%d.%m. %H:%M} – {_span[1]:%d.%m. %H:%M} UTC"))
    mo.vstack(_out)
    return check, con, fl, fuel_co2, jobs


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 14.1 – Back to 2019?

    `apt_traffic` has one row per airport and day: `FLT_DEP_1` departures, `FLT_ARR_1` arrivals, `FLT_TOT_1` both
    (IFR flights, source: Network Manager). Compare **2026 with 2019**, the last year before Covid, for **LSZH** (Zurich), **LSGG** (Geneva)
    and **LFSB** (Basel-Mulhouse – it lies in France and is listed under France).

    Store in `recovery` one row per airport and month with the columns `airport`, `month`, `flights_2026`,
    `flights_2019` (average movements **per day**) and `pct_of_2019` (= 2026 / 2019 × 100). Only months that 2026
    already has.

    /// details | Hint
    One `GROUP BY APT_ICAO, MONTH_NUM` with conditional averages:
    `AVG(FLT_TOT_1) FILTER (WHERE YEAR = 2026)`. Months without 2026 data: `HAVING flights_2026 IS NOT NULL`.
    ///
    """)
    return


@app.cell
def _(con, mo):
    # >>> TODO 14.1: recovery = airport, month, flights_2026, flights_2019 (per day), pct_of_2019
    # ?? recovery = mo.sql("SELECT 'TODO' AS airport, 1 AS month, 0 AS flights_2026, 0 AS flights_2019, 0 AS pct_of_2019", engine=con)
    recovery = mo.sql(
        """
        SELECT APT_ICAO                                              AS airport,
               MONTH_NUM                                             AS month,
               ROUND(AVG(FLT_TOT_1) FILTER (WHERE YEAR = 2026))      AS flights_2026,
               ROUND(AVG(FLT_TOT_1) FILTER (WHERE YEAR = 2019))      AS flights_2019,
               ROUND(100 * AVG(FLT_TOT_1) FILTER (WHERE YEAR = 2026)
                         / AVG(FLT_TOT_1) FILTER (WHERE YEAR = 2019), 1) AS pct_of_2019
        FROM apt_traffic
        WHERE APT_ICAO IN ('LSZH', 'LSGG', 'LFSB')
        GROUP BY ALL
        HAVING flights_2026 IS NOT NULL
        ORDER BY airport, month
        """,
        engine=con,
    )
    # <<< TODO 14.1
    return (recovery,)


@app.cell(hide_code=True)
def _(check, mo, recovery):
    import plotly.express as px

    _cols = {"airport", "month", "flights_2026", "flights_2019", "pct_of_2019"}
    _ok = (_cols <= set(recovery.columns) and set(recovery["airport"]) == {"LSZH", "LSGG", "LFSB"}
           and len(recovery) >= 9 and recovery["pct_of_2019"].between(30, 200).all())
    _out = []
    if _ok:
        _ytd = (recovery.groupby("airport")[["flights_2026", "flights_2019"]].mean()
                .assign(pct=lambda d: 100 * d.flights_2026 / d.flights_2019)["pct"])
        _out.append(check(True, "2026 so far, in % of 2019: " + ", ".join(f"**{a}** {p:.0f} %" for a, p in _ytd.items()),
                          ""))
        _out.append(px.line(recovery, x="month", y="pct_of_2019", color="airport", markers=True, height=320,
                            labels={"month": "month of 2026", "pct_of_2019": "movements per day, % of 2019"})
                    .add_hline(y=100, line_dash="dot"))
    else:
        _out.append(check(False, "", f"Expected one row per airport (LSZH, LSGG, LFSB) and month with the columns "
                                     f"{sorted(_cols)} – and a plausible pct_of_2019."))
    mo.vstack(_out)
    return (px,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 14.2 – Count the movements at Zurich yourself

    A **movement** is a departure or an arrival. From `silver.flights` (Swiss feed) count per **local hour**
    (`hour_local`, Swiss time) the **different aircraft** that were within **15 km** of LSZH (47.4581, 8.5481 –
    airport reference point from [OurAirports](https://ourairports.com/data/)),
    **in the air** and **below 6,000 ft**:

    - `departures`: `flight_phase = 'climbing'`
    - `arrivals`: `flight_phase = 'descending'`

    Store the result in `ours` (columns `hour_local`, `departures`, `arrivals`).

    The official numbers end on 30.09. – so the check below compares your average **per hour** with the official
    average of the **same weekday in September**, scaled to a day of **17 operating hours** (06:00–23:00: flights
    may be planned from 06:00 until 22:45 / 22:55, 23:00–23:30 is only for delays –
    [Flughafen Zürich](https://www.flughafen-zuerich.ch/de/unternehmen/magazin/2025/politik_betriebszeiten)). Only
    complete hours of your data count.

    /// details | Hint
    Haversine and local time as in notebook 10 (TODO 10.1):
    `date_trunc('hour', timezone('Europe/Zurich', snapshot_time::TIMESTAMPTZ))`, and
    `COUNT(DISTINCT icao24) FILTER (WHERE flight_phase = 'climbing')`.
    ///
    """)
    return


@app.cell
def _(con, mo):
    # >>> TODO 14.2: ours = departures and arrivals at LSZH per local hour (15 km, airborne, < 6000 ft)
    # ?? ours = mo.sql("SELECT TIMESTAMP '2026-10-20 12:00' AS hour_local, 0 AS departures, 0 AS arrivals", engine=con)
    ours = mo.sql(
        """
        WITH near AS (
            SELECT icao24, flight_phase,
                   date_trunc('hour', timezone('Europe/Zurich', snapshot_time::TIMESTAMPTZ)) AS hour_local
            FROM silver.flights
            WHERE region = 'switzerland' AND NOT on_ground AND altitude_ft < 6000
              AND 2 * 6371 * ASIN(SQRT(POWER(SIN(RADIANS(latitude - 47.4581) / 2), 2)
                  + COS(RADIANS(47.4581)) * COS(RADIANS(latitude))
                    * POWER(SIN(RADIANS(longitude - 8.5481) / 2), 2))) <= 15)
        SELECT hour_local,
               COUNT(DISTINCT icao24) FILTER (WHERE flight_phase = 'climbing')   AS departures,
               COUNT(DISTINCT icao24) FILTER (WHERE flight_phase = 'descending') AS arrivals
        FROM near
        GROUP BY ALL
        ORDER BY hour_local
        """,
        engine=con,
    )
    # <<< TODO 14.2
    return (ours,)


@app.cell(hide_code=True)
def _(check, con, mo, ours, px):
    _out = []
    try:
        _snaps = con.execute("""
            SELECT date_trunc('hour', timezone('Europe/Zurich', snapshot_time::TIMESTAMPTZ)) AS hour_local,
                   COUNT(DISTINCT snapshot_time) AS snapshots
            FROM silver.flights WHERE region = 'switzerland' GROUP BY ALL""").df()
        _full = _snaps[_snaps.snapshots >= 0.85 * _snaps.snapshots.median()]["hour_local"]
        _o = ours.assign(movements=ours["departures"] + ours["arrivals"])
        _o["hour_local"] = _o["hour_local"].astype("datetime64[us]")
        _op = _o[_o.hour_local.isin(_full.astype("datetime64[us]")) & _o.hour_local.dt.hour.between(6, 22)]
        _day = _o.hour_local.max()
        _wd = (_day.dayofweek + 1) % 7                     # DuckDB dayofweek: Sunday = 0
        _official = con.execute("""
            SELECT AVG(FLT_TOT_1), AVG(FLT_DEP_1), AVG(FLT_ARR_1) FROM apt_traffic
            WHERE APT_ICAO = 'LSZH' AND YEAR = 2026 AND MONTH_NUM = 9 AND dayofweek(FLT_DATE) = ?""", [_wd]).fetchone()
        _rate = _op["movements"].mean()
        _est = _rate * 17                               # hours 06..22 local
        _ratio = _est / _official[0]
        _ok = len(_op) >= 3 and 0.6 < _ratio < 1.4
        _out.append(check(
            _ok,
            f"your data: **{_rate:.0f} movements per operating hour** ({len(_op)} complete hours) → "
            f"**{_est:,.0f} per day**. Official: **{_official[0]:,.0f}** on an average {_day:%A} in September 2026 – "
            f"you capture **{_ratio * 100:.0f} %**",
            f"{len(_op)} complete operating hours, {_rate:.0f} movements per hour → {_est:,.0f} per day vs. "
            f"{_official[0]:,.0f} official ({_ratio * 100:.0f} %). Expected 60–140 % – check the filters (15 km, "
            f"< 6,000 ft, climbing / descending) and the local time." if len(_op) else
            "Expected departures and arrivals per local hour at LSZH (and at least 3 complete hours of data)."))
        if len(_o):
            _out.append(px.bar(_o, x="hour_local", y=["departures", "arrivals"], height=300,
                               labels={"hour_local": "Swiss time", "value": "aircraft", "variable": ""}))
    except Exception as _e:
        _out.append(check(False, "", f"Expected departures and arrivals per local hour at LSZH ({_e})."))
    mo.vstack(_out)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Fresher than the CSV: the dashboard feed (unofficial)

    The EUROCONTROL traffic dashboard reads a JSON file with the daily flights of large European airports (113 on
    20.10.2026, data **up to yesterday**). It is an internal feed of the dashboard, not a documented API: the format can change, and the
    numbers are preliminary. Use it for a quick look, not as the official source.
    """)
    return


@app.cell
def _(fl, mo):
    import json

    import pandas as pd

    try:
        _rows = json.loads(fl.cached("https://www.eurocontrol.int/Economics/tfc_apt_100_data.json",
                                      "eurocontrol_apt100.json", max_age_days=1).read_text())
        _df = pd.DataFrame(_rows[2:], columns=_rows[0])        # row 0: names, row 1: types, then data as strings
        _fresh = (_df[_df["Entity"] == "Zurich"][["Day", "Flights", "Flights 2025 (Reference)", "Flights 2019 (Reference)"]]
                 .assign(Day=lambda d: pd.to_datetime(d["Day"]))
                 .astype({"Flights": float, "Flights 2025 (Reference)": float, "Flights 2019 (Reference)": float})
                 .sort_values("Day").tail(14))
        _fresh["weekday"] = _fresh["Day"].dt.day_name()
        _view = mo.vstack([mo.md("Zurich, last 14 days (dashboard feed, preliminary):"), _fresh])
    except Exception as _e:
        _view = mo.callout(mo.md(f"The dashboard feed is not available right now (`{_e}`) – not needed for the "
                                 f"exercises."), kind="warn")
    _view
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Why the sample is not the census

    - **Receivers**: OpenSky only knows what its receivers hear. ADS-B is a radio signal and needs line of sight: the radio
      horizon of an aircraft at 30,000 ft is about 390 km (d = √(2·k·R·h), k = 4/3, R = 6,371 km – our own
      calculation, real receivers reach less), close to the ground behind a hill it is zero. Zurich is covered well
      (see 14.2); we would expect a valley airport in the Alps (Sion, Samedan) to be covered worse – check it.
    - **Who sends ADS-B**: in European airspace, IFR aircraft heavier than 5,700 kg or faster than 250 kt must
      (aircraft first certified before 7 June 1995 are exempt) – [EASA FAQ](https://www.easa.europa.eu/en/faq/119357).
      Lighter aircraft, VFR flights (gliders) and military flights are not covered by this rule; many of them do not
      send ADS-B. EUROCONTROL counts **IFR** flights only – we also see helicopters and VFR traffic below 6,000 ft.
    - **Snapshots**: every ~35 s. A climb through our 15 km circle lasts about 2 minutes (our data, 20.10.2026: median
      4 snapshots per departure, only 3 % seen once), so we rarely miss one – but an aircraft that flies the same
      airport twice in one hour counts once.
    - **Coverage** is therefore not one number: it depends on place, altitude, aircraft type and time.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 14.3 – CO₂: order-of-magnitude check

    EUROCONTROL publishes the CO₂ per state and month in `co2_state` (`CO2_QTY_TONNES`; `TF` = traffic, i.e.
    flights). It is the CO₂ of the **IFR flights departing from the state** that pay EUROCONTROL route charges –
    **for the whole flight**: Zurich–New York counts fully for Switzerland, New York–Zurich not at all
    ([dataset definition](https://ansperformance.eu/reference/dataset/emissions/)).

    Our estimate measures something else: the CO₂ burnt **inside a box over Switzerland** by everything that flies
    there – departures, arrivals and overflights, but only the part over Switzerland. The cell below runs the
    OpenAP model of notebook 08 (`gold.fuel_burn`) over your **whole** Swiss history into `burn_ch`
    (one row per position: `co2_kg` = CO₂ until the next snapshot).

    Compute `co2_hourly`: per **UTC hour** the CO₂ in **tonnes** (`co2_t`) of positions with latitude 45.8–47.8 and
    longitude 5.9–10.5 – only hours with at least 90 snapshots (complete hours). Then `co2_month_t` = the mean hourly
    value × 24 × 30.

    /// details | Hint
    `date_trunc('hour', snapshot_time)`, `SUM(co2_kg) / 1000`, `HAVING COUNT(DISTINCT snapshot_time) >= 90`;
    then in Python `co2_hourly["co2_t"].mean() * 24 * 30`.
    ///
    """)
    return


@app.cell
def _(con, fuel_co2, jobs, mo):
    if not con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'aircraft_types'").fetchone()[0]:
        with mo.status.spinner(title="downloading the OpenSky aircraft database (~108 MB, once) ..."):
            jobs.run_job(con, "aircraft_types", log=lambda *_: None)
    with mo.status.spinner(title="OpenAP fuel flow for every position ..."):
        _f = con.execute("""
            SELECT f.snapshot_time, f.icao24, f.callsign, f.airline_icao, f.on_ground, f.altitude_ft, f.speed_kmh,
                   f.vertical_rate, f.flight_phase, t.typecode
            FROM silver.flights f LEFT JOIN silver.aircraft_types t USING (icao24)
            WHERE f.region = 'switzerland'""").df()
        _burn = fuel_co2.estimate_fuel_burn(_f.drop(columns="typecode"),
                                            _f[["icao24", "typecode"]].drop_duplicates("icao24"))
        _burn["snapshot_time"] = _burn["snapshot_time"].dt.tz_localize(None)
        con.register("burn_df", _burn)
        con.execute("""CREATE OR REPLACE TABLE burn_ch AS
            SELECT b.*, f.latitude, f.longitude, f.on_ground FROM burn_df b
            JOIN silver.flights f USING (icao24, snapshot_time)""")
    _air, _modelled = con.execute("SELECT COUNT(*) FILTER (WHERE NOT on_ground), "
                                  "COUNT(fuel_kg_per_h) FILTER (WHERE NOT on_ground) FROM burn_ch").fetchone()
    mo.md(f"`burn_ch`: {_air:,} airborne positions, **{100 * _modelled / max(_air, 1):.0f} %** with a fuel estimate "
          f"(the rest: unknown type or a type OpenAP does not model)")
    return


@app.cell
def _(con, mo):
    # >>> TODO 14.3: co2_hourly = CO2 in t per complete UTC hour inside the Swiss box; co2_month_t = mean x 24 x 30
    # ?? co2_hourly = mo.sql("SELECT TIMESTAMP '2026-10-20 12:00' AS hour, 0.0 AS co2_t", engine=con)
    # ?? co2_month_t = 0.0
    co2_hourly = mo.sql(
        """
        SELECT date_trunc('hour', snapshot_time) AS hour,
               SUM(co2_kg) / 1000               AS co2_t,
               COUNT(DISTINCT icao24)           AS aircraft
        FROM burn_ch
        WHERE latitude BETWEEN 45.8 AND 47.8 AND longitude BETWEEN 5.9 AND 10.5
        GROUP BY ALL
        HAVING COUNT(DISTINCT snapshot_time) >= 90
        ORDER BY hour
        """,
        engine=con,
    )
    co2_month_t = float(co2_hourly["co2_t"].mean()) * 24 * 30
    # <<< TODO 14.3
    return co2_hourly, co2_month_t


@app.cell(hide_code=True)
def _(check, co2_hourly, co2_month_t, con, mo, px):
    _out = []
    try:
        _month, _co2, _tf = con.execute("""
            SELECT FLIGHT_MONTH, CO2_QTY_TONNES, TF FROM co2_state
            WHERE STATE_NAME ILIKE 'switzerland' ORDER BY FLIGHT_MONTH DESC LIMIT 1""").fetchone()
        _ratio = co2_month_t / _co2
        _ok = len(co2_hourly) >= 3 and "co2_t" in co2_hourly and 0.1 < _ratio < 10
        _out.append(check(
            _ok,
            f"our estimate: **{co2_hourly['co2_t'].mean():,.0f} t CO₂ per hour** over Switzerland → "
            f"**{co2_month_t / 1000:,.0f} kt per month**. EUROCONTROL, {_month:%B %Y}: **{_co2 / 1000:,.0f} kt** of "
            f"{_tf:,} departing flights – ratio {_ratio:.2f}: the same order of magnitude, but not the same thing",
            f"co2_month_t = {co2_month_t:,.0f} t vs. EUROCONTROL {_co2:,.0f} t – expected the same order of "
            f"magnitude (ratio 0.1–10) from at least 3 complete hours."))
        if _ok:
            _out.append(px.bar(co2_hourly, x="hour", y="co2_t", height=280,
                               labels={"hour": "hour (UTC)", "co2_t": "t CO₂ over Switzerland"}))
    except Exception as _e:
        _out.append(check(False, "", f"Expected co2_hourly (hour, co2_t) and co2_month_t ({_e})."))
    mo.vstack(_out)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 14.4 – Where does Zurich stand in Europe?

    Rank all airports by their **average movements per day in 2026** (`FLT_TOT_1`). Store in `ranking` the
    columns `rank` (1 = busiest), `airport` (ICAO code), `name`, `state` and `flights_per_day`, busiest first – all
    airports, not only the top.

    /// details | Hint
    `RANK() OVER (ORDER BY AVG(FLT_TOT_1) DESC)` works directly in a `GROUP BY` query.
    `ANY_VALUE(APT_NAME)` for the name.
    ///
    """)
    return


@app.cell
def _(con, mo):
    # >>> TODO 14.4: ranking = rank, airport, name, state, flights_per_day (2026, busiest first, all airports)
    # ?? ranking = mo.sql("SELECT 1 AS rank, 'TODO' AS airport, 'TODO' AS name, 'TODO' AS state, 0 AS flights_per_day", engine=con)
    ranking = mo.sql(
        """
        SELECT RANK() OVER (ORDER BY AVG(FLT_TOT_1) DESC) AS rank,
               APT_ICAO                                   AS airport,
               ANY_VALUE(APT_NAME)                        AS name,
               ANY_VALUE(STATE_NAME)                      AS state,
               ROUND(AVG(FLT_TOT_1))                      AS flights_per_day
        FROM apt_traffic
        WHERE YEAR = 2026
        GROUP BY APT_ICAO
        ORDER BY rank
        """,
        engine=con,
    )
    # <<< TODO 14.4
    return (ranking,)


@app.cell(hide_code=True)
def _(check, mo, px, ranking):
    _cols = {"rank", "airport", "name", "state", "flights_per_day"}
    _ok = (_cols <= set(ranking.columns) and len(ranking) > 100 and "LSZH" in set(ranking["airport"])
           and ranking["flights_per_day"].is_monotonic_decreasing)
    _out = []
    if _ok:
        _zrh = ranking[ranking.airport == "LSZH"].iloc[0]
        _gva = ranking[ranking.airport == "LSGG"]
        _out.append(check(True, f"{len(ranking)} airports – busiest: **{ranking.name.iloc[0]}** "
                                f"({ranking.flights_per_day.iloc[0]:,.0f} per day); **Zurich is number {_zrh['rank']}** "
                                f"({_zrh.flights_per_day:,.0f})" +
                          (f", Geneva number {_gva['rank'].iloc[0]}" if len(_gva) else ""), ""))
        _top = ranking[(ranking["rank"] <= 20) | ranking.airport.isin(["LSZH", "LSGG", "LFSB"])]
        _out.append(px.bar(_top, x="flights_per_day", y="name", orientation="h", height=520,
                           color=_top.airport.isin(["LSZH", "LSGG", "LFSB"]).map({True: "Swiss / Basel", False: "other"}),
                           labels={"flights_per_day": "movements per day, 2026", "name": "", "color": ""})
                    .update_yaxes(autorange="reversed"))
    else:
        _out.append(check(False, "", f"Expected all airports with the columns {sorted(_cols)}, busiest first."))
    mo.vstack(_out)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Think about it

    - **Sample vs. census**: your hourly count matched the official number quite well at Zurich. Would it at Sion or
      Lugano? Which kind of question can a sample answer, and which needs the census?
    - What does **coverage** mean for *your* analyses of 07–10 – detours, CO₂ per airline, night flights? Where does
      a missing receiver or a missing transponder bias the result, and in which direction?
    - The CO₂ comparison: our box counts overflights and only the Swiss part of each flight; EUROCONTROL counts whole
      departing flights incl. long-haul. Which number belongs in a Swiss climate target? The federal greenhouse gas
      inventory lists international flights separately, outside the national total
      ([FOEN](https://www.bafu.admin.ch/de/treibhausgasinventar-flugverkehr)) – find out how it computes them.
    - 2026 vs. 2019: which airport has recovered fully – and is "back to 2019" good news for the climate?

    **Next:** [15 – Holidays and traffic](15_holidays.py)
    """)
    return


if __name__ == "__main__":
    app.run()
