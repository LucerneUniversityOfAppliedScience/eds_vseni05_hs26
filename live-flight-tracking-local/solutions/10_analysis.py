import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium", app_title="10 – Analyses on DuckDB")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 10 – Analyses: what does your data say?

    Everything you collected – live, backfill, the analyses of 07–09 – sits in one DuckDB file. This notebook asks
    questions about **mobility and sustainability** with SQL. The filters at the top change every chart below
    (marimo re-runs the dependent cells).

    Data: `fl.snapshot()` – a copy of `flights.duckdb`, so the app can keep collecting. Re-run the first cell to get
    the newest data.

    **Remember:** `silver.flights` has one row per aircraft and *snapshot* (Switzerland every ~35 s, world every
    ~6 min). "How many aircraft" is always `COUNT(DISTINCT icao24)`, never `COUNT(*)`.
    """)
    return


@app.cell
def _(mo):
    import sys

    sys.path.insert(0, str(mo.notebook_dir().parent))
    import flightlab as fl
    from enrichment import jobs

    def check(ok, good, bad):
        return mo.callout(mo.md(("✅ " + good) if ok else ("❌ " + bad)), kind="success" if ok else "danger")

    con = fl.snapshot()
    fl.ensure(con, "aircraft_types", "flight_routes", "flights_with_routes", "fuel_burn", "route_efficiency")                     # tables the app computes – here if you only ran collector.py
    for _job, _table in [("aircraft_types", "aircraft_types"), ("flight_routes", "flight_routes")]:
        if not con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{_table}'").fetchone()[0]:
            jobs.run_job(con, _job, log=lambda *_: None)          # app not run yet: compute it in the copy
    _span = con.execute("SELECT MIN(snapshot_time), MAX(snapshot_time), COUNT(DISTINCT icao24) FROM silver.flights").fetchone()
    mo.md(f"**{_span[2]:,} aircraft** between {_span[0]:%d.%m. %H:%M} and {_span[1]:%d.%m. %H:%M} UTC")
    return check, con, fl


@app.cell
def _(con, mo):
    _feeds = [r for (r,) in con.execute("SELECT DISTINCT region FROM silver.flights ORDER BY 1").fetchall()]
    feed = mo.ui.dropdown(options=_feeds, value="switzerland" if "switzerland" in _feeds else _feeds[0], label="Feed")
    feed
    return (feed,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1 · Traffic over the day

    Different aircraft per hour. Swiss local time is UTC + 1 (CET) in winter and UTC + 2 in summer time, which runs from
    the last Sunday of March to the last Sunday of October ([Sommerzeitverordnung, SR 941.299.1, Art. 2](https://www.fedlex.admin.ch/eli/cc/1984/1105_1105_1105/de))
    – DuckDB converts it: `timezone('Europe/Zurich', snapshot_time::TIMESTAMPTZ)`.
    """)
    return


@app.cell
def _(con, feed, mo):
    traffic = mo.sql(
        f"""
        SELECT date_trunc('hour', timezone('Europe/Zurich', snapshot_time::TIMESTAMPTZ)) AS hour_local,
               COUNT(DISTINCT icao24)                                                 AS aircraft,
               COUNT(DISTINCT icao24) FILTER (WHERE NOT on_ground)                    AS airborne
        FROM silver.flights
        WHERE region = '{feed.value}'
        GROUP BY ALL ORDER BY hour_local
        """,
        engine=con,
    )
    return (traffic,)


@app.cell
def _(feed, mo, traffic):
    import plotly.express as px

    mo.vstack([px.bar(traffic, x="hour_local", y="airborne", height=300,
                      labels={"hour_local": "hour (Swiss time)", "airborne": f"aircraft in the air ({feed.value})"}),
               mo.md(f"Peak: **{int(traffic.airborne.max()) if len(traffic) else 0} aircraft** at "
                     f"{traffic.loc[traffic.airborne.idxmax(), 'hour_local']:%H:%M}" if len(traffic) else "")])
    return (px,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 10.1 – Night flights at Zurich

    Regular traffic at Zurich airport runs from **06:00 to 23:30** (local time); the half hour 23:00–23:30 is only for
    delayed flights. After 23:30 the airport may allow exceptions only for unforeseen extraordinary events, e.g. severe
    weather ([Flughafen Zürich, 19.2.2025](https://www.flughafen-zuerich.ch/de/unternehmen/magazin/2025/politik_betriebszeiten);
    [VIL Art. 39d](https://www.fedlex.admin.ch/eli/cc/1994/3050_3050_3050/de#art_39_d)). Federal law alone would allow
    delayed flights until 00:30 ([VIL Art. 39a](https://www.fedlex.admin.ch/eli/cc/1994/3050_3050_3050/de#art_39_a))
    – Zurich's operating rules are stricter.

    Count the **different aircraft** that were close to the airport and low – within **15 km** of LSZH
    (47.4581, 8.5481, the airport position in [OurAirports](https://ourairports.com/data/), also in `silver.airports`)
    and **below 5,000 ft** (the airport lies at 1,417 ft according to OurAirports) – per
    **local hour** between 22:00 and 07:00. Store the result in `night` with the columns `hour_local` and `aircraft`.

    /// details | Hint
    The haversine formula from notebook 06, the local time from section 1 and `hour(...)` for the hour of the day:
    `WHERE hour(local_time) >= 22 OR hour(local_time) < 7`.
    ///
    """)
    return


@app.cell
def _(con, mo):
    # >>> TODO 10.1: night = aircraft near LSZH (15 km, < 5000 ft) per local hour, 22:00–07:00
    # ?? night = mo.sql("SELECT 0 AS hour_local, 0 AS aircraft", engine=con)
    night = mo.sql(
        """
        WITH near AS (
            SELECT icao24, timezone('Europe/Zurich', snapshot_time::TIMESTAMPTZ) AS local_time
            FROM silver.flights
            WHERE altitude_ft < 5000 AND region = 'switzerland'
              AND 2 * 6371 * ASIN(SQRT(POWER(SIN(RADIANS(latitude - 47.4581) / 2), 2)
                  + COS(RADIANS(47.4581)) * COS(RADIANS(latitude)) * POWER(SIN(RADIANS(longitude - 8.5481) / 2), 2))) <= 15)
        SELECT * FROM (
            SELECT hour(local_time) AS hour_local, COUNT(DISTINCT icao24) AS aircraft
            FROM near
            WHERE hour(local_time) >= 22 OR hour(local_time) < 7
            GROUP BY ALL)
        ORDER BY (hour_local + 2) % 24                       -- 22, 23, 0, 1, ... 6
        """,
        engine=con,
    )
    # <<< TODO 10.1
    return (night,)


@app.cell(hide_code=True)
def _(check, night):
    _ok = {"hour_local", "aircraft"} <= set(night.columns) and night["hour_local"].isin(
        [22, 23, 0, 1, 2, 3, 4, 5, 6]).all()
    _curfew = int(night[night.hour_local.isin([0, 1, 2, 3, 4, 5])]["aircraft"].sum()) if _ok else 0
    check(_ok and len(night) > 0 and night["aircraft"].sum() > 0,
          f"{int(night.aircraft.sum())} aircraft near Zurich at night – {_curfew} between 00:00 and 06:00 "
          f"(after 23:30 only exceptions: who were they? look at callsign and altitude)" if _ok and len(night) else "",
          "Expected aircraft per local hour between 22 and 7 near LSZH (no data at night yet? collect over night "
          "or backfill 24 h).")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 10.2 – Which routes, and could you take the train?

    `silver.flight_routes` holds the scheduled route (adsbdb) of the callsigns the app looked up. Count the
    **different flights** (`icao24` + `callsign`) per route `origin_name → destination_name` in the Swiss feed and
    add the great-circle distance `route_distance_km`. Store the top 20 in `routes` with a column `rail_option`:
    **True if the route is shorter than 800 km**. The 800 km are **our own assumption**, not an official threshold – a
    rough line below which a direct train can compete. Fastest direct trains according to SBB:
    [Zurich–Paris 4 h 04](https://www.sbb.ch/en/leisure-holidays/europe/countries-cities/france/zurich-paris.html),
    [Zurich–Munich 3 h 32](https://www.sbb.ch/en/leisure-holidays/europe/countries-cities/germany/zurich-munich.html),
    [Zurich–Milan 3 h 17](https://business.sbb.ch/en/tickets-offers/tickets/tickets-europe/italy.html).
    Notebook 11 looks at real train times.
    """)
    return


@app.cell
def _(con, mo):
    # >>> TODO 10.2: routes = top 20 routes by number of flights, with route_distance_km and rail_option (< 800 km)
    # ?? routes = mo.sql("SELECT 'from' AS origin_name, 'to' AS destination_name, 0 AS flights, 0 AS route_distance_km, FALSE AS rail_option", engine=con)
    routes = mo.sql(
        """
        WITH seen AS (
            SELECT DISTINCT icao24, trim(callsign) AS callsign FROM silver.flights
            WHERE region = 'switzerland' AND callsign IS NOT NULL)
        SELECT r.origin_name, r.destination_name, COUNT(*) AS flights,
               ROUND(ANY_VALUE(r.route_distance_km)) AS route_distance_km,
               ANY_VALUE(r.route_distance_km) < 800 AS rail_option
        FROM seen s JOIN silver.flight_routes r USING (callsign)
        GROUP BY ALL
        ORDER BY flights DESC
        LIMIT 20
        """,
        engine=con,
    )
    # <<< TODO 10.2
    return (routes,)


@app.cell(hide_code=True)
def _(check, routes):
    _ok = {"origin_name", "destination_name", "flights", "route_distance_km", "rail_option"} <= set(routes.columns)
    _rail = int(routes[routes.rail_option == True]["flights"].sum()) if _ok else 0      # noqa: E712
    check(_ok and len(routes) > 3 and routes["flights"].iloc[0] >= routes["flights"].iloc[-1] and routes.flights.sum() > 0,
          f"{int(routes.flights.sum())} flights on the top 20 routes, {_rail} of them shorter than 800 km",
          "Expected the top routes with flights, route_distance_km and rail_option.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 10.3 – Landing here or just passing through?

    Many aircraft over Switzerland neither take off nor land here – they only cross at cruise altitude. How many?
    `gold.flights_with_routes` (the aircraft on the map right now, with route) has the flags
    `departing_from_region`, `arriving_in_region` and `overflight`. Count the aircraft per category in `transit`
    (columns `category`, `aircraft`) – only aircraft **over Switzerland right now** with a plausible route (with the
    world feed, the table also holds aircraft over Asia). "Over Switzerland" is a box, lat 45.8–47.8, lon 5.9–10.5:
    the Swiss municipalities (BFS, gg25, notebook 12) reach from 45.82 to 47.81 N and 5.95 to 10.49 E, and the box
    also contains bits of the neighbouring countries.
    """)
    return


@app.cell
def _(con, mo):
    # >>> TODO 10.3: transit = aircraft per category (departing / arriving / overflight) right now
    # ?? transit = mo.sql("SELECT 'todo' AS category, 0 AS aircraft", engine=con)
    transit = mo.sql(
        """
        SELECT CASE WHEN departing_from_region AND arriving_in_region THEN 'domestic'
                    WHEN departing_from_region THEN 'departing'
                    WHEN arriving_in_region THEN 'arriving'
                    ELSE 'overflight' END AS category,
               COUNT(DISTINCT icao24) AS aircraft
        FROM gold.flights_with_routes
        WHERE has_route AND route_plausible
          AND latitude BETWEEN 45.8 AND 47.8 AND longitude BETWEEN 5.9 AND 10.5     -- over Switzerland right now
        GROUP BY ALL ORDER BY aircraft DESC
        """,
        engine=con,
    )
    # <<< TODO 10.3
    return (transit,)


@app.cell(hide_code=True)
def _(check, mo, px, transit):
    _ok = {"category", "aircraft"} <= set(transit.columns) and "overflight" in set(transit.get("category", []))
    _share = (transit.set_index("category")["aircraft"].get("overflight", 0) / transit.aircraft.sum() * 100
              if _ok and transit.aircraft.sum() else 0)
    mo.vstack([check(_ok, f"{_share:.0f} % of the aircraft with a known route only fly over the region",
                     "Expected the categories departing / arriving / overflight with the number of aircraft."),
               px.pie(transit, names="category", values="aircraft", height=280) if _ok else ""])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 10.4 – How old is the fleet?

    Join the aircraft of the Swiss feed with `silver.aircraft_types` (`built_year`) and compute per airline the
    number of different aircraft and the **average age** in years (`year(current_date) - built_year`). Only airlines
    with at least 5 aircraft of known age. Store it in `fleet`, oldest first. Pick an airline in the dropdown below.

    The OpenSky database has a build year for only a few airliners (class data, 20.10.2026: 77 of 2,035 aircraft
    of known airlines in the Swiss feed) – look at how many aircraft drop out in yours.
    """)
    return


@app.cell
def _(con, mo):
    # >>> TODO 10.4: fleet = airline, aircraft, avg_age_years (>= 5 aircraft with known age), oldest first
    # ?? fleet = mo.sql("SELECT 'todo' AS airline, 0 AS aircraft, 0 AS avg_age_years", engine=con)
    fleet = mo.sql(
        """
        WITH ac AS (
            SELECT DISTINCT f.icao24, COALESCE(a.airline_name, 'unknown / private') AS airline, t.built_year
            FROM silver.flights f
            LEFT JOIN silver.airlines a USING (airline_icao)
            JOIN silver.aircraft_types t USING (icao24)
            WHERE f.region = 'switzerland' AND t.built_year IS NOT NULL)
        SELECT airline, COUNT(*) AS aircraft, ROUND(AVG(year(current_date) - built_year), 1) AS avg_age_years
        FROM ac GROUP BY airline HAVING COUNT(*) >= 5
        ORDER BY avg_age_years DESC
        """,
        engine=con,
    )
    # <<< TODO 10.4
    return (fleet,)


@app.cell(hide_code=True)
def _(check, fleet):
    _ok = {"airline", "aircraft", "avg_age_years"} <= set(fleet.columns) and len(fleet) > 0 and fleet.aircraft.min() >= 5
    check(_ok, f"{len(fleet)} airlines – oldest fleet: {fleet.airline.iloc[0]} ({fleet.avg_age_years.iloc[0]} years), "
               f"youngest: {fleet.airline.iloc[-1]} ({fleet.avg_age_years.iloc[-1]} years)" if _ok else "",
          "Expected airlines with >= 5 aircraft and their average age, oldest first.")
    return


@app.cell
def _(con, fleet, mo):
    airline = mo.ui.dropdown(options=list(fleet["airline"]) if "airline" in fleet else [],
                             value=(list(fleet["airline"]) or [None])[0] if "airline" in fleet else None,
                             label="Airline")
    airline
    return (airline,)


@app.cell
def _(airline, con, mo):
    mo.stop(not airline.value, mo.md("*pick an airline*"))
    _types = con.execute("""
        SELECT t.typecode, ANY_VALUE(t.model) AS model, COUNT(DISTINCT f.icao24) AS aircraft,
               ROUND(AVG(year(current_date) - t.built_year), 1) AS avg_age_years
        FROM silver.flights f LEFT JOIN silver.airlines a USING (airline_icao)
        JOIN silver.aircraft_types t USING (icao24)
        WHERE f.region = 'switzerland' AND COALESCE(a.airline_name, 'unknown / private') = ?
        GROUP BY ALL ORDER BY aircraft DESC""", [airline.value]).df()
    mo.vstack([mo.md(f"### {airline.value}: aircraft types"), _types])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 10.5 – Breaking news: the Skyguide outage of 20 October 2026

    What [20 Minuten](https://www.20min.ch/story/schweiz-technische-stoerung-bei-skyguide-luftraum-kapazitaeten-reduziert-103652678)
    reported on 20 October 2026: on Tuesday afternoon, a technical fault at Skyguide temporarily stopped take-offs and
    landings at Zurich airport. The faulty system generates the radar picture for the air traffic controllers.
    Capacity in Swiss airspace was halved for a while; after the fix Skyguide raised it to 80 % for a 24-hour
    stabilisation phase. The article gives **no exact times** – **your data has them.**

    Count the **departures** per **15 minutes (Swiss time)** at **LSZH** (47.4581, 8.5481, OurAirports, as in 10.1) and – for
    comparison, the article mentions only Zurich – **LSGG** (46.2381, 6.1090, OurAirports) between 10:00 and 16:00 UTC:
    different aircraft within **15 km**, **below 6,000 ft** and `flight_phase = 'climbing'`. Store the result in
    `outage` with the columns `t_local`, `airport`, `departures`.

    The times in the check below (and the red band in the chart) come from **our own data** (the class bucket), not
    from a press release: Zurich departures fell from about 10 to about 3 per 15 minutes in the buckets 14:15–15:00
    Swiss time and were back at 15:15.

    The class bucket on R2 has the whole afternoon – if your database does not, the button loads exactly this window
    into your copy (about 700 files, no OpenSky credits).

    /// details | Hint
    A small airport table with `VALUES`, joined with the haversine distance from 10.1:
    ```sql
    WITH apt(airport, lat, lon) AS (VALUES ('LSZH', 47.4581, 8.5481), ('LSGG', 46.2381, 6.1090))
    SELECT time_bucket(INTERVAL 15 MINUTE, timezone('Europe/Zurich', f.snapshot_time::TIMESTAMPTZ)) AS t_local, ...
    FROM silver.flights f JOIN apt a ON <distance> <= 15
    ```
    ///
    """)
    return


@app.cell
def _(con, mo):
    _have = con.execute("""
        SELECT COUNT(*) FROM bronze.snapshots WHERE region = 'switzerland'
          AND time BETWEEN epoch(TIMESTAMP '2026-10-20 10:00') AND epoch(TIMESTAMP '2026-10-20 16:00')""").fetchone()[0]
    load_event = mo.ui.run_button(label="Load 20.10.2026, 10:00–16:00 UTC from R2")
    mo.hstack([mo.md(f"Snapshots in the window: **{_have}** of about 610"), load_event], justify="start")
    return (load_event,)


@app.cell
def _(con, fl, load_event, mo):
    import datetime

    if load_event.value:
        with mo.status.spinner(title="backfilling the afternoon from R2 ..."):
            fl.backfill(con, "switzerland", since=datetime.datetime(2026, 10, 20, 10),
                        until=datetime.datetime(2026, 10, 20, 16), env=fl.load_env(), log=lambda *_: None)
            fl.build_silver(con)
    event_loaded = load_event.value
    return (event_loaded,)


@app.cell
def _(con, event_loaded, mo):
    event_loaded                                            # rerun after the backfill
    # >>> TODO 10.5: outage = departures per 15 minutes (Swiss time) at LSZH and LSGG, 10:00–16:00 UTC
    # ?? outage = mo.sql("SELECT TIMESTAMP '2026-10-20 12:00' AS t_local, 'LSZH' AS airport, 0 AS departures", engine=con)
    outage = mo.sql(
        """
        WITH apt(airport, lat, lon) AS (VALUES ('LSZH', 47.4581, 8.5481), ('LSGG', 46.2381, 6.1090))
        SELECT time_bucket(INTERVAL 15 MINUTE, timezone('Europe/Zurich', f.snapshot_time::TIMESTAMPTZ)) AS t_local,
               a.airport,
               COUNT(DISTINCT f.icao24)                                                       AS departures
        FROM silver.flights f
        JOIN apt a
          ON 2 * 6371 * ASIN(SQRT(POWER(SIN(RADIANS(f.latitude - a.lat) / 2), 2)
             + COS(RADIANS(a.lat)) * COS(RADIANS(f.latitude)) * POWER(SIN(RADIANS(f.longitude - a.lon) / 2), 2))) <= 15
        WHERE f.region = 'switzerland' AND f.flight_phase = 'climbing' AND f.altitude_ft < 6000
          AND f.snapshot_time BETWEEN TIMESTAMP '2026-10-20 10:00' AND TIMESTAMP '2026-10-20 16:00'
        GROUP BY ALL
        ORDER BY t_local, airport
        """,
        engine=con,
    )
    # <<< TODO 10.5
    return (outage,)


@app.cell(hide_code=True)
def _(check, mo, outage, px):
    _ok = {"t_local", "airport", "departures"} <= set(outage.columns) and outage["departures"].sum() > 20
    _out = []
    if _ok:
        _zrh = outage[outage.airport == "LSZH"].set_index("t_local")["departures"]
        _before = _zrh.between_time("12:00", "14:00").mean()
        _during = _zrh.between_time("14:15", "15:00").mean()
        _out.append(check(_during < 0.5 * _before,
                          f"Zurich: {_before:.0f} departures per 15 minutes before 14:00, {_during:.0f} between "
                          f"14:15 and 15:00 – the stop is in your data",
                          f"Zurich before: {_before:.0f}, 14:15–15:00: {_during:.0f} per 15 minutes – no drop? "
                          f"Check the filters (15 km, < 6,000 ft, climbing) and the time zone."))
        _out.append(px.line(outage, x="t_local", y="departures", color="airport", markers=True, height=320,
                            labels={"t_local": "Swiss time, 20.10.2026", "departures": "departures per 15 min"})
                    .add_vrect(x0="2026-10-20 14:10", x1="2026-10-20 15:10", fillcolor="red", opacity=0.1,
                               line_width=0, annotation_text="drop in our data"))
    else:
        _out.append(check(False, "", "Expected departures per 15 minutes at LSZH and LSGG on 20.10.2026 – press the "
                                     "button above if the window is missing in your database."))
    mo.vstack(_out)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **Think about it**

    - The article says landings stopped too – what do the **arrivals** show? (`flight_phase = 'descending'`) Were
      aircraft sent into holding patterns or to other airports?
    - "Capacity halved for a while": does the number of aircraft **in the air over Switzerland** show it? (section 1,
      per 15 min)
    - How long did it take until Zurich was back to normal – and did the evening wave leave late?
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Your own question

    Write the SQL in the cell below. Ideas:

    - Which **airline** has the most CO₂ per aircraft-hour? (`gold.fuel_burn`)
    - Do **detours** (`gold.route_efficiency`) differ between airlines – or between destinations in Asia and Europe?
    - **Weekdays**: is Friday busier than Tuesday? (needs a few days of backfill: `uv run collector.py --backfill-hours 120 --no-live`)
    - **Low overflights**: which Swiss places see aircraft below 10,000 ft most often (round lat/lon to 0.05°)?
    - Which **registration countries** dominate the world feed – and which aircraft fly in restricted airspace
      (`gold.current_firs`)?

    **Next:** [11 – Plane or train?](11_rail_vs_plane.py) – then 12–19 go deeper (overflights, weather, official
    numbers, holidays, contrails, disruptions, holdings, runways), 20 turns your analysis into a dashboard.
    """)
    return


@app.cell
def _(con, mo):
    mine = mo.sql(
        """
        SELECT airline_name, COUNT(DISTINCT icao24) AS aircraft
        FROM gold.current_positions
        GROUP BY ALL ORDER BY aircraft DESC LIMIT 10
        """,
        engine=con,
    )
    return


if __name__ == "__main__":
    app.run()
