import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium", app_title="05 – Silver")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 05 – Silver: clean positions, and the trap a backfill sets

    Bronze is raw: metres, m/s, callsigns with spaces, aircraft without a position. **Silver** is what analyses use:
    one clean row per aircraft and moment, units for humans, the airline code, a flight phase – and the **lookups**:
    the airline's name and what kind of aircraft it is, joined from reference tables. Silver is where codes get their
    meaning; gold (06) only reads it.

    This notebook is mostly **SQL** – marimo runs SQL cells directly on DuckDB (`mo.sql(..., engine=con)`) and
    gives you a DataFrame back.

    The second half is about the most common bug of incremental pipelines – and you will see it happen.
    """)
    return


@app.cell
def _(mo):
    import sys

    import duckdb
    import pandas as pd

    sys.path.insert(0, str(mo.notebook_dir().parent))
    import flightlab as fl

    def check(ok, good, bad):
        return mo.callout(mo.md(("✅ " + good) if ok else ("❌ " + bad)), kind="success" if ok else "danger")

    con = fl.practice(5)                            # practice_05.duckdb – continues from the step before
    fl.init_db(con)
    fl.ensure_sample_data(con)               # skipped 02-04? then 2 hours from R2 (or one live call)
    _n = con.execute("SELECT COUNT(*), COUNT(DISTINCT time) FROM bronze.snapshots").fetchone()
    mo.md(f"bronze: **{_n[0]} snapshots** to work with")
    return check, con, fl, pd


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 5.1 – The silver SELECT

    Complete the SQL so that it returns, per row of `bronze.states`:

    | column | rule |
    |---|---|
    | snapshot_time | `to_timestamp(time)::TIMESTAMP` (UTC) |
    | callsign | `NULLIF(callsign, '')` |
    | airline_icao | the first 3 letters of the callsign **if** it looks like an airline: 3 letters + a digit (`SWR123`), else NULL – `regexp_matches(callsign, '^[A-Z]{3}[0-9]')` |
    | altitude_ft | `baro_altitude * 3.28084`, rounded |
    | speed_kmh | `velocity * 3.6`, rounded |
    | heading | `true_track`, rounded (strictly the direction of travel over ground, not where the nose points) |
    | flight_phase | `on ground` / `climbing` (vertical_rate > 2.5 m/s) / `descending` (< -2.5) / `cruise` |

    and drops rows **without position** or with an impossible altitude (outside -500 … 20,000 m).

    The thresholds are **our own assumptions**, not standards: 2.5 m/s (≈ 500 ft/min) separates a real climb or
    descent from level flight with a little noise; -500 … 20,000 m is a generous plausibility range. The airline rule
    is a heuristic: airlines get a three-letter ICAO designator ([ICAO 3LD](https://www.icao.int/3LD/Home/Rules)),
    and their callsigns usually start with it followed by the flight number – private aircraft often send their
    registration instead (`HBXYZ`).
    """)
    return


@app.cell
def _(con, mo):
    # >>> TODO 5.1: the silver SELECT (columns and filter as in the table above)
    # ?? silver_preview = mo.sql("SELECT * FROM bronze.states LIMIT 20", engine=con)
    silver_preview = mo.sql(
        """
        SELECT
            to_timestamp(time)::TIMESTAMP                     AS snapshot_time,
            icao24, region,
            NULLIF(callsign, '')                              AS callsign,
            CASE WHEN regexp_matches(callsign, '^[A-Z]{3}[0-9]') THEN substr(callsign, 1, 3) END AS airline_icao,
            origin_country, latitude, longitude, on_ground,
            ROUND(baro_altitude)                              AS altitude_m,
            ROUND(baro_altitude * 3.28084)                    AS altitude_ft,
            ROUND(velocity * 3.6)                             AS speed_kmh,
            ROUND(true_track)                                 AS heading,
            vertical_rate,
            CASE WHEN on_ground THEN 'on ground'
                 WHEN vertical_rate > 2.5 THEN 'climbing'
                 WHEN vertical_rate < -2.5 THEN 'descending'
                 ELSE 'cruise' END                            AS flight_phase,
            squawk, category
        FROM bronze.states
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
          AND (baro_altitude IS NULL OR baro_altitude BETWEEN -500 AND 20000)
        LIMIT 200
        """,
        engine=con,
    )
    # <<< TODO 5.1
    return (silver_preview,)


@app.cell(hide_code=True)
def _(check, silver_preview):
    _cols = set(silver_preview.columns)
    _need = {"snapshot_time", "airline_icao", "altitude_ft", "speed_kmh", "flight_phase"}
    _ok = _need <= _cols
    _phases = set(silver_preview["flight_phase"]) if "flight_phase" in _cols else set()
    _air = silver_preview[silver_preview["callsign"].fillna("").str.match(r"^[A-Z]{3}\d")] if _ok else None
    check(_ok and _phases <= {"on ground", "climbing", "descending", "cruise"} and _air is not None
          and (_air["airline_icao"] == _air["callsign"].str[:3]).all(),
          f"columns ok, phases {sorted(_phases)}, airline codes e.g. "
          f"{', '.join(_air['airline_icao'].dropna().unique()[:5]) if _ok else ''}",
          f"Missing columns {sorted(_need - _cols)} – or flight_phase / airline_icao do not follow the rules.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 5.2 – Airlines

    `airline_icao` says `SWR` – silver should also say *Swiss International Air Lines*. The names come from
    [OpenFlights](https://openflights.org/data) (Open Database License), `airlines.dat`, downloaded once into `cache/`.

    The file has **no header**; the columns are `id, name, alias, iata, icao, callsign, country, active`,
    missing values are written as `\N`. Keep rows whose `icao` is three capital letters, prefer active airlines
    (`active == "Y"`) when a code appears twice, and store **`silver.airlines`** with `airline_icao`, `airline_iata`,
    `airline_name`, `callsign`, `country`.

    Careful: OpenFlights itself warns that `active` is not reliable. `SWR` appears twice, both marked `Y`:
    *Swiss International Air Lines* and *Swissair*. Sort with `kind="stable"`, so the file order decides between equal
    rows and the result does not change from run to run. And **one row per code** matters for the join in 5.3: two
    rows for `SWR` would duplicate every Swiss position.
    """)
    return


@app.cell
def _(con, fl, pd):
    airlines_dat = fl.cached(fl.AIRLINES_URL, "airlines.dat")

    # >>> TODO 5.2: silver.airlines (one row per ICAO code, active ones first)
    # ?? airlines = pd.DataFrame()
    _al = pd.read_csv(airlines_dat, header=None, na_values=["\\N"], keep_default_na=False,
                      names=["id", "name", "alias", "iata", "icao", "callsign", "country", "active"])
    _al = _al[_al["icao"].str.fullmatch(r"[A-Z]{3}", na=False)].sort_values("active", ascending=False,
                                                                                  kind="stable")
    airlines = _al.drop_duplicates("icao").rename(columns={"icao": "airline_icao", "iata": "airline_iata",
                                                           "name": "airline_name"})[
        ["airline_icao", "airline_iata", "airline_name", "callsign", "country"]]
    con.execute("CREATE OR REPLACE TABLE silver.airlines AS SELECT * FROM airlines")
    # <<< TODO 5.2
    return (airlines,)


@app.cell(hide_code=True)
def _(airlines, check, con):
    try:
        _swr = con.execute("SELECT airline_name FROM silver.airlines WHERE airline_icao = 'SWR'").fetchone()
        _dup = con.execute("SELECT COUNT(*) - COUNT(DISTINCT airline_icao) FROM silver.airlines").fetchone()[0]
    except Exception:
        _swr, _dup = None, -1
    check(len(airlines) > 1000 and _swr is not None and "Swiss International" in _swr[0] and _dup == 0,
          f"{len(airlines):,} airlines, SWR = {_swr and _swr[0]}",
          "silver.airlines should contain SWR = Swiss International Air Lines (not Swissair – stable sort?), "
          "one row per airline_icao.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Aircraft categories – a fixed code list

    `category` is a number from 0 to 20. What the numbers mean is defined by OpenSky
    ([REST API docs](https://openskynetwork.github.io/opensky-api/rest.html), "Aircraft category"): 2 = light,
    4 = large, 6 = heavy, 8 = rotorcraft ... That list is in `fl.AIRCRAFT_CATEGORIES` – the cell below turns it into
    the lookup table **`silver.aircraft_categories`**. Nothing to do here.
    """)
    return


@app.cell
def _(con, fl, pd):
    categories = pd.DataFrame({"category": list(fl.AIRCRAFT_CATEGORIES),
                               "description": list(fl.AIRCRAFT_CATEGORIES.values())})
    con.execute("CREATE OR REPLACE TABLE silver.aircraft_categories AS SELECT * FROM categories")
    return (categories,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 5.3 – The lookups in silver

    Extend your silver SELECT from 5.1 with two `LEFT JOIN`s:

    - `silver.airlines` -> `airline_name`. The join key is the airline code you compute – so the condition repeats
      the rule: `a.airline_icao = substr(s.callsign, 1, 3) AND regexp_matches(s.callsign, '^[A-Z]{3}[0-9]')`.
    - `silver.aircraft_categories` on `category` -> `aircraft_category` (the `description`).

    **LEFT** join, because a position without a known airline (private aircraft, a code OpenFlights doesn't know) is
    still a position. And check that the join doesn't add rows: silver must have exactly as many rows as before.
    """)
    return


@app.cell
def _(con, mo):
    # >>> TODO 5.3: silver SELECT + airline_name + aircraft_category (LEFT JOINs)
    # ?? silver_lookup = mo.sql("SELECT * FROM bronze.states s LIMIT 200", engine=con)
    silver_lookup = mo.sql(
        """
        SELECT
            to_timestamp(s.time)::TIMESTAMP                   AS snapshot_time,
            s.icao24, s.region,
            NULLIF(s.callsign, '')                            AS callsign,
            CASE WHEN regexp_matches(s.callsign, '^[A-Z]{3}[0-9]') THEN substr(s.callsign, 1, 3) END AS airline_icao,
            a.airline_name,
            s.category, c.description                         AS aircraft_category,
            ROUND(s.baro_altitude * 3.28084)                  AS altitude_ft
        FROM bronze.states s
        LEFT JOIN silver.airlines a
               ON a.airline_icao = substr(s.callsign, 1, 3) AND regexp_matches(s.callsign, '^[A-Z]{3}[0-9]')
        LEFT JOIN silver.aircraft_categories c ON c.category = s.category
        WHERE s.latitude IS NOT NULL AND s.longitude IS NOT NULL
          AND (s.baro_altitude IS NULL OR s.baro_altitude BETWEEN -500 AND 20000)
        """,
        engine=con,
    )
    # <<< TODO 5.3
    return (silver_lookup,)


@app.cell(hide_code=True)
def _(check, con, silver_lookup):
    _valid = con.execute("""SELECT COUNT(*) FROM bronze.states WHERE latitude IS NOT NULL AND longitude IS NOT NULL
                            AND (baro_altitude IS NULL OR baro_altitude BETWEEN -500 AND 20000)""").fetchone()[0]
    _cols = set(silver_lookup.columns)
    _ok = {"airline_name", "aircraft_category"} <= _cols and len(silver_lookup) == _valid
    _named = silver_lookup["airline_name"].notna().mean() if _ok else 0
    check(_ok, f"{len(silver_lookup):,} rows – as many as without the joins; {_named:.0%} of the positions have an "
               f"airline name",
          f"Need the columns airline_name and aircraft_category, and exactly {_valid:,} rows (no LIMIT, no rows "
          f"added by the join) – got {len(silver_lookup):,}.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## The trap: incremental with a time watermark

    Silver should not be rebuilt from all of bronze every 35 seconds – only **new** rows. The obvious idea: remember
    the newest `snapshot_time` in silver (the *watermark*) and insert only bronze rows that are newer.

    Let's play the real sequence of events:

    1. Your collector runs since this morning – silver is up to date with the **newest** snapshot.
    2. Then you run the **backfill** (04): it brings the snapshots of the last days – all **older** than the watermark.

    The cell below does exactly that with a watermark-based `update_silver_watermark()`. Look at the numbers.
    """)
    return


@app.cell
def _(con):
    SILVER_COLUMNS = """
        to_timestamp(s.time)::TIMESTAMP AS snapshot_time, s.icao24, s.region, NULLIF(s.callsign, '') AS callsign,
        CASE WHEN regexp_matches(s.callsign, '^[A-Z]{3}[0-9]') THEN substr(s.callsign, 1, 3) END AS airline_icao,
        s.origin_country, s.latitude, s.longitude, s.on_ground, ROUND(s.baro_altitude) AS altitude_m,
        ROUND(s.baro_altitude * 3.28084) AS altitude_ft, ROUND(s.velocity * 3.6) AS speed_kmh,
        ROUND(s.true_track) AS heading, s.vertical_rate,
        CASE WHEN s.on_ground THEN 'on ground' WHEN s.vertical_rate > 2.5 THEN 'climbing'
             WHEN s.vertical_rate < -2.5 THEN 'descending' ELSE 'cruise' END AS flight_phase,
        s.squawk, s.category, a.airline_name, c.description AS aircraft_category"""
    SILVER_FROM = """bronze.states s
        LEFT JOIN silver.airlines a
               ON a.airline_icao = substr(s.callsign, 1, 3) AND regexp_matches(s.callsign, '^[A-Z]{3}[0-9]')
        LEFT JOIN silver.aircraft_categories c ON c.category = s.category"""
    VALID = ("s.latitude IS NOT NULL AND s.longitude IS NOT NULL "
             "AND (s.baro_altitude IS NULL OR s.baro_altitude BETWEEN -500 AND 20000)")

    def fresh_silver(name):
        con.execute(f"DROP TABLE IF EXISTS silver.{name}")
        con.execute(f"""CREATE TABLE silver.{name} AS SELECT {SILVER_COLUMNS} FROM {SILVER_FROM} LIMIT 0""")
        con.execute(f"ALTER TABLE silver.{name} ADD PRIMARY KEY (icao24, snapshot_time)")

    # the scenario: "visible" = what bronze contains at that moment (live first, the backfill later)
    newest = con.execute("SELECT MAX(time) FROM bronze.snapshots").fetchone()[0]
    stage_live = f"s.time = {newest}"                      # 1. only the newest snapshot has arrived
    stage_all = "TRUE"                                     # 2. the backfill brought all older ones
    return SILVER_COLUMNS, SILVER_FROM, VALID, fresh_silver, stage_all, stage_live


@app.cell
def _(SILVER_COLUMNS, SILVER_FROM, VALID, con, fresh_silver, mo, stage_all, stage_live):
    def update_silver_watermark(visible):
        """Insert bronze rows newer than the newest snapshot_time already in silver."""
        con.execute(f"""
            INSERT INTO silver.flights_watermark
            SELECT {SILVER_COLUMNS} FROM {SILVER_FROM}
            WHERE {visible} AND {VALID}
              AND to_timestamp(s.time)::TIMESTAMP >
                  (SELECT COALESCE(MAX(snapshot_time), TIMESTAMP '1970-01-01') FROM silver.flights_watermark)
            ON CONFLICT DO NOTHING""")

    fresh_silver("flights_watermark")
    update_silver_watermark(stage_live)                    # morning: the collector
    update_silver_watermark(stage_all)                     # afternoon: the backfill
    _bronze = con.execute("SELECT COUNT(DISTINCT time) FROM bronze.states").fetchone()[0]
    _silver = con.execute("SELECT COUNT(DISTINCT snapshot_time) FROM silver.flights_watermark").fetchone()[0]
    mo.callout(mo.md(f"""
    **bronze: {_bronze} snapshots – silver (watermark): {_silver} snapshot(s).**

    The backfill is in bronze, but silver never got it: every backfilled row is *older* than the watermark, so the
    `>` filter drops it. No error, no warning – the dashboards are simply wrong. This is the bug.
    """), kind="warn")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 5.4 – Incremental per snapshot instead of per time

    Remember **which snapshots** were processed, not up to which time:

    1. a table `silver.processed` with the key `(region, time)`
    2. `update_silver_by_snapshot(visible)`: find the visible bronze snapshots that are **not** in `silver.processed`
       (an *anti join*), insert their states into `silver.flights_by_snapshot`, then add them to `silver.processed`.

    Run the same scenario – silver must end up with **all** snapshots.

    /// details | Hint
    ```sql
    -- snapshots still to do
    SELECT DISTINCT s.region, s.time FROM bronze.states s
    WHERE {visible}
      AND NOT EXISTS (SELECT 1 FROM silver.processed p WHERE p.region = s.region AND p.time = s.time)
    ```
    Insert with `... FROM bronze.states s WHERE (s.region, s.time) IN (SELECT region, time FROM todo)`. DuckDB also
    has `SEMI JOIN` / `ANTI JOIN` as keywords.
    ///
    """)
    return


@app.cell
def _(SILVER_COLUMNS, SILVER_FROM, VALID, con, fresh_silver, stage_all, stage_live):
    fresh_silver("flights_by_snapshot")
    con.execute("DROP TABLE IF EXISTS silver.processed")
    con.execute("CREATE TABLE silver.processed (region VARCHAR, time BIGINT, PRIMARY KEY (region, time))")

    # >>> TODO 5.4: update_silver_by_snapshot(visible) – anti join on silver.processed, insert, mark as processed
    # ?? def update_silver_by_snapshot(visible):
    # ??     pass
    def update_silver_by_snapshot(visible):
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE todo AS
            SELECT DISTINCT s.region, s.time FROM bronze.states s
            WHERE {visible}
              AND NOT EXISTS (SELECT 1 FROM silver.processed p WHERE p.region = s.region AND p.time = s.time)""")
        con.execute(f"""
            INSERT INTO silver.flights_by_snapshot
            SELECT {SILVER_COLUMNS} FROM {SILVER_FROM}
            WHERE (s.region, s.time) IN (SELECT region, time FROM todo) AND {VALID}
            ON CONFLICT DO NOTHING""")
        con.execute("INSERT INTO silver.processed SELECT region, time FROM todo ON CONFLICT DO NOTHING")
    # <<< TODO 5.4

    update_silver_by_snapshot(stage_live)                  # morning: the collector
    update_silver_by_snapshot(stage_all)                   # afternoon: the backfill
    update_silver_by_snapshot(stage_all)                   # and once more – nothing may change
    return


@app.cell(hide_code=True)
def _(check, con):
    _bronze = con.execute("SELECT COUNT(DISTINCT time) FROM bronze.states").fetchone()[0]
    _silver = con.execute("SELECT COUNT(DISTINCT snapshot_time) FROM silver.flights_by_snapshot").fetchone()[0]
    _dupes = con.execute("SELECT COUNT(*) - COUNT(DISTINCT (icao24, snapshot_time)) "
                         "FROM silver.flights_by_snapshot").fetchone()[0]
    check(_silver == _bronze and _dupes == 0,
          f"silver has all {_silver} snapshots, no duplicates – the backfill arrived, and running it again changed "
          f"nothing.",
          f"silver has {_silver} of {_bronze} snapshots ({_dupes} duplicates).")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Think about it

    - Where else does the watermark trap bite? (Late events from a phone that was offline, a corrected invoice ...)
    - `ON CONFLICT DO NOTHING` on `(icao24, snapshot_time)`: when can the same aircraft at the same second come twice?
      (Hint: the bucket has *switzerland* and *world* – both see the aircraft over Zurich.)
    - `flightlab.build_silver(con)` is the finished version used by the collector and the app – compare it with yours.

    - Silver stores the airline name **as it was when the row was inserted**. If OpenFlights renames an airline, old
      rows keep the old name – rebuild silver, or update it. When is that what you want, and when not?

    **Next:** [06 – Gold](06_gold.py) – airports and the views the map reads.
    """)
    return


if __name__ == "__main__":
    app.run()
