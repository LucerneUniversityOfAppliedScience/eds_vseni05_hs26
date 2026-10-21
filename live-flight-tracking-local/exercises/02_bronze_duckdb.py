import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium", app_title="02 – Bronze in DuckDB")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 02 – Store the message: bronze in DuckDB

    In 01 the answer was gone as soon as the notebook stopped. Now we **keep** every answer – exactly as it came,
    nothing cleaned yet. That raw layer is called **bronze**: if a later step has a bug, we rebuild everything from
    bronze instead of calling the API again.

    [DuckDB](https://duckdb.org) is a database in a single file that runs inside your Python process – no server to
    install, SQL like in Databricks or Snowflake, built for analytical queries
    ([why DuckDB](https://duckdb.org/why_duckdb.html)). We use one file with three **schemas**: `bronze`, `silver`, `gold`.

    This notebook works in **`practice_02.duckdb`**, your playground. Notebooks 03–06 each start from a copy of the
    step before (`practice_03.duckdb`, …), so you can keep several of them open – DuckDB lets only one process write
    a file ([concurrency](https://duckdb.org/docs/stable/connect/concurrency)). One catch: if you go back to an
    earlier notebook and store more there, the next one starts again from that newer copy. The real data later goes
    to `flights.duckdb`, written by `collector.py` and the app.
    """)
    return


@app.cell
def _(mo):
    import json
    import sys
    from datetime import datetime, timezone

    import duckdb
    import pandas as pd

    sys.path.insert(0, str(mo.notebook_dir().parent))
    import flightlab as fl                          # the finished functions of notebook 01 (fetch_states, ...)

    def check(ok, good, bad):
        return mo.callout(mo.md(("✅ " + good) if ok else ("❌ " + bad)), kind="success" if ok else "danger")

    con = fl.practice(2)                            # practice_02.duckdb – continues from the step before
    return check, con, datetime, fl, json, pd, timezone


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## One fresh message

    `fl.fetch_states` is your code from 01 (1 credit). Re-run this cell to get a newer snapshot.
    """)
    return


@app.cell
def _(fl):
    data, credits_left = fl.fetch_states(fl.REGIONS["switzerland"])
    f"snapshot time {data['time']}, {len(data['states'] or [])} aircraft, {credits_left} credits left"
    return credits_left, data


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 2.1 – Schemas and the snapshot table

    Create the three schemas `bronze`, `silver`, `gold` and the table **`bronze.snapshots`** – one row per API answer:

    | column | type | |
    |---|---|---|
    | region | VARCHAR | which bounding box (switzerland, world, ...) |
    | time | BIGINT | OpenSky time of the snapshot (unix seconds) |
    | fetched_at | TIMESTAMP | when we received it |
    | source | VARCHAR | `api` (our own call) or `r2` (class data, notebook 04) |
    | credits_remaining | INTEGER | |
    | aircraft | INTEGER | number of state vectors |
    | payload | JSON | the whole raw answer |

    The **primary key** is `(region, time)`: the same snapshot can arrive twice (we call faster than OpenSky updates,
    or the backfill of notebook 04 brings it again) – the key makes sure we store it only once.

    /// details | Hint
    `CREATE SCHEMA IF NOT EXISTS ...` and `CREATE TABLE IF NOT EXISTS bronze.snapshots (..., PRIMARY KEY (region, time))`.
    `con.execute(sql)` runs one statement.
    ///
    """)
    return


@app.cell
def _(con):
    # TODO 2.1: schemas bronze, silver, gold + table bronze.snapshots with PRIMARY KEY (region, time)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")      # ... silver, gold and the table bronze.snapshots
    return


@app.cell(hide_code=True)
def _(check, con):
    _cols = con.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'bronze' "
                        "AND table_name = 'snapshots' ORDER BY ordinal_position").fetchall()
    _pk = con.execute("SELECT constraint_column_names FROM duckdb_constraints() WHERE schema_name = 'bronze' "
                      "AND table_name = 'snapshots' AND constraint_type = 'PRIMARY KEY'").fetchall()
    check([c for (c,) in _cols] == ["region", "time", "fetched_at", "source", "credits_remaining", "aircraft", "payload"]
          and _pk and sorted(_pk[0][0]) == ["region", "time"],
          "bronze.snapshots exists with the primary key (region, time)",
          "Expected the 7 columns in the order of the table above and PRIMARY KEY (region, time).")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 2.2 – Insert the snapshot – only once

    Write `insert_snapshot(data, region, source, credits)` that inserts **one row** into `bronze.snapshots` and
    returns **True if the row was new, False if it was already there**. Store the raw answer with `json.dumps(data)`.

    /// details | Hint
    `INSERT INTO bronze.snapshots VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING RETURNING time` – the `?` are
    filled from a list (never build SQL with f-strings from data!). `RETURNING` gives back the inserted rows:
    `.fetchall()` is an empty list when nothing was inserted
    ([INSERT docs](https://duckdb.org/docs/current/sql/statements/insert.html)).
    ///
    """)
    return


@app.cell
def _(con, datetime, json, timezone):
    # TODO 2.2: insert_snapshot(data, region, source, credits) -> True if new, False if already there
    def insert_snapshot(data, region, source="api", credits=None):
        return False
    return (insert_snapshot,)


@app.cell(hide_code=True)
def _(check, con, insert_snapshot):
    _fake = {"time": 1, "states": [["abc123", "TEST1   ", "Switzerland", 1, 1, 8.5, 47.4, 1000.0, False, 100.0,
                                    90.0, 0.0, None, 1000.0, "1000", False, 0, 3]]}
    try:
        _first, _second = insert_snapshot(_fake, "test"), insert_snapshot(_fake, "test")
        _n = con.execute("SELECT COUNT(*) FROM bronze.snapshots WHERE region = 'test'").fetchone()[0]
        con.execute("DELETE FROM bronze.snapshots WHERE region = 'test'")
    except Exception as _e:                          # table missing (TODO 2.1) or SQL error
        _first, _second, _n = "error – " + str(_e).splitlines()[0], None, 0
    check(_first is True and _second is False and _n == 1,
          "new snapshot -> True, the same again -> False, stored once",
          f"Got {_first!r} then {_second!r}, {_n} row(s) stored – expected True, False and 1.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 2.3 – One row per aircraft: `bronze.states`

    For SQL we want one row per aircraft and snapshot. Create `bronze.states` (primary key `(region, time, icao24)`)
    and write `insert_states(data, region)` that inserts all state vectors of a snapshot – still raw: metres, m/s,
    callsign only trimmed. A DataFrame makes this one statement: DuckDB can `SELECT * FROM df` directly from a pandas
    variable ([SQL on pandas](https://duckdb.org/docs/current/guides/python/sql_on_pandas.html)).

    /// details | Hint
    Build a list of rows `[region, time, v[0], v[1].strip(), v[2], ...]`, make `df = pd.DataFrame(rows, columns=...)`
    and run `INSERT INTO bronze.states SELECT * FROM df ON CONFLICT DO NOTHING`.
    ///
    """)
    return


@app.cell
def _(con, pd):
    STATE_COLUMNS = ["icao24", "callsign", "origin_country", "time_position", "last_contact", "longitude",
                     "latitude", "baro_altitude", "on_ground", "velocity", "true_track", "vertical_rate",
                     "geo_altitude", "squawk", "category"]
    INDEX = {"icao24": 0, "callsign": 1, "origin_country": 2, "time_position": 3, "last_contact": 4, "longitude": 5,
             "latitude": 6, "baro_altitude": 7, "on_ground": 8, "velocity": 9, "true_track": 10,
             "vertical_rate": 11, "geo_altitude": 13, "squawk": 14, "category": 17}

    # TODO 2.3: table bronze.states + insert_states(data, region)
    def insert_states(data, region):
        pass
    return (insert_states,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 2.4 – Put it together: `store_snapshot`

    `store_snapshot(data, region, source, credits)`: insert the snapshot; **only if it was new**, insert its states.
    Return True/False like `insert_snapshot`. This is the function the collector (03) and the backfill (04) use.
    """)
    return


@app.cell
def _(insert_snapshot, insert_states):
    # TODO 2.4: store_snapshot = insert_snapshot + insert_states (only for new snapshots)
    def store_snapshot(data, region, source="api", credits=None):
        return False
    return (store_snapshot,)


@app.cell(hide_code=True)
def _(check, con, credits_left, data, store_snapshot):
    try:
        _new = store_snapshot(data, "switzerland", "api", int(credits_left) if credits_left else None)
        _again = store_snapshot(data, "switzerland", "api", int(credits_left) if credits_left else None)
        _snap, _states = con.execute(
            "SELECT (SELECT COUNT(*) FROM bronze.snapshots), (SELECT COUNT(*) FROM bronze.states)").fetchone()
        _here = con.execute("SELECT COUNT(*) FROM bronze.states WHERE time = ?", [int(data["time"])]).fetchone()[0]
    except Exception as _e:                          # tables of TODO 2.1 / 2.3 missing
        _new, _again, _snap, _states, _here = "error – " + str(_e).splitlines()[0], None, 0, 0, 0
    _out = check(_again is False and _here == len(data["states"] or []),
          f"Snapshot {data['time']} stored ({'new' if _new else 'was already there'}), the second call was skipped. "
          f"bronze now holds {_snap} snapshots and {_states} aircraft positions.",
          f"store_snapshot returned {_new!r} / {_again!r}; {_here} states of this snapshot in bronze.states, "
          f"expected {len(data['states'] or [])}.")
    stored = _states
    _out
    return (stored,)


@app.cell
def _(con, mo):
    try:
        _table = mo.as_html(con.sql("SELECT region, time, to_timestamp(time) AS snapshot_utc, source, aircraft, "
                                    "credits_remaining FROM bronze.snapshots ORDER BY time DESC LIMIT 10").df())
    except Exception:
        _table = "*(bronze.snapshots does not exist yet – TODO 2.1)*"
    mo.md(f"""
    ## Look into bronze

    {_table}

    Re-run the cell *One fresh message* above a few times (wait ~10 s – anonymous data has a time resolution of
    10 s, with an account 5 s, see [Limitations](https://openskynetwork.github.io/opensky-api/rest.html#limitations))
    and watch the table grow. Re-running too fast can give a `time` that is already stored – and `store_snapshot`
    skips it.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 2.5 – Your first SQL on bronze

    Bronze keeps OpenSky's units: altitude in **metres**, speed in **m/s**
    ([field list](https://openskynetwork.github.io/opensky-api/rest.html#response)).
    Write a query on `bronze.states` for the **newest snapshot** (`time = (SELECT MAX(time) FROM bronze.states)`):
    the **5 highest aircraft in the air**, with `icao24`, `callsign`, `altitude_ft`
    (`baro_altitude × 3.28084`, rounded) and `speed_kmh` (`velocity × 3.6`, rounded). Store it in `highest`.

    /// details | Hint
    `round(baro_altitude * 3.28084)`, `WHERE NOT on_ground AND baro_altitude IS NOT NULL`,
    `ORDER BY altitude_ft DESC LIMIT 5`. In marimo, `mo.sql("...", engine=con)` runs SQL and shows the result.
    ///
    """)
    return


@app.cell
def _(con, mo, stored):
    stored                                                   # run after the snapshot is stored
    # TODO 2.5: highest = the 5 highest airborne aircraft of the newest snapshot (icao24, callsign, altitude_ft, speed_kmh)
    highest = mo.sql("SELECT icao24, callsign, baro_altitude AS altitude_ft, velocity AS speed_kmh FROM bronze.states LIMIT 5", engine=con)
    return (highest,)


@app.cell(hide_code=True)
def _(check, con, highest):
    try:
        _top = con.execute("""SELECT round(MAX(baro_altitude) * 3.28084) FROM bronze.states
                              WHERE time = (SELECT MAX(time) FROM bronze.states) AND NOT on_ground""").fetchone()[0]
        _ok = ({"icao24", "callsign", "altitude_ft", "speed_kmh"} <= set(highest.columns) and len(highest) == 5
               and float(highest["altitude_ft"].iloc[0]) == float(_top)
               and highest["altitude_ft"].is_monotonic_decreasing)
        _msg = (f"Highest right now: {highest['callsign'].iloc[0] or highest['icao24'].iloc[0]} at "
                f"{highest['altitude_ft'].iloc[0]:,.0f} ft, {highest['speed_kmh'].iloc[0]:,.0f} km/h" if _ok else "")
    except Exception as _e:
        _ok, _msg = False, ""
    check(_ok, _msg, "Expected 5 rows (icao24, callsign, altitude_ft, speed_kmh), highest first, in feet and km/h.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Think about it

    - Why is the key `(region, time)` and not `fetched_at`?
    - Why keep the whole JSON in `payload` when `bronze.states` already has every field?
    - `SELECT payload->>'$.time', json_array_length(payload->'$.states') FROM bronze.snapshots` – DuckDB reads JSON
      directly. Try it in a new cell.

    **Next:** [03 – The collector](03_collector.py) – do this every 35 seconds, without burning your credits.
    """)
    return


if __name__ == "__main__":
    app.run()
