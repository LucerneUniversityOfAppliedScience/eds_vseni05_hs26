import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium", app_title="04 – Backfill from R2")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 04 – Backfill: yesterday's data from the class bucket

    Your collector started today. But a producer at your teacher's home has been storing the very same OpenSky
    answers since 19 October 2026 – one gzip-compressed JSON file per snapshot in a private **Cloudflare R2** bucket (S3 API):

    ```
    switzerland/year=2026/month=10/day=19/hour=17/switzerland_1792431642.json.gz     every ~35 s
    world/year=2026/month=10/day=19/hour=17/world_1792431700.json.gz                 every ~6 min, 6,000–13,000 aircraft
    ```

    A **backfill** loads that history into your bronze – with the same `store_snapshot`. Because bronze has the key
    `(region, time)`, it does not matter what came first: live data and backfill **merge**, nothing is stored twice,
    and you can run the backfill as often as you like (*idempotent*).

    You need the **read-only** key from your teacher in `.env`: `R2_ENDPOINT`, `R2_ACCESS_KEY_ID`,
    `R2_SECRET_ACCESS_KEY` (see `.env.template`). It can only read – nobody can delete anything with it.
    """)
    return


@app.cell
def _(mo):
    import gzip
    import json
    import sys
    from datetime import datetime, timedelta, timezone

    import duckdb

    sys.path.insert(0, str(mo.notebook_dir().parent))
    import flightlab as fl

    def check(ok, good, bad):
        return mo.callout(mo.md(("✅ " + good) if ok else ("❌ " + bad)), kind="success" if ok else "danger")

    con = fl.practice(4)                            # practice_04.duckdb – continues from the step before
    fl.init_db(con)
    env = fl.load_env()
    has_r2 = all(env.get(k) for k in ("R2_ENDPOINT", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"))
    mo.callout(mo.md("R2 key found in `.env`." if has_r2 else
                     "**No R2 key in `.env`** – TODO 4.1 works without it, 4.2–4.4 need it. Ask your teacher."),
               kind="info" if has_r2 else "warn")
    return check, con, datetime, env, fl, gzip, has_r2, json, timedelta, timezone


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 4.1 – Which folders?

    Listing a whole bucket is slow and the bucket grows by about 2,700 files a day (per full hour: 103 switzerland
    + 10 world). Thanks to the **partitioning** we only
    list the hours we need. Write `hour_prefixes(region, since, until)` that returns one prefix per hour from `since`
    to `until` (both included, `datetime` in UTC):

    `["switzerland/year=2026/month=10/day=19/hour=22/", "switzerland/year=2026/month=10/day=19/hour=23/", ...]`

    /// details | Hint
    Start at `since.replace(minute=0, second=0, microsecond=0)` and add `timedelta(hours=1)` while `t <= until`.
    `f"{t:%Y}"` formats the year, `%m %d %H` month, day, hour with leading zeros.
    ///
    """)
    return


@app.cell
def _(timedelta):
    # >>> TODO 4.1: hour_prefixes(region, since, until) -> list of "region/year=YYYY/month=MM/day=DD/hour=HH/"
    # ?? def hour_prefixes(region, since, until):
    # ??     return []
    def hour_prefixes(region, since, until):
        t, out = since.replace(minute=0, second=0, microsecond=0), []
        while t <= until:
            out.append(f"{region}/year={t:%Y}/month={t:%m}/day={t:%d}/hour={t:%H}/")
            t += timedelta(hours=1)
        return out
    # <<< TODO 4.1
    return (hour_prefixes,)


@app.cell(hide_code=True)
def _(check, datetime, hour_prefixes):
    _p = hour_prefixes("switzerland", datetime(2026, 10, 19, 22, 40), datetime(2026, 10, 20, 1, 5))
    check(_p == ["switzerland/year=2026/month=10/day=19/hour=22/", "switzerland/year=2026/month=10/day=19/hour=23/",
                 "switzerland/year=2026/month=10/day=20/hour=00/", "switzerland/year=2026/month=10/day=20/hour=01/"],
          "4 hours across midnight: 22, 23, 00, 01",
          f"22:40 to 01:05 should give the 4 hours 22, 23, 00, 01 (crossing midnight) – got {_p}")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 4.2 – List the files of the last 3 hours

    `fl.r2_client(env)` returns an S3 client (boto3) and the bucket name. For every prefix, list the objects with the
    paginator `list_objects_v2` (one page holds at most 1,000 keys –
    [ListObjectsV2](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html); R2 implements the same
    [S3 API](https://developers.cloudflare.com/r2/api/s3/api/)) and collect the keys that end with `.json.gz`
    in `keys` (sorted).

    /// details | Hint
    ```python
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            obj["Key"]
    ```
    ///
    """)
    return


@app.cell
def _(datetime, env, fl, has_r2, hour_prefixes, mo, timedelta, timezone):
    mo.stop(not has_r2, mo.md("*needs the R2 key*"))
    s3, bucket = fl.r2_client(env)
    until = datetime.now(timezone.utc).replace(tzinfo=None)
    since = until - timedelta(hours=3)

    # >>> TODO 4.2: keys = sorted list of all *.json.gz keys of region switzerland between since and until
    # ?? keys = []
    keys = []
    for _prefix in hour_prefixes("switzerland", since, until):
        for _page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=_prefix):
            keys += [o["Key"] for o in _page.get("Contents", []) if o["Key"].endswith(".json.gz")]
    keys = sorted(keys)
    # <<< TODO 4.2
    mo.md(f"**{len(keys)} files** – first: `{keys[0] if keys else '-'}`, last: `{keys[-1] if keys else '-'}`")
    return bucket, keys, s3


@app.cell(hide_code=True)
def _(check, keys):
    check(len(keys) > 300 and all(k.startswith("switzerland/") and k.endswith(".json.gz") for k in keys),
          f"{len(keys)} snapshots in 3 hours (~1 every 35 s)",
          f"Expected several hundred switzerland/... .json.gz keys – got {len(keys)}.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 4.3 – Read one file – in memory

    Download the **last** key with `s3.get_object(Bucket=bucket, Key=key)["Body"].read()`, unpack it with
    `gzip.decompress` and parse it with `json.loads` into `snapshot`. No file on your disk – the bytes go straight
    into Python. It is the same structure as the API answer of notebook 01.
    """)
    return


@app.cell
def _(bucket, gzip, json, keys, s3):
    # >>> TODO 4.3: snapshot = the last key, downloaded, unpacked and parsed
    # ?? snapshot = {}
    snapshot = json.loads(gzip.decompress(s3.get_object(Bucket=bucket, Key=keys[-1])["Body"].read()))
    # <<< TODO 4.3
    {k: (v if k != "states" else f"{len(v)} state vectors") for k, v in snapshot.items()}
    return (snapshot,)


@app.cell(hide_code=True)
def _(check, keys, snapshot):
    check(bool(keys) and isinstance(snapshot.get("states"), list) and str(snapshot.get("time")) in keys[-1],
          f"snapshot {snapshot.get('time')} with {len(snapshot.get('states') or [])} aircraft – the file name "
          f"carries the same time",
          "`snapshot` should be the parsed JSON with `time` and `states` (the time is also in the file name).")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 4.4 – Backfill with merge

    Write `backfill(keys)`: for every key read the snapshot and store it with
    `fl.store_snapshot(con, snapshot, "switzerland", "r2", snapshot.get("credits_remaining"))`. Return the number of
    **new** snapshots.

    Make it fast: the OpenSky time is in the file name (`switzerland_<time>.json.gz`). Read the times that are
    already in `bronze.snapshots` first and **skip those keys without downloading**.

    /// details | Hint
    `have = {t for (t,) in con.execute("SELECT time FROM bronze.snapshots WHERE region = 'switzerland'").fetchall()}`
    and `int(key.rsplit("_", 1)[1].split(".")[0])` for the time in a key.
    ///
    """)
    return


@app.cell
def _(bucket, con, fl, gzip, json, s3):
    # >>> TODO 4.4: backfill(keys) -> number of new snapshots (skip known times without downloading)
    # ?? def backfill(keys):
    # ??     return 0
    def backfill(keys):
        have = {t for (t,) in con.execute(
            "SELECT time FROM bronze.snapshots WHERE region = 'switzerland'").fetchall()}
        new = 0
        for key in keys:
            if int(key.rsplit("_", 1)[1].split(".")[0]) in have:
                continue
            snap = json.loads(gzip.decompress(s3.get_object(Bucket=bucket, Key=key)["Body"].read()))
            new += fl.store_snapshot(con, snap, "switzerland", "r2", snap.get("credits_remaining"))
        return new
    # <<< TODO 4.4
    return (backfill,)


@app.cell(hide_code=True)
def _(backfill, check, con, keys, mo):
    _before = con.execute("SELECT COUNT(*) FROM bronze.snapshots").fetchone()[0]
    with mo.status.spinner(title=f"backfilling {len(keys)} files ..."):
        _first = backfill(keys)
        _second = backfill(keys)
    _after = con.execute("SELECT COUNT(*) FROM bronze.snapshots").fetchone()[0]
    mo.vstack([
        check(_first > 0 and _second == 0 and _after == _before + _first,
              f"first run: {_first} new snapshots, second run: {_second} – idempotent. "
              f"bronze: {_before} -> {_after} snapshots",
              f"first run {_first} new, second run {_second} (should be 0), bronze {_before} -> {_after}."),
        con.sql("SELECT source, COUNT(*) AS snapshots, MIN(to_timestamp(time)) AS first_utc, "
                "MAX(to_timestamp(time)) AS last_utc FROM bronze.snapshots GROUP BY source").df(),
    ])
    backfilled = _after
    return (backfilled,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## SQL on bronze: what did you just load?

    Bronze now holds hours of raw data. Four questions, each one SQL query – write them in the `mo.sql` cells
    (`time` is unix seconds: `to_timestamp(time)` turns it into a timestamp, the session runs in UTC).

    ### TODO 4.5 – Coverage per hour

    Per **hour (UTC)** from `bronze.snapshots`: the number of snapshots and the average number of aircraft per
    snapshot (rounded). Columns `hour`, `snapshots`, `avg_aircraft`, oldest hour first. Store it in `per_hour`.
    At one snapshot every ~35 s, a complete hour has about 100 – fewer means a gap.

    /// details | Hint
    `time_bucket(INTERVAL 1 HOUR, to_timestamp(time)) AS hour`, `COUNT(*)`, `round(AVG(aircraft))`, `GROUP BY hour`
    ([time_bucket](https://duckdb.org/docs/stable/sql/functions/date)).
    ///
    """)
    return


@app.cell
def _(backfilled, con, mo):
    backfilled                                               # run after the backfill
    # >>> TODO 4.5: per_hour = hour, snapshots, avg_aircraft (oldest hour first)
    # ?? per_hour = mo.sql("SELECT NULL::TIMESTAMPTZ AS hour, 0 AS snapshots, 0 AS avg_aircraft", engine=con)
    per_hour = mo.sql(
        """
        SELECT time_bucket(INTERVAL 1 HOUR, to_timestamp(time)) AS hour,
               COUNT(*)                                         AS snapshots,
               round(AVG(aircraft))                             AS avg_aircraft
        FROM bronze.snapshots
        GROUP BY hour
        ORDER BY hour
        """,
        engine=con,
    )
    # <<< TODO 4.5
    return (per_hour,)


@app.cell(hide_code=True)
def _(check, con, per_hour):
    try:
        _total = con.execute("SELECT COUNT(*) FROM bronze.snapshots").fetchone()[0]
        _ok = ({"hour", "snapshots", "avg_aircraft"} <= set(per_hour.columns) and len(per_hour) > 1
               and int(per_hour["snapshots"].sum()) == _total and per_hour["hour"].is_monotonic_increasing)
        _gaps = int((per_hour["snapshots"] < 90).sum()) if _ok else 0
    except Exception:
        _ok, _total, _gaps = False, 0, 0
    check(_ok, f"{len(per_hour)} hours, {_total} snapshots – {_gaps} hour(s) with fewer than 90 (first and last "
               f"hour are usually incomplete)" if _ok else "",
          "Expected one row per hour with snapshots and avg_aircraft; the snapshots must add up to all of bronze.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### TODO 4.6 – Who flies over Switzerland?

    From `bronze.states`: the 10 **registration countries** (`origin_country`) with the most **different aircraft**
    (`COUNT(DISTINCT icao24)`). Columns `origin_country`, `aircraft`, most first. Store it in `countries`.
    """)
    return


@app.cell
def _(backfilled, con, mo):
    backfilled
    # >>> TODO 4.6: countries = origin_country, aircraft (top 10 by distinct icao24)
    # ?? countries = mo.sql("SELECT origin_country, COUNT(*) AS aircraft FROM bronze.states GROUP BY ALL LIMIT 10", engine=con)
    countries = mo.sql(
        """
        SELECT origin_country, COUNT(DISTINCT icao24) AS aircraft
        FROM bronze.states
        GROUP BY origin_country
        ORDER BY aircraft DESC
        LIMIT 10
        """,
        engine=con,
    )
    # <<< TODO 4.6
    return (countries,)


@app.cell(hide_code=True)
def _(check, con, countries):
    try:
        _top = con.execute("""SELECT origin_country, COUNT(DISTINCT icao24) AS n FROM bronze.states
                              GROUP BY ALL ORDER BY n DESC LIMIT 1""").fetchone()
        _ok = ({"origin_country", "aircraft"} <= set(countries.columns) and len(countries) == 10
               and countries["origin_country"].iloc[0] == _top[0] and int(countries["aircraft"].iloc[0]) == _top[1])
    except Exception:
        _ok, _top = False, ("", 0)
    check(_ok, f"Most aircraft: {_top[0]} ({_top[1]}) – the country of registration, not the airline's home or the "
               f"route" if _ok else "",
          "Expected 10 rows origin_country, aircraft – counted as different aircraft (DISTINCT icao24), most first.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### TODO 4.7 – Raw data is messy

    One row that counts the problems in `bronze.states`: `rows` (all), `no_position` (`latitude IS NULL`),
    `no_altitude` (`baro_altitude IS NULL`), `on_ground`, `no_callsign` (`NULL` or empty) and `old_position` –
    rows whose position is more than 60 s older than the snapshot (`time - time_position > 60`: OpenSky keeps
    sending the last known position, see `time_position` in the
    [field list](https://openskynetwork.github.io/opensky-api/rest.html#response)). Store it in `quality`.

    That is why there is a **silver** layer (notebook 05): it drops rows without position and turns empty callsigns
    into NULL.

    /// details | Hint
    `COUNT(*) FILTER (WHERE latitude IS NULL) AS no_position, …` – one `FILTER` per column
    ([aggregates](https://duckdb.org/docs/stable/sql/functions/aggregates)).
    ///
    """)
    return


@app.cell
def _(backfilled, con, mo):
    backfilled
    # >>> TODO 4.7: quality = one row: rows, no_position, no_altitude, on_ground, no_callsign, old_position
    # ?? quality = mo.sql("SELECT COUNT(*) AS rows FROM bronze.states", engine=con)
    quality = mo.sql(
        """
        SELECT COUNT(*)                                                        AS rows,
               COUNT(*) FILTER (WHERE latitude IS NULL)                        AS no_position,
               COUNT(*) FILTER (WHERE baro_altitude IS NULL)                   AS no_altitude,
               COUNT(*) FILTER (WHERE on_ground)                               AS on_ground,
               COUNT(*) FILTER (WHERE callsign IS NULL OR callsign = '')        AS no_callsign,
               COUNT(*) FILTER (WHERE time - time_position > 60)                AS old_position
        FROM bronze.states
        """,
        engine=con,
    )
    # <<< TODO 4.7
    return (quality,)


@app.cell(hide_code=True)
def _(check, con, quality):
    _cols = ["rows", "no_position", "no_altitude", "on_ground", "no_callsign", "old_position"]
    try:
        _want = con.execute("""SELECT COUNT(*), COUNT(*) FILTER (WHERE latitude IS NULL),
                                      COUNT(*) FILTER (WHERE baro_altitude IS NULL), COUNT(*) FILTER (WHERE on_ground),
                                      COUNT(*) FILTER (WHERE callsign IS NULL OR callsign = ''),
                                      COUNT(*) FILTER (WHERE time - time_position > 60)
                               FROM bronze.states""").fetchone()
        _got = tuple(int(quality[c].iloc[0]) for c in _cols)
        _ok = _got == tuple(int(v) for v in _want)
    except Exception:
        _ok, _want = False, (1, 0, 0, 0, 0, 0)
    check(_ok, f"{_want[0]:,} rows: {_want[1]:,} without position, {_want[2]:,} without altitude, {_want[3]:,} on the "
               f"ground, {_want[4]:,} without callsign, {_want[5]:,} with a position older than 60 s" if _ok else "",
          "Expected one row with " + ", ".join(_cols) + " – each a COUNT(*) FILTER (WHERE …).")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### TODO 4.8 – Who stayed longest?

    Per aircraft (`icao24`): the callsign (any non-empty one), the first and last time it was seen, the
    **minutes** in between and the number of snapshots. The 10 with the most minutes, as `longest` with the
    columns `icao24`, `callsign`, `first_seen`, `last_seen`, `minutes`, `snapshots`.

    /// details | Hint
    `ANY_VALUE(NULLIF(callsign, ''))`, `to_timestamp(MIN(time))`, `(MAX(time) - MIN(time)) / 60.0`,
    `GROUP BY icao24`. Long stays are often aircraft **on the ground** at an airport – check `on_ground` for the top
    ones.
    ///
    """)
    return


@app.cell
def _(backfilled, con, mo):
    backfilled
    # >>> TODO 4.8: longest = icao24, callsign, first_seen, last_seen, minutes, snapshots (top 10 by minutes)
    # ?? longest = mo.sql("SELECT icao24, NULL AS callsign, NULL AS first_seen, NULL AS last_seen, 0 AS minutes, 0 AS snapshots FROM bronze.states LIMIT 10", engine=con)
    longest = mo.sql(
        """
        SELECT icao24,
               ANY_VALUE(NULLIF(callsign, ''))        AS callsign,
               to_timestamp(MIN(time))                AS first_seen,
               to_timestamp(MAX(time))                AS last_seen,
               round((MAX(time) - MIN(time)) / 60.0)  AS minutes,
               COUNT(*)                               AS snapshots
        FROM bronze.states
        GROUP BY icao24
        ORDER BY minutes DESC
        LIMIT 10
        """,
        engine=con,
    )
    # <<< TODO 4.8
    return (longest,)


@app.cell(hide_code=True)
def _(check, con, longest):
    try:
        _max = con.execute("""SELECT round(MAX(d) / 60.0) FROM (
                                SELECT MAX(time) - MIN(time) AS d FROM bronze.states GROUP BY icao24)""").fetchone()[0]
        _ok = ({"icao24", "callsign", "first_seen", "last_seen", "minutes", "snapshots"} <= set(longest.columns)
               and len(longest) == 10 and float(longest["minutes"].iloc[0]) == float(_max))
        _ground = con.execute("SELECT AVG(on_ground::INT) FROM bronze.states WHERE icao24 = ?",
                              [longest["icao24"].iloc[0]]).fetchone()[0] if _ok else 0
    except Exception:
        _ok, _max, _ground = False, 0, 0
    check(_ok, f"Longest: {longest['callsign'].iloc[0] or longest['icao24'].iloc[0]} – {_max:,.0f} minutes, on the "
               f"ground in {100 * (_ground or 0):.0f} % of its rows" if _ok else "",
          "Expected 10 rows with icao24, callsign, first_seen, last_seen, minutes, snapshots – most minutes first.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Think about it

    - Your live snapshots (source `api`) and the bucket's (source `r2`) can have the **same** `time` – when you and
      the producer happened to get the same snapshot. Which one wins? Does it matter?
    - Why skip by file name *before* downloading instead of letting `ON CONFLICT DO NOTHING` sort it out?
    - The world files: `fl.backfill(con, "world", since=...)` – up to 13,000 aircraft every 6 minutes. How many rows per day?

    For the real database there is a ready-made version: `uv run collector.py --backfill-hours 48` (backfill first,
    then live).

    **Next:** [05 – Silver](05_silver.py) – from raw lists to clean positions, and a trap a backfill sets for you.
    """)
    return


if __name__ == "__main__":
    app.run()
