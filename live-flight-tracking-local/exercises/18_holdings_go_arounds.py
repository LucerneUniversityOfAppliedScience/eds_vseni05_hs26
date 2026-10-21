import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium", app_title="18 – Holdings and go-arounds")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 18 – Holding patterns and go-arounds

    Two things approach control does when an airport cannot take an aircraft right now:

    - **Holding:** the aircraft flies a racetrack over a fixed point and waits: a 180° turn, a straight outbound leg
      of **1 minute** (1½ minutes above 14,000 ft), another 180° turn and the inbound leg back to the fix. Standard
      holdings turn right, at 25° bank or 3° per second (whichever is less), at most 230 kt up to 14,000 ft
      ([ICAO Doc 8168 Vol. I, Part II, Section 6](https://www.caa.gov.lv/lv/media/1905/download), 2.1.1–2.3.1).
      Zurich's holdings are used between 7,000 ft and FL240 (chart below), several aircraft stacked above each other.
    - **Go-around:** the crew does not continue an approach or a landing
      ([SKYbrary](https://skybrary.aero/articles/go-around)) – e.g. runway not clear, unstable approach – and climbs
      away. An ADS-B study of 20 large European airports (2019–2023) found 1.5 to 6 go-arounds per 1,000 landings;
      Zurich: 1,031 in 284,497 landings, about 3.6 per 1,000
      ([Figuet et al. 2023, Journal of Open Aviation Science](https://journals.open.tudelft.nl/joas/article/view/7219), Table 1).

    Both are visible in the positions alone: a holding is **heading turning through 360° at constant altitude**, a
    go-around is **down, then up again without touching the ground**. You build simple versions, compare them with
    the finished modules in `enrichment/` – and ask what the arriving aircraft did during the **Skyguide outage** of
    20 October 2026 (no departures at Zurich from 14:15 for about an hour, according to
    [Blick](https://www.blick.ch/schweiz/zuerich/am-flughafen-zuerich-koennen-keine-flieger-mehr-starten/ds1e9ml);
    our analysis window `OUTAGE` is 14:10–15:10 Swiss time).
    """)
    return


@app.cell
def _(mo):
    import sys

    sys.path.insert(0, str(mo.notebook_dir().parent))
    import numpy as np
    import pandas as pd
    import plotly.express as px

    import flightlab as fl
    from enrichment import go_arounds, holdings

    def check(ok, good, bad):
        return mo.callout(mo.md(("✅ " + good) if ok else ("❌ " + bad)), kind="success" if ok else "danger")

    con = fl.snapshot()
    fl.ensure(con, "metar", "flight_routes")                     # tables the app computes – here if you only ran collector.py
    # the afternoon of the outage (UTC; Swiss summer time = UTC + 2). Change it to look at another window.
    T0, T1 = pd.Timestamp("2026-10-20 10:00"), pd.Timestamp("2026-10-20 16:00")
    OUTAGE = (pd.Timestamp("2026-10-20 12:10"), pd.Timestamp("2026-10-20 13:10"))      # 14:10–15:10 Swiss time
    LSZH = (47.4581, 8.5481, 1417)      # lat, lon, elevation ft: OurAirports reference point (vACC chart: elev. 1417)
    _n = con.execute("SELECT COUNT(*) FROM bronze.snapshots WHERE region = 'switzerland' AND time BETWEEN ? AND ?",
                     [int(T0.timestamp()), int(T1.timestamp())]).fetchone()[0]
    mo.md(f"Window **{T0:%d.%m. %H:%M}–{T1:%H:%M} UTC**: {_n} Swiss snapshots in your copy")
    return LSZH, OUTAGE, T0, T1, check, con, fl, go_arounds, holdings, np, pd, px


@app.cell
def _(T0, T1, con, mo):
    _have = con.execute("""SELECT COUNT(*) FROM bronze.snapshots WHERE region = 'switzerland'
                           AND time BETWEEN ? AND ?""", [int(T0.timestamp()), int(T1.timestamp())]).fetchone()[0]
    load_r2 = mo.ui.run_button(label="Load the window from R2")
    mo.hstack([mo.md(f"Snapshots in the window: **{_have}** of about {int((T1 - T0).total_seconds() / 35)}"),
               load_r2], justify="start")
    return (load_r2,)


@app.cell
def _(T0, T1, con, fl, load_r2, mo):
    if load_r2.value:
        with mo.status.spinner(title="backfilling the window from R2 ..."):
            fl.backfill(con, "switzerland", since=T0.to_pydatetime(), until=T1.to_pydatetime(), env=fl.load_env(),
                        log=lambda *_: None)
            fl.build_silver(con)
    r2_loaded = load_r2.value
    return (r2_loaded,)


@app.cell
def _(LSZH, T0, T1, con, r2_loaded):
    r2_loaded                                                   # rerun after the backfill
    # all positions within 80 km of Zurich in the window, with the distance to the airport
    near = con.execute(f"""
        SELECT * FROM (
        SELECT snapshot_time, icao24, callsign, latitude, longitude, on_ground, altitude_ft, speed_kmh, heading,
               vertical_rate, flight_phase, category,
               2 * 6371 * ASIN(SQRT(POWER(SIN(RADIANS(latitude - {LSZH[0]}) / 2), 2)
                 + COS(RADIANS({LSZH[0]})) * COS(RADIANS(latitude))
                 * POWER(SIN(RADIANS(longitude - {LSZH[1]}) / 2), 2))) AS dist_km
        FROM silver.flights
        WHERE region = 'switzerland' AND snapshot_time BETWEEN ? AND ?)
        WHERE dist_km <= 80
        ORDER BY icao24, snapshot_time""", [T0.to_pydatetime(), T1.to_pydatetime()]).df()
    near.head()
    return (near,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 18.1 – How far has an aircraft turned?

    A holding is two 180° turns per lap. Add up the heading changes between consecutive positions:
    `cumulative_turn(headings, min_step=2)` returns the **sum of the absolute changes** in degrees.

    Two details:

    - **Wrap-around:** from 355° to 5° is a turn of +10°, not −350°. `(b − a + 180) % 360 − 180` gives the signed
      change in [−180, 180).
    - **Noise:** the heading is rounded to whole degrees and wobbles by 1° on a straight line. Ignore steps smaller
      than `min_step` – otherwise a long straight flight adds up to a full circle.

    /// details | Hint
    `np.diff(np.asarray(headings, dtype=float))` gives the raw steps; wrap them, keep `np.abs(steps) >= min_step`,
    sum the absolute values and return a `float`.
    ///
    """)
    return


@app.cell
def _(np):
    # TODO 18.1: cumulative_turn(headings, min_step) = sum of |heading changes| in degrees (wrapped, noise ignored)
    def cumulative_turn(headings, min_step=2.0):
        return 0.0
    return (cumulative_turn,)


@app.cell(hide_code=True)
def _(check, cumulative_turn, holdings):
    try:
        _cases = {"full circle 0, 90, 180, 270, 0": ([0, 90, 180, 270, 0], 360),
                  "355° → 5°": ([355, 5], 10), "350 → 10 → 350": ([350, 10, 350], 40),
                  "straight with 1° noise": ([100, 101, 100, 101, 100, 101] * 20, 0)}
        _got = {k: cumulative_turn(h) for k, (h, _) in _cases.items()}
        _bad = [f"{k}: {_got[k]:.0f}° (expected {v})" for k, (_, v) in _cases.items() if abs(_got[k] - v) > 0.5]
        _same = abs(cumulative_turn([10, 50, 120, 200, 300, 20, 90]) - holdings.turn_stats([10, 50, 120, 200, 300, 20, 90])[0]) < 0.5
        _ok = not _bad and _same
    except Exception as _e:
        _ok, _bad = False, [f"{type(_e).__name__}: {_e}"]
    check(_ok, "all four test tracks right – and the same result as `holdings.turn_stats` of the finished module",
          "Not yet: " + "; ".join(_bad))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Zurich's holding fixes

    The official source, the Swiss AIP, is not open: the eAIP is a paid subscription
    ([skybriefing](https://www.skybriefing.com/prices/evfr-manual)). So the values below are **not official**:

    - holdings, inbound courses (magnetic), leg time 1 min and levels 7,000 ft – FL240: from an approach chart
      **for flight simulation** ([vACC Switzerland, LSZH RNAV1 transitions RWY 16, revision 20 Mar 2025](https://vacc.ch/file/547),
      "valid for flight simulation use only"),
    - coordinates: from [OpenNav](https://opennav.com/waypoint/CH/AMIKI), a third-party waypoint database that does
      not name its source. Positions read off the vACC chart lie 4–6 km away from these,
    - `serves`: our own reading of where each fix lies relative to the airport – the chart does not say which
      arrivals use which holding.

    Good enough to say "near which fix" – not for anything else. Check them against the AIP if you have access.
    """)
    return


@app.cell
def _(pd):
    FIXES = pd.DataFrame({
        "fix": ["RILAX", "AMIKI", "GIPOL"],
        "latitude": [47.9428, 47.5739, 47.5053],           # OpenNav (not official), see the text above
        "longitude": [8.5136, 9.0375, 8.0408],
        "inbound_course": [188, 274, 77],                  # magnetic, from the vACC chart
        "serves": ["arrivals from the north", "arrivals from the east", "arrivals from the south and west"],
    })
    FIXES
    return (FIXES,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 18.2 – A simple holding detector

    Write `is_holding(track)` for the positions of **one aircraft in a 20-minute window** (a DataFrame sorted by
    time with `heading`, `altitude_ft`, `speed_kmh`, `latitude`, `longitude`). It is a holding when

    - there are at least 8 positions,
    - the aircraft turned through **at least 360°** (`cumulative_turn`),
    - at roughly **constant altitude**: max − min below 2,000 ft (our tolerance: ATC may clear a holding aircraft
      to a lower level while it waits),
    - in a **small area**: every position within 15 km of the mean position (at 230 kt and 10,000 ft without wind,
      the racetrack of Doc 8168 is about 17 × 8 km: 1-minute legs of 8 km plus turns of 4 km radius at 25° bank –
      our computation),
    - at **airliner speed**: mean speed at least 250 km/h – our threshold to keep out helicopters, gliders and
      light aircraft circling.

    The cell below it slides the 20-minute window over the afternoon in steps of 10 minutes – only positions between
    6,500 ft and 30,000 ft, where Zurich's stacks are – and collects the hits in `my_holdings`, with the nearest fix.
    """)
    return


@app.cell
def _(cumulative_turn, np):
    # TODO 18.2: is_holding(track) -> True when >= 8 points, >= 360° turned, alt range < 2000 ft, radius <= 15 km, >= 250 km/h
    def is_holding(track):
        return False
    return (is_holding,)


@app.cell
def _(FIXES, T0, T1, cumulative_turn, holdings, is_holding, near, np, pd):
    def racetrack(minutes=16, lat=47.94, lon=8.51, alt=9000.0):
        """A synthetic holding: 1 min straight, 180° right turn, 1 min back, 180° turn – one point every 35 s."""
        t = np.arange(0, minutes * 60, 35)
        phase = (t % 240) / 240                              # 4-minute lap
        heading = np.where(phase < 0.25, 188, np.where(phase < 0.5, 188 + (phase - 0.25) * 4 * 180,
                           np.where(phase < 0.75, 8, 8 + (phase - 0.75) * 4 * 180))) % 360
        ang = 2 * np.pi * phase
        return pd.DataFrame({"snapshot_time": pd.Timestamp("2026-10-20 12:00") + pd.to_timedelta(t, unit="s"),
                             "icao24": "test01", "callsign": "TEST1", "latitude": lat + 0.03 * np.cos(ang),
                             "longitude": lon + 0.05 * np.sin(ang), "altitude_ft": alt + 50 * np.sin(ang),
                             "speed_kmh": 420.0, "heading": heading, "on_ground": False, "category": 0})

    # Zurich's stacks start at 7,000 ft (chart above) – lower circles are light aircraft, not holdings
    _air = near[(~near["on_ground"].fillna(False).astype(bool)) & near["altitude_ft"].between(6500, 30000)
                & near["heading"].notna()]
    _rows = []
    for _start in pd.date_range(T0, T1, freq="10min"):
        _w = _air[(_air["snapshot_time"] >= _start) & (_air["snapshot_time"] < _start + pd.Timedelta(minutes=20))]
        for _icao, _t in _w.groupby("icao24"):
            try:
                _hit = is_holding(_t)
            except Exception:
                _hit = False
            if _hit:
                _d = np.hypot((FIXES["longitude"] - _t["longitude"].mean()) * 75.1,
                              (FIXES["latitude"] - _t["latitude"].mean()) * 111.2)
                _rows.append({"icao24": _icao, "callsign": _t["callsign"].dropna().iloc[-1] if _t["callsign"].notna().any() else None,
                              "first_seen": _t["snapshot_time"].min(), "last_seen": _t["snapshot_time"].max(),
                              "laps": round(cumulative_turn(_t["heading"]) / 360, 1),
                              "altitude_ft": int(_t["altitude_ft"].median()), "nearest_fix": FIXES["fix"].iloc[int(_d.argmin())],
                              "fix_km": round(float(_d.min()), 1)})
    my_holdings = (pd.DataFrame(_rows, columns=["icao24", "callsign", "first_seen", "last_seen", "laps", "altitude_ft",
                                                "nearest_fix", "fix_km"])
                   .sort_values("laps", ascending=False).drop_duplicates("icao24").sort_values("first_seen"))
    my_holdings
    return my_holdings, racetrack


@app.cell(hide_code=True)
def _(check, is_holding, my_holdings, racetrack):
    try:
        _hold = racetrack()
        _straight = _hold.assign(heading=188.0)
        _climb = _hold.assign(altitude_ft=_hold["altitude_ft"] + range(0, 300 * len(_hold), 300))
        _slow = _hold.assign(speed_kmh=150.0)
        _res = {"racetrack": is_holding(_hold), "straight": is_holding(_straight), "climbing circle": is_holding(_climb),
                "slow circle": is_holding(_slow), "too short": is_holding(_hold.head(5))}
        _ok = _res == {"racetrack": True, "straight": False, "climbing circle": False, "slow circle": False,
                       "too short": False}
    except Exception as _e:
        _ok, _res = False, f"{type(_e).__name__}: {_e}"
    check(_ok, f"synthetic racetrack found, straight / climbing / slow / short tracks rejected. Real data: "
               f"**{len(my_holdings)} holding(s)** near Zurich in the window",
          f"Test tracks: {_res} – expected only the racetrack to be True.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 18.3 – A simple go-around detector

    Write `go_around(track)` for **all positions of one aircraft** in the window (sorted by time, with `dist_km` to
    LSZH, `altitude_ft`, `on_ground`). Return a dict `{"icao24", "callsign", "lowest_at", "lowest_ft", "climb_ft"}`
    for a go-around, otherwise `None`:

    1. **low point:** the lowest airborne position within **8 km** of the airport and below **2,500 ft above the
       airport** (elevation 1,417 ft),
    2. it **came down** to it: in the 5 minutes before, it was at least 300 ft higher and **not on the ground**
       (this excludes departures),
    3. it **climbs again**: in the 5 minutes after – but only *before* any ground contact – it gains more than
       **500 ft**.

    /// details | Hint
    `low = track[~on_ground & (dist_km <= 8) & (altitude_ft - 1417 < 2500)]`, the low point
    `lo = low.loc[low["altitude_ft"].idxmin()]`, then two time windows `before` and `after` around
    `lo["snapshot_time"]`. Cut `after` at the first `on_ground` row.
    ///
    """)
    return


@app.cell
def _(LSZH, pd):
    # TODO 18.3: go_around(track) -> dict(icao24, callsign, lowest_at, lowest_ft, climb_ft) or None
    def go_around(track, elevation_ft=LSZH[2]):
        return None
    return (go_around,)


@app.cell
def _(go_around, near, pd):
    _found = []
    for _, _t in near[near["dist_km"] <= 30].groupby("icao24"):
        try:
            _r = go_around(_t)
        except Exception:
            _r = None
        if _r:
            _found.append(_r)
    my_go_arounds = pd.DataFrame(_found, columns=["icao24", "callsign", "lowest_at", "lowest_ft", "climb_ft"])
    my_go_arounds
    return (my_go_arounds,)


@app.cell(hide_code=True)
def _(LSZH, check, go_around, my_go_arounds, np, pd):
    def _profile(alts, ground_at=None):
        t = pd.Timestamp("2026-10-20 12:00") + pd.to_timedelta(np.arange(len(alts)) * 35, unit="s")
        g = [ground_at is not None and i >= ground_at for i in range(len(alts))]
        d = np.linspace(20, 0, len(alts))
        return pd.DataFrame({"snapshot_time": t, "icao24": "test02", "callsign": "TEST2", "altitude_ft": alts,
                             "on_ground": g, "dist_km": np.abs(d - d[int(np.argmin(alts))]) + 2})

    try:
        _ga = _profile([6000, 5000, 4200, 3500, 2900, 2400, 2300, 3000, 3800, 4500, 5000, 5000])
        _land = _profile([6000, 5000, 4200, 3500, 2900, 2200, 1700, 1450, 1450, 1450], ground_at=7)
        _dep = _profile([1450, 1450, 1900, 2600, 3500, 4500, 5500, 6500], ground_at=None).assign(
            on_ground=[True, True] + [False] * 6)
        _res = [go_around(_ga), go_around(_land), go_around(_dep)]
        _ok = (_res[0] is not None and _res[0]["climb_ft"] > 500 and _res[1] is None and _res[2] is None)
    except Exception as _e:
        _ok, _res = False, [f"{type(_e).__name__}: {_e}"]
    check(_ok, f"test go-around found (low point {_res[0]['lowest_ft'] if _ok else 0} ft, climb "
               f"{_res[0]['climb_ft'] if _ok else 0} ft), landing and departure ignored. Real data: "
               f"**{len(my_go_arounds)} go-around(s)** at LSZH in the window ({LSZH[2]:,} ft elevation)",
          f"Test profiles (go-around, landing, departure) gave {_res} – expected a dict, None, None.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Compare with the finished modules

    `enrichment/holdings.py` and `enrichment/go_arounds.py` do the same with more care: holdings near *any* large
    airport, left/right turn direction, laps; go-arounds at every airport, with heights corrected by the METAR
    pressure (QNH). In the app, the jobs `holdings` and `go_arounds` only look at the **last 20 / 60 minutes before
    the newest snapshot** (`LOOKBACK` in the module) – for an event in the past we call the module functions
    directly on our window instead of `jobs.run_job`.
    """)
    return


@app.cell
def _(T0, T1, con, go_arounds, holdings, my_go_arounds, my_holdings, near, pd):
    _airports = con.execute("SELECT * FROM silver.airports").df()
    _air = near[(~near["on_ground"].fillna(False).astype(bool)) & near["altitude_ft"].between(1000, 30000)]
    _mod_h = [holdings.detect_holdings(_air[(_air["snapshot_time"] >= _s)
                                            & (_air["snapshot_time"] < _s + pd.Timedelta(minutes=20))], _airports)
              for _s in pd.date_range(T0, T1, freq="10min")]
    module_holdings = (pd.concat(_mod_h).drop_duplicates("icao24") if any(len(_h) for _h in _mod_h)
                       else pd.DataFrame(columns=holdings.COLUMNS))
    _metar = con.execute("SELECT * FROM silver.metar").df()
    module_go_arounds = go_arounds.detect_go_arounds(
        near[near["on_ground"].fillna(False).astype(bool) | (near["altitude_ft"] < 10000)], _airports, _metar)
    _zrh = module_go_arounds[module_go_arounds["airport_icao"] == "LSZH"]
    comparison = pd.DataFrame({
        "detector": ["holdings (80 km around LSZH)", "go-arounds at LSZH"],
        "mine": [len(my_holdings), len(my_go_arounds)],
        "module": [len(module_holdings), len(_zrh)],
        "both": [len(set(my_holdings["icao24"]) & set(module_holdings["icao24"])),
                 len(set(my_go_arounds["icao24"]) & set(_zrh["icao24"]))]})
    comparison
    return comparison, module_go_arounds, module_holdings


@app.cell
def _(LSZH, mo, module_go_arounds, near, pd, px):
    _zrh = module_go_arounds[module_go_arounds["airport_icao"] == "LSZH"]
    if len(_zrh):
        _ga = _zrh.iloc[0]
        _t = near[(near["icao24"] == _ga["icao24"]) & near["snapshot_time"].between(
            _ga["lowest_at"] - pd.Timedelta(minutes=12), _ga["climb_until"] + pd.Timedelta(minutes=12))]
        _out = mo.vstack([mo.md(f"### {_ga['callsign']}: go-around at {_ga['lowest_at']:%H:%M} UTC, lowest "
                                f"{_ga['min_height_ft']:,.0f} ft above the airport"),
                          px.line(_t, x="snapshot_time", y="altitude_ft", markers=True, height=260,
                                  labels={"snapshot_time": "UTC", "altitude_ft": "altitude (ft)"})
                          .add_hline(y=LSZH[2], line_dash="dot", annotation_text="LSZH elevation")])
    else:
        _out = mo.md("*no go-around at LSZH in the window*")
    _out
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 18.4 – What did the arrivals do during the outage?

    No holdings does not mean "no delay". Controllers can also make arrivals lose time with **vectoring, path
    stretching or speed changes** ([SKYbrary: AMAN](https://skybrary.aero/articles/arrival-manager-aman)), and flow
    management prefers to delay aircraft **on the ground** at their departure airport
    ([SKYbrary: ATFM](https://skybrary.aero/articles/air-traffic-flow-management-atfm)). One number shows the
    delay in the air: the **time from 50 km to touchdown**.

    For every aircraft that **landed at LSZH** in the window – the first position within 4 km of the airport and
    below 1,800 ft (on the runway or just above it) – find the first airborne position within 50 km in the 60
    minutes before that, and compute the minutes in between. Store it in `approach` with the columns `icao24`,
    `callsign`, `landed_at`, `minutes` and `period` (`before` / `outage` / `after`, by `landed_at` and `OUTAGE`).

    /// details | Hint
    `landed = near[(near.dist_km < 4) & (near.altitude_ft < 1800) & near.flight_phase.isin(["descending",
    "on ground"])].groupby("icao24")["snapshot_time"].min()`. Merge it back onto `near` and keep positions
    `dist_km <= 50`, not on the ground, between `landed_at − 60 min` and `landed_at`; the minimum time per aircraft
    is the entry. `pd.cut` or `np.select` for the period.
    ///
    """)
    return


@app.cell
def _(OUTAGE, near, np, pd):
    # TODO 18.4: approach = per landing at LSZH: icao24, callsign, landed_at, minutes (50 km -> touchdown), period
    approach = pd.DataFrame({"icao24": ["todo"], "callsign": [None], "landed_at": [OUTAGE[0]], "minutes": [0.0], "period": ["before"]})
    return (approach,)


@app.cell(hide_code=True)
def _(approach, check, mo, px):
    try:
        _ok = ({"icao24", "landed_at", "minutes", "period"} <= set(approach.columns) and len(approach) >= 20
               and set(approach["period"]) <= {"before", "outage", "after"} and "outage" in set(approach["period"])
               and 4 < approach["minutes"].median() < 40)
        _s = approach.groupby("period", sort=False)["minutes"].agg(["count", "median", "max"]).round(1) if _ok else None
        _txt = ", ".join(f"{p}: {r['count']:.0f} landings, median {r['median']:.0f} min (max {r['max']:.0f})"
                         for p, r in _s.iterrows()) if _ok else ""
        _out = [check(_ok, f"50 km → touchdown at Zurich – {_txt}",
                      "Expected one row per landing with minutes between 4 and 40 (median) and all three periods – "
                      "is the window loaded (button at the top)?")]
        if _ok:
            _out.append(px.box(approach, x="period", y="minutes", points="all", height=300,
                               category_orders={"period": ["before", "outage", "after"]},
                               hover_data=["callsign", "landed_at"],
                               labels={"minutes": "minutes from 50 km to touchdown"}))
    except Exception as _e:
        _out = [check(False, "", f"approach could not be evaluated: {type(_e).__name__}: {_e}")]
    mo.vstack(_out)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Diversions?

    An aircraft that was **on its way into Zurich** – descending below 10,000 ft within 40 km around the outage –
    but was never seen at the airport (within 5 km, below 3,500 ft) and was last seen **more than 30 km away** may
    have diverted (or was simply on its way to another
    airport and passed by). The route lookup (`silver.flight_routes`, adsbdb) adds where it was *supposed* to go –
    remember from 07 that these routes are sometimes wrong.
    """)
    return


@app.cell
def _(OUTAGE, approach, con, near, pd):
    _win = near[near["snapshot_time"].between(OUTAGE[0] - pd.Timedelta(minutes=30), OUTAGE[1] + pd.Timedelta(minutes=30))]
    _inbound = _win[(_win["dist_km"] <= 40) & (_win["altitude_ft"] < 10000) & (_win["flight_phase"] == "descending")]
    _at_zrh = set(near.loc[(near["dist_km"] < 5) & (near["altitude_ft"] < 3500), "icao24"])   # landed or departed
    _cand = set(_inbound["icao24"]) - _at_zrh
    _last = (near[near["icao24"].isin(_cand)].sort_values("snapshot_time").groupby("icao24")
             .agg(callsign=("callsign", "last"), last_seen=("snapshot_time", "max"), last_km=("dist_km", "last"),
                  last_alt_ft=("altitude_ft", "last"), last_phase=("flight_phase", "last")).reset_index())
    _routes = con.execute("SELECT callsign, origin_icao, destination_icao, destination_name FROM silver.flight_routes").df()
    diversions = (_last[_last["last_km"] > 30].merge(_routes, on="callsign", how="left")
                  .sort_values("last_km", ascending=False).reset_index(drop=True))
    diversions
    return (diversions,)


@app.cell(hide_code=True)
def _(approach, comparison, diversions, mo, my_holdings, module_holdings):
    _out = approach.groupby("period")["minutes"].median() if len(approach) and "period" in approach else {}
    _to_zrh = diversions[diversions["destination_icao"].eq("LSZH") & (diversions["last_km"] > 20)] \
        if "destination_icao" in diversions else diversions.iloc[0:0]
    mo.md(f"""
    ### What happened to the arrivals on 20 October?

    - Holdings in the window: **{len(my_holdings)}** (yours), **{len(module_holdings)}** (module).
    - Median time from 50 km to touchdown: before **{_out.get('before', float('nan')):.0f} min**, during the outage
      **{_out.get('outage', float('nan')):.0f} min**, after **{_out.get('after', float('nan')):.0f} min**.
    - Descending near Zurich around the outage, never at the airport, last seen > 30 km away:
      **{len(diversions)}** aircraft, of which
      **{len(_to_zrh)}** had LSZH as (looked-up) destination and were last seen more than 20 km away.

    Read it together with notebook 17: in our counts the outage mainly stopped **departures** (the press reported
    that starts *and* landings were briefly interrupted). Did the arrivals keep coming – and what does that tell
    you about where the delay was absorbed?
    """) if len(comparison) else mo.md("")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Think about it

    - The window ends where the Swiss feed ends (lat 45.6–48.0, lon 5.8–10.7). RILAX lies about 54 km north of the
      airport – what would you miss if the stack were further out? Would the world feed (every 6 min) be enough
      to see a racetrack?
    - Your go-around detector uses the raw pressure altitude. On a day with QNH 990 hPa, how many feet is the
      altitude off (near the ground about 27–28 ft per hPa – our computation with the standard atmosphere) – and
      which threshold would that break?
    - About 3.6 go-arounds per 1,000 landings at Zurich (study above): how many days of data do you need before your
      count says anything about Zurich?

    **Next:** [19 – Runways and wind](19_runways_wind.py)
    """)
    return


if __name__ == "__main__":
    app.run()
