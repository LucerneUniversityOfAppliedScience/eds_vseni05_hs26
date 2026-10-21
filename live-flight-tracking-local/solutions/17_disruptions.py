import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium", app_title="17 – Detecting disruptions")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 17 – Detecting disruptions automatically

    In notebook 10 you *saw* the Skyguide outage of 20 October 2026 in a chart, because you knew where to look. What
    the press reported: a failure of the system that generates the controllers' radar picture; at Zurich no
    departure from 14:15 for about an hour ([Blick](https://www.blick.ch/schweiz/zuerich/am-flughafen-zuerich-koennen-keine-flieger-mehr-starten/ds1e9ml));
    starts and landings at Zurich and Geneva briefly interrupted ([SRF](https://www.srf.ch/news/schweiz/stoerung-eines-systems-radarproblem-weniger-landungen-am-flughafen-zuerich)).
    An app has to find such events on its own: compare every 15 minutes with the **usual** value and raise an alert
    when the traffic is far below it.

    The hard part is "usual". Three ideas:

    1. **Same time on other days** – Tuesday 14:30 compared with Monday 14:30.
    2. **The last few hours** – a rolling, *robust* baseline within the day.
    3. **Same weekday and slot over weeks** – Tuesday 14:30 compared with the last Tuesdays at 14:30. The best one,
       but it needs weeks of data.

    Our class bucket started on 19 October, so idea 3 is not possible yet and idea 1 only works for a few hours. We
    build idea 2 and write the code so that it uses ideas 1 and 3 as soon as the data is there.
    """)
    return


@app.cell
def _(mo):
    import sys

    sys.path.insert(0, str(mo.notebook_dir().parent))
    import numpy as np
    import pandas as pd

    import flightlab as fl

    def check(ok, good, bad):
        return mo.callout(mo.md(("✅ " + good) if ok else ("❌ " + bad)), kind="success" if ok else "danger")

    con = fl.snapshot()
    _span = con.execute("""SELECT MIN(snapshot_time), MAX(snapshot_time), COUNT(DISTINCT snapshot_time)
                           FROM silver.flights WHERE region = 'switzerland'""").fetchone()
    mo.md(f"Swiss feed: **{_span[2]:,} snapshots** between {_span[0]:%d.%m. %H:%M} and {_span[1]:%d.%m. %H:%M} UTC")
    return check, con, fl, np, pd


@app.cell
def _(con, mo):
    _have = con.execute("""SELECT COUNT(*) FROM bronze.snapshots
                           WHERE region = 'switzerland' AND time >= epoch(TIMESTAMP '2026-10-19 00:00')""").fetchone()[0]
    load_r2 = mo.ui.run_button(label="Load missing snapshots since 19.10.2026 from R2")
    mo.hstack([mo.md(f"Swiss snapshots since 19.10.: **{_have:,}** (the bucket has one every ~35 s, about 100 per "
                     f"hour). Gaps in your copy look like disruptions – fill them first."), load_r2], justify="start")
    return (load_r2,)


@app.cell
def _(con, fl, load_r2, mo):
    import datetime

    if load_r2.value:
        with mo.status.spinner(title="backfilling from R2 (only the missing files are downloaded) ..."):
            fl.backfill(con, "switzerland", since=datetime.datetime(2026, 10, 19),
                        until=datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None),
                        env=fl.load_env(), log=lambda *_: None)
            fl.build_silver(con)
    r2_loaded = load_r2.value
    return (r2_loaded,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 17.1 – Movements per 15 minutes, with the zeros

    Count per **15-minute slot (Swiss time)** and airport the **departures** (different aircraft, `climbing`) and
    **arrivals** (`descending`) within **15 km** and **below 6,000 ft** of LSZH (47.4581, 8.5481) and LSGG (46.2381,
    6.1090) – airport reference points from [OurAirports](https://ourairports.com/data/) – the definition of TODO
    10.5, now for all days. Store it in `slots` with the columns `t_local`,
    `airport`, `departures`, `arrivals`.

    **The trap:** a `GROUP BY` only returns slots in which something happened. At night Zurich has no departures –
    and a missing row is not a zero. Build a **grid** of all slots in which the Swiss feed has snapshots (`CROSS JOIN`
    with the airports) and `LEFT JOIN` the counts onto it (`COALESCE(..., 0)`). Slots without any snapshot stay
    missing: no data is not the same as no traffic.

    /// details | Hint
    ```sql
    WITH apt(airport, lat, lon) AS (VALUES ('LSZH', 47.4581, 8.5481), ('LSGG', 46.2381, 6.1090)),
    grid AS (SELECT DISTINCT time_bucket(INTERVAL 15 MINUTE,
                    timezone('Europe/Zurich', snapshot_time::TIMESTAMPTZ)) AS t_local
             FROM silver.flights WHERE region = 'switzerland'),
    moves AS (SELECT ..., COUNT(DISTINCT f.icao24) FILTER (WHERE f.flight_phase = 'climbing') AS departures, ...)
    SELECT g.t_local, a.airport, COALESCE(m.departures, 0) AS departures, ...
    FROM grid g CROSS JOIN apt a LEFT JOIN moves m USING (t_local, airport)
    ```
    ///
    """)
    return


@app.cell
def _(con, mo, r2_loaded):
    r2_loaded                                               # rerun after the backfill
    # >>> TODO 17.1: slots = departures and arrivals per 15 min (Swiss time) at LSZH and LSGG, 0 when nothing moved
    # ?? slots = mo.sql("SELECT TIMESTAMP '2026-10-20 12:00' AS t_local, 'LSZH' AS airport, 0 AS departures, 0 AS arrivals", engine=con)
    slots = mo.sql(
        """
        WITH apt(airport, lat, lon) AS (VALUES ('LSZH', 47.4581, 8.5481), ('LSGG', 46.2381, 6.1090)),
        grid AS (
            SELECT DISTINCT time_bucket(INTERVAL 15 MINUTE, timezone('Europe/Zurich', snapshot_time::TIMESTAMPTZ)) AS t_local
            FROM silver.flights WHERE region = 'switzerland'),
        moves AS (
            SELECT time_bucket(INTERVAL 15 MINUTE, timezone('Europe/Zurich', f.snapshot_time::TIMESTAMPTZ)) AS t_local,
                   a.airport,
                   COUNT(DISTINCT f.icao24) FILTER (WHERE f.flight_phase = 'climbing')   AS departures,
                   COUNT(DISTINCT f.icao24) FILTER (WHERE f.flight_phase = 'descending') AS arrivals
            FROM silver.flights f
            JOIN apt a
              ON 2 * 6371 * ASIN(SQRT(POWER(SIN(RADIANS(f.latitude - a.lat) / 2), 2)
                 + COS(RADIANS(a.lat)) * COS(RADIANS(f.latitude)) * POWER(SIN(RADIANS(f.longitude - a.lon) / 2), 2))) <= 15
            WHERE f.region = 'switzerland' AND f.altitude_ft < 6000
            GROUP BY ALL)
        SELECT g.t_local, a.airport, COALESCE(m.departures, 0) AS departures, COALESCE(m.arrivals, 0) AS arrivals
        FROM grid g CROSS JOIN apt a
        LEFT JOIN moves m USING (t_local, airport)
        ORDER BY airport, t_local
        """,
        engine=con,
    )
    # <<< TODO 17.1
    return (slots,)


@app.cell(hide_code=True)
def _(check, slots):
    try:
        _ok_cols = {"t_local", "airport", "departures", "arrivals"} <= set(slots.columns)
        _zrh = slots[slots["airport"] == "LSZH"] if _ok_cols else slots.iloc[0:0]
        _zeros = int((_zrh["departures"] == 0).sum()) if _ok_cols else 0
        _ok = (_ok_cols and len(_zrh) > 50 and set(slots["airport"]) == {"LSZH", "LSGG"}
               and len(slots) == 2 * slots["t_local"].nunique() and _zeros > 5)
        _msg = (f"{slots['t_local'].nunique()} slots of 15 minutes, {_zeros} of them without a departure at Zurich "
                f"(night curfew) – they are rows with 0 now, not missing rows" if _ok else "")
    except Exception as _e:
        _ok, _msg = False, ""
    check(_ok, _msg, "Expected one row per slot and airport (both airports in every slot, also with 0 movements) – "
                     "build the grid of slots first and LEFT JOIN the counts.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Idea 1: the same time on another day

    Each line is one day, plotted over the time of day. Where two lines overlap you can compare Tuesday with Monday
    directly. With data from 19 October evening on, the overlap is small – after a week it is the whole day.
    """)
    return


@app.cell
def _(mo, pd, slots):
    import plotly.express as px

    _z = slots[slots["airport"] == "LSZH"].copy() if "airport" in slots else slots.iloc[0:0]
    if len(_z):
        _z["day"] = _z["t_local"].dt.strftime("%a %d.%m.")
        _z["time_of_day"] = pd.Timestamp("2000-01-01") + (_z["t_local"] - _z["t_local"].dt.normalize())
        _overlap = int((_z.groupby(_z["t_local"].dt.strftime("%H:%M"))["day"].nunique() > 1).sum())
        _out = mo.vstack([
            px.line(_z, x="time_of_day", y="departures", color="day", markers=True, height=300,
                    labels={"time_of_day": "time of day (Swiss time)", "departures": "LSZH departures per 15 min"})
            .update_xaxes(tickformat="%H:%M"),
            mo.md(f"Slots of the day that exist on **at least two days**: **{_overlap}** of 96")])
    else:
        _out = mo.md("*no slots yet*")
    _out
    return (px,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 17.2 – A robust rolling baseline

    Idea 2: the expected value of a slot is the **median of the previous 12 slots** (3 hours), *without* the slot
    itself – otherwise the outage would pull its own baseline down. The spread is the **MAD** (median absolute
    deviation, `median(|x − median(x)|)`), scaled with 1.4826 = 1 / 0.6745 (0.6745 is the 75 % quantile of the
    standard normal distribution) – for normally distributed data the scaled MAD estimates the standard deviation
    ([scipy: `median_abs_deviation`, `scale="normal"`](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.median_abs_deviation.html)).
    Sigma is at least 1 (a quiet hour with always 0 has MAD 0 – no division by zero).

    Median and MAD are **robust**: one crazy value in the window does not move them, the mean and the standard
    deviation would. Then the z-score: `z = (count − expected) / sigma`.

    Write `robust_baseline(counts, window=12)`: input a pandas Series (one airport, sorted by time), output a
    DataFrame with the columns `expected`, `sigma`, `z` and the same index. Require at least `window // 2` previous
    slots, otherwise NaN.

    /// details | Hint
    `counts.shift(1)` moves every value one slot later, so a rolling window never sees the current slot.
    `.rolling(window, min_periods=window // 2).median()`, and for the MAD
    `.rolling(...).apply(lambda x: np.median(np.abs(x - np.median(x))), raw=True)`. `np.maximum(1.4826 * mad, 1.0)`.
    ///
    """)
    return


@app.cell
def _(np, pd):
    # >>> TODO 17.2: robust_baseline(counts, window) -> DataFrame expected, sigma, z (median/MAD of the previous slots)
    # ?? def robust_baseline(counts, window=12):
    # ??     return pd.DataFrame({"expected": counts * 0.0, "sigma": counts * 0.0 + 1, "z": counts * 0.0}, index=counts.index)
    def robust_baseline(counts, window=12):
        prev = counts.astype(float).shift(1)                       # never look at the slot itself
        roll = prev.rolling(window, min_periods=window // 2)
        expected = roll.median()
        mad = roll.apply(lambda x: np.median(np.abs(x - np.median(x))), raw=True)
        sigma = np.maximum(1.4826 * mad, 1.0)
        return pd.DataFrame({"expected": expected, "sigma": sigma, "z": (counts - expected) / sigma},
                            index=counts.index)
    # <<< TODO 17.2
    return (robust_baseline,)


@app.cell(hide_code=True)
def _(check, pd, robust_baseline):
    try:
        _drop = robust_baseline(pd.Series([10, 11, 9, 10, 12, 10, 9, 11, 10, 10, 11, 9, 3]))
        _spike = robust_baseline(pd.Series([10, 11, 9, 10, 12, 10, 9, 11, 10, 10, 11, 9, 100, 10]))
        _short = robust_baseline(pd.Series([5, 6]))
        _ok = (abs(_drop["expected"].iloc[-1] - 10) < 0.01 and _drop["z"].iloc[-1] < -3
               and abs(_spike["expected"].iloc[-1] - 10) < 0.01 and _short["expected"].isna().all()
               and _drop["sigma"].min() >= 1)
        _msg = (f"a drop to 3 after twelve slots around 10: expected {_drop['expected'].iloc[-1]:.0f} ± "
                f"{_drop['sigma'].iloc[-1]:.1f}, z = {_drop['z'].iloc[-1]:.1f}; one spike of 100 does not move the "
                f"next baseline ({_spike['expected'].iloc[-1]:.0f})" if _ok else "")
    except Exception as _e:
        _ok, _msg = False, ""
    check(_ok, _msg, "Test series 10, 11, 9, ... then 3: expected 10 (median of the previous 12, without the current "
                     "slot), z below −3, sigma at least 1, NaN when fewer than window // 2 previous slots exist.")
    return


@app.cell
def _(pd, robust_baseline, slots):
    # long format: one row per slot, airport and kind (departures / arrivals), with the baseline of its own series
    _long = (slots.melt(["t_local", "airport"], value_vars=["departures", "arrivals"], var_name="kind", value_name="n")
             .sort_values(["airport", "kind", "t_local"]).reset_index(drop=True)) if "airport" in slots else None
    scored = (pd.concat([_g.join(robust_baseline(_g["n"])) for _, _g in _long.groupby(["airport", "kind"])])
              if _long is not None and len(_long) else pd.DataFrame(columns=["t_local", "airport", "kind", "n",
                                                                             "expected", "sigma", "z"]))
    scored
    return (scored,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 17.3 – The flag rule and the event table

    A low z-score alone gives many false alarms: 15 minutes with 2 departures instead of 7 happen on a normal day.
    Write `find_events(df, k=2.5, min_expected=4, max_share=0.6, min_slots=2, hours=(6, 22))` that

    1. **flags** a slot when `expected >= min_expected` (quiet night slots with ~0 traffic can never alarm),
       `z <= -k` **and** `n <= max_share * expected` (with 0.6: at least 40 % missing – a z-score with a small
       sigma alone is not enough), and the hour of `t_local` lies within `hours` (06:00–22:59 – the rolling baseline does not know that
       Zurich stops at 23:30, see below),
    2. joins **consecutive** flagged slots of the same `airport` and `kind` into one event and keeps only events with
       at least `min_slots` slots (one bad slot is noise, two in a row are a pattern),
    3. returns one row per event: `airport`, `kind`, `start` (first slot), `end` (last slot + 15 minutes), `slots`,
       `observed` (mean count), `minimum`, `expected` (median baseline), `sigma` (median).

    Then `events = find_events(scored)`. The test case: the Skyguide outage must be found – and not much else.

    /// details | Hint
    A run id per series: `run = (flag != flag.shift()).cumsum()` changes its value at every switch between flagged
    and not flagged. Group the flagged rows by `["airport", "kind", run]` and aggregate with
    `.agg(start=("t_local", "min"), last=("t_local", "max"), slots=("n", "size"), ...)`.
    ///
    """)
    return


@app.cell
def _(pd, scored):
    EVENT_COLUMNS = ["airport", "kind", "start", "end", "slots", "observed", "minimum", "expected", "sigma"]

    # >>> TODO 17.3: find_events(df, ...) -> one row per run of >= min_slots flagged slots
    # ?? def find_events(df, k=2.5, min_expected=4, max_share=0.6, min_slots=2, hours=(6, 22)):
    # ??     return pd.DataFrame(columns=EVENT_COLUMNS)
    def find_events(df, k=2.5, min_expected=4, max_share=0.6, min_slots=2, hours=(6, 22)):
        d = df.sort_values(["airport", "kind", "t_local"]).copy()
        d["flag"] = ((d["expected"] >= min_expected) & (d["z"] <= -k) & (d["n"] <= max_share * d["expected"])
                     & d["t_local"].dt.hour.between(*hours))
        d["run"] = (d["flag"] != d.groupby(["airport", "kind"])["flag"].shift()).cumsum()
        ev = (d[d["flag"]].groupby(["airport", "kind", "run"])
              .agg(start=("t_local", "min"), last=("t_local", "max"), slots=("n", "size"), observed=("n", "mean"),
                   minimum=("n", "min"), expected=("expected", "median"), sigma=("sigma", "median"))
              .reset_index())
        ev = ev[ev["slots"] >= min_slots].copy()
        ev["end"] = ev["last"] + pd.Timedelta(minutes=15)
        return ev[EVENT_COLUMNS].sort_values("start").reset_index(drop=True)
    # <<< TODO 17.3

    events = find_events(scored)
    events
    return (events, find_events)


@app.cell(hide_code=True)
def _(check, con, events, mo, pd, px, r2_loaded, scored):
    _out = []
    try:
        _t = pd.Timestamp("2026-10-20 14:30")
        _have = r2_loaded or con.execute("""SELECT COUNT(DISTINCT snapshot_time) > 100 FROM silver.flights
            WHERE region = 'switzerland' AND snapshot_time BETWEEN '2026-10-20 11:00' AND '2026-10-20 13:30'""").fetchone()[0]
        _hit = events[(events["airport"] == "LSZH") & (events["kind"] == "departures")
                      & (events["start"] <= _t) & (events["end"] > _t)] if len(events) else events
        _ok = len(_hit) == 1 and len(events) <= 3 and _hit["start"].iloc[0] >= pd.Timestamp("2026-10-20 13:45")
        _h = _hit.iloc[0] if len(_hit) else None
        _out.append(check(
            _ok, f"Skyguide outage found: LSZH departures {_h['start']:%H:%M}–{_h['end']:%H:%M}, "
                 f"{_h['observed']:.0f} per 15 min instead of {_h['expected']:.0f} – and {len(events) - 1} other "
                 f"event(s) in {scored['t_local'].nunique()} slots" if _ok else "",
            ("Expected exactly one LSZH departure event around 14:15–15:00 on 20.10.2026 and at most 3 events in "
             f"total (found {len(events)}). Flag rule: expected ≥ 4, z ≤ −k, n ≤ max_share × expected, 06–22 h; "
             "then runs of ≥ 2 slots.") if _have else
            "Your copy has no data for the afternoon of 20.10.2026 – press the R2 button at the top."))
        _z = scored[(scored["airport"] == "LSZH") & (scored["kind"] == "departures")]
        _fig = (px.line(_z, x="t_local", y=["n", "expected"], height=320,
                        labels={"t_local": "Swiss time", "value": "LSZH departures per 15 min", "variable": ""})
                .add_scatter(x=_z["t_local"], y=_z["expected"] - 2.5 * _z["sigma"], mode="lines",
                             line={"dash": "dot", "width": 1}, name="expected − 2.5 σ"))
        for _, _r in events.iterrows():
            _fig.add_vrect(x0=_r["start"], x1=_r["end"], fillcolor="red", opacity=0.12, line_width=0,
                           annotation_text=f"{_r['airport']} {_r['kind']}")
        _out.append(_fig)
    except Exception as _e:
        _out.append(check(False, "", f"find_events failed: {type(_e).__name__}: {_e}"))
    mo.vstack(_out)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 17.4 – The alert as the app would show it

    Write `alert_text(e)` for one row of `events` (a pandas Series), e.g.

    `LSZH: 3 departures per 15 min 14:15–15:00, expected 10 ± 2`

    – airport, observed (rounded), kind, start and end as `HH:MM`, expected and sigma rounded to whole numbers.
    """)
    return


@app.cell
def _():
    # >>> TODO 17.4: alert_text(e) -> "LSZH: 3 departures per 15 min 14:15–15:00, expected 10 ± 2"
    # ?? def alert_text(e):
    # ??     return "TODO"
    def alert_text(e):
        return (f"{e['airport']}: {e['observed']:.0f} {e['kind']} per 15 min {e['start']:%H:%M}–{e['end']:%H:%M}, "
                f"expected {e['expected']:.0f} ± {e['sigma']:.0f}")
    # <<< TODO 17.4
    return (alert_text,)


@app.cell(hide_code=True)
def _(alert_text, check, events, mo, pd):
    import re

    try:
        _sample = pd.Series({"airport": "LSGG", "kind": "arrivals", "start": pd.Timestamp("2026-10-20 09:00"),
                             "end": pd.Timestamp("2026-10-20 09:45"), "slots": 3, "observed": 1.4, "minimum": 1,
                             "expected": 6.0, "sigma": 1.48})
        _txt = alert_text(_sample)
        _ok = _txt == "LSGG: 1 arrivals per 15 min 09:00–09:45, expected 6 ± 1"
        _alerts = [alert_text(_r) for _, _r in events.iterrows()]
        _ok = _ok and all(re.match(r"^\w{4}: \d+ \w+ per 15 min \d\d:\d\d–\d\d:\d\d, expected \d+ ± \d+$", _a)
                          for _a in _alerts)
    except Exception as _e:
        _ok, _txt, _alerts = False, f"{type(_e).__name__}: {_e}", []
    mo.vstack([check(_ok, f"{len(_alerts)} alert(s) ready for the app", f"For a test event the text was «{_txt}» – "
                     "expected «LSGG: 1 arrivals per 15 min 09:00–09:45, expected 6 ± 1»."),
               *[mo.callout(mo.md(f"**{_a}**"), kind="warn") for _a in (_alerts if _ok else [])]])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## A second signal: aircraft in the air over Switzerland

    After the short interruption, Skyguide kept the Swiss airspace at **50 % capacity** as a precaution and raised it
    to 80 % in the evening, for a 24-hour stabilisation phase ([watson](https://www.watson.ch/schweiz/luftfahrt/514525323-betrieb-an-schweizer-flughaefen-nach-stoerung-wieder-angelaufen),
    [20 Minuten](https://www.20min.ch/story/schweiz-technische-stoerung-bei-skyguide-luftraum-kapazitaeten-reduziert-103652678)).
    That affects overflights too, not only Zurich and Geneva. Same method, other series: the **average number of
    airborne aircraft per snapshot** in a box around Switzerland (lat 45.8–47.8, lon 5.9–10.5) per 15 minutes – your `robust_baseline` and `find_events`, with other
    parameters: aircraft already in the air keep flying, so the stock drops more slowly than the departures
    (`k=2`, `max_share=0.8`).
    """)
    return


@app.cell
def _(alert_text, con, find_events, mo, pd, px, r2_loaded, robust_baseline):
    r2_loaded
    _air = con.execute("""
        SELECT time_bucket(INTERVAL 15 MINUTE, timezone('Europe/Zurich', snapshot_time::TIMESTAMPTZ)) AS t_local,
               'CH' AS airport, 'airborne aircraft' AS kind,
               ROUND(COUNT(*) / COUNT(DISTINCT snapshot_time)) AS n
        FROM silver.flights
        WHERE region = 'switzerland' AND NOT on_ground
          AND latitude BETWEEN 45.8 AND 47.8 AND longitude BETWEEN 5.9 AND 10.5
        GROUP BY ALL ORDER BY t_local""").df()
    try:
        _sc = _air.join(robust_baseline(_air["n"]))
        _ev = find_events(_sc, k=2, min_expected=20, max_share=0.8)
        _fig = px.line(_sc, x="t_local", y=["n", "expected"], height=300,
                       labels={"t_local": "Swiss time", "value": "airborne over CH (per snapshot)", "variable": ""})
        for _, _r in _ev.iterrows():
            _fig.add_vrect(x0=_r["start"], x1=_r["end"], fillcolor="red", opacity=0.12, line_width=0)
        _alerts = [mo.callout(mo.md(f"**{alert_text(_r)}**"), kind="warn") for _, _r in _ev.iterrows()]
        _out = mo.vstack([_fig, *(_alerts or [mo.md("*no event in this series*")])])
    except Exception as _e:
        _out = mo.md(f"*finish 17.2–17.4 first ({type(_e).__name__})*")
    _out
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Getting better with more data: weekday × slot

    The rolling baseline has two weaknesses you met above:

    - It does not know the **schedule**. Zurich operates from 06:00 to 23:30, the last half hour only to catch up
      on delays ([Flughafen Zürich](https://www.flughafen-zuerich.ch/de/unternehmen/verantwortung/laerm-und-schallschutz/flugbetrieb)).
      In our data of 19 October the 23:00 slot still had 5 departures, the 23:15 slot none. Without the `hours`
      filter that is an alarm every night.
    - It needs a few slots to "forget" an outage: in the fourth slot of the outage the baseline already contains
      three low values.

    With weeks of data, the expected value of *Tuesday 14:30* becomes the **median of the same slot on the last N
    Tuesdays** – it knows the curfew, the morning wave and the quiet Sunday. Until enough same weekdays exist, the
    same slot on *any* earlier day is the next best thing; the rolling baseline is the fallback. The cell below does
    exactly this and shows which baseline each slot got. Only **earlier** days count (an alert cannot look into the
    future), and at least 3 of them – with one earlier day, yesterday's outage would be today's normal.
    """)
    return


@app.cell
def _(np, pd, scored):
    def slot_baseline(df, min_days=3, n_days=6):
        """expected / sigma from the same 15-min slot on earlier days: same weekday if >= min_days of them exist,
        otherwise any weekday, otherwise the rolling baseline already in df. Uses whatever days there are."""
        d = df.sort_values("t_local").copy()
        d["slot"] = d["t_local"].dt.strftime("%H:%M")
        d["weekday"] = d["t_local"].dt.dayofweek
        d["baseline"] = np.where(d["expected"].notna(), "rolling 3 h", "none")
        for _name, _keys in [("same slot, any day", ["airport", "kind", "slot"]),
                             ("same slot, same weekday", ["airport", "kind", "slot", "weekday"])]:
            _g = d.groupby(_keys)["n"]
            _med = _g.transform(lambda s: s.shift(1).rolling(n_days, min_periods=1).median())
            _mad = _g.transform(lambda s: s.shift(1).rolling(n_days, min_periods=1)
                                .apply(lambda x: np.median(np.abs(x - np.median(x))), raw=True))
            _days = _g.transform(lambda s: s.shift(1).rolling(n_days, min_periods=1).count())
            _use = _days >= min_days
            d.loc[_use, "expected"] = _med[_use]
            d.loc[_use, "sigma"] = np.maximum(1.4826 * _mad[_use], np.sqrt(_med[_use]).clip(lower=1))
            d.loc[_use, "baseline"] = _name
        d["z"] = (d["n"] - d["expected"]) / d["sigma"]
        return d.drop(columns=["slot", "weekday"])

    try:
        best = slot_baseline(scored) if len(scored) else scored
        _counts = best["baseline"].value_counts().rename("slots").reset_index() if len(best) else pd.DataFrame()
    except Exception as _e:
        best, _counts = scored, pd.DataFrame({"error": [f"{type(_e).__name__}: {_e}"]})
    _counts
    return (best,)


@app.cell
def _(alert_text, best, find_events, mo):
    try:
        _ev = find_events(best)
        _out = mo.vstack([mo.md(f"Events with the best available baseline per slot: **{len(_ev)}**"),
                          *[mo.callout(mo.md(f"**{alert_text(_r)}**"), kind="warn") for _, _r in _ev.iterrows()]])
    except Exception as _e:
        _out = mo.md(f"*finish 17.3–17.4 first ({type(_e).__name__})*")
    _out
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Think about it

    - Departures stopped, but did the **arrivals** drop as well? The press wrote that starts *and* landings were
      briefly interrupted – how long would a landing stop have to be before your 15-minute counts show it?
    - Play with `k` and `min_slots`: how many false alarms do you get with `k = 2` and `min_slots = 1`? How late is
      the alert with `min_slots = 3`? An alert system trades **sensitivity** against **false alarms** and **delay**.
    - Fog, a runway closure, a strike, a big storm: which of them would this detector find – and which would it
      miss because traffic drops slowly over hours?
    - Once you have four Tuesdays: re-run the notebook. Which baseline do the slots get now, and does the curfew
      still need the `hours` filter?

    **Next:** [18 – Holding patterns and go-arounds](18_holdings_go_arounds.py)
    """)
    return


if __name__ == "__main__":
    app.run()
