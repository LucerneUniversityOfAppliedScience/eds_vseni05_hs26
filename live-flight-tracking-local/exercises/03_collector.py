import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium", app_title="03 – The collector")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 03 – The collector: one message every 35 seconds

    One snapshot is a photo. A **stream** of snapshots shows movement: climbs, approaches, holding patterns. The
    collector calls OpenSky again and again and stores every answer with `store_snapshot` from 02.

    Three things make the difference between a toy loop and a collector that runs all night:

    1. **Credits.** The header `X-Rate-Limit-Remaining` tells how many are left. Stop before 0 – otherwise OpenSky
       answers `429 Too Many Requests` until the daily refill; the header `X-Rate-Limit-Retry-After-Seconds` says
       how long to wait ([API credits](https://openskynetwork.github.io/opensky-api/rest.html#api-credits)).
    2. **429 and network errors** can happen on a long run. Wait (longer each time – *exponential backoff*) and try
       again instead of crashing. Your loop below handles the 429; `flightlab.collect` (used by `collector.py` and
       the app) also retries network errors.
    3. **A steady rhythm.** If a call takes 2 s, sleep 33 s, not 35 – otherwise the loop drifts.

    Budget: one call every 35 s = 86,400 / 35 ≈ 2,470 calls per day. Anonymous: 400 per day and IP. With a free
    OpenSky account: 4,000 – put `OPENSKY_CLIENT_ID` / `OPENSKY_CLIENT_SECRET` into `.env` (see `.env.template`).
    """)
    return


@app.cell
def _(mo):
    import sys
    import time

    import duckdb
    import requests

    sys.path.insert(0, str(mo.notebook_dir().parent))
    import flightlab as fl                       # fetch_states (01), store_snapshot (02)

    def check(ok, good, bad):
        return mo.callout(mo.md(("✅ " + good) if ok else ("❌ " + bad)), kind="success" if ok else "danger")

    con = fl.practice(3)                            # practice_03.duckdb – continues from the step before
    fl.init_db(con)                              # bronze tables as in 02 (if you skipped 02)
    return check, con, fl, requests, time


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 3.1 – Log in (optional)

    Write `get_token()`: read `.env` with `fl.load_env()`. If `OPENSKY_CLIENT_ID` and `OPENSKY_CLIENT_SECRET` are set,
    return `fl.get_token(id, secret)`, otherwise `None` (= anonymous).
    """)
    return


@app.cell
def _(fl):
    # TODO 3.1: get_token() -> token string with an account in .env, None without
    def get_token():
        return None
    token = get_token()
    "logged in with an OpenSky account" if token else "anonymous (400 credits per day for your IP address)"
    return (token,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 3.2 – The loop

    Write `collect(minutes, interval, fetch, store, sleep, min_credits=20)`:

    - repeat until `minutes` are over: `data, credits = fetch()`, then `store(data, credits)` (returns True if new)
    - count the **new** snapshots and return that number
    - stop early when `credits < min_credits`
    - on `requests.HTTPError` with status **429**: double the waiting time (start: `interval`, max 600 s), `sleep`
      that long and try again
    - sleep `interval` minus the time the call took (never negative)

    `fetch`, `store` and `sleep` are **parameters** – that is how the check below can test your loop with a fake API
    in a millisecond instead of waiting 35 seconds per call.

    /// details | Hint
    ```python
    end = time.time() + minutes * 60
    while time.time() < end:
        t0 = time.time()
        try:
            data, credits = fetch()
            ...
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 429: ...
    ```
    `e.response.status_code` holds the HTTP status.
    ///
    """)
    return


@app.cell
def _(requests, time):
    # TODO 3.2: collect(minutes, interval, fetch, store, sleep, min_credits) -> number of new snapshots
    def collect(minutes, interval, fetch, store, sleep=time.sleep, min_credits=20):
        return 0
    return (collect,)


@app.cell(hide_code=True)
def _(check, collect, requests):
    def _answer(t, credits):
        return {"time": t, "states": []}, credits

    def _too_many():
        _r = requests.Response()
        _r.status_code = 429
        raise requests.HTTPError("429", response=_r)

    _script = [lambda: _answer(1, 100), _too_many, lambda: _answer(1, 99), lambda: _answer(2, 98),
               lambda: _answer(3, 10), lambda: _answer(4, 9)]
    _calls, _waits, _seen = iter(_script), [], set()

    def _store(data, credits):
        _new = data["time"] not in _seen
        _seen.add(data["time"])
        return _new

    try:
        _n = collect(minutes=1, interval=35, fetch=lambda: next(_calls)(), store=_store, sleep=_waits.append)
        _err = None
    except Exception as _e:
        _n, _err = None, f"{type(_e).__name__}: {_e}"
    check(_n == 3 and 70 in _waits and len(_waits) == 4,
          f"3 new snapshots (time 1 twice -> skipped), waited 70 s after the 429, stopped at 10 credits. "
          f"Waits: {[round(w) for w in _waits]}",
          f"Expected 3 new snapshots, a 70 s wait after the 429 and a stop at 10 credits – got {_n} new, "
          f"waits {[round(w) for w in _waits]}" + (f", error {_err}" if _err else ""))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Run it for real

    Two minutes, one call every 15 seconds = 8 credits. The button starts it (marimo does not run it on its own).
    """)
    return


@app.cell
def _(mo):
    run = mo.ui.run_button(label="Collect for 2 minutes")
    run
    return (run,)


@app.cell
def _(collect, con, fl, mo, run, token):
    mo.stop(not run.value, mo.md("*Press the button to start.*"))
    _store = lambda data, credits: fl.store_snapshot(con, data, "switzerland", "api", credits)
    _fetch = lambda: fl.fetch_states(fl.REGIONS["switzerland"], token)
    with mo.status.spinner(title="collecting ..."):
        _n = collect(minutes=2, interval=15, fetch=_fetch, store=_store)
    mo.md(f"**{_n} new snapshots.** In bronze now: "
          f"{con.execute('SELECT COUNT(*) FROM bronze.snapshots').fetchone()[0]} snapshots, "
          f"{con.execute('SELECT COUNT(*) FROM bronze.states').fetchone()[0]} positions.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## All night long: `collector.py`

    A notebook is the wrong place for a loop that runs for hours. The same logic (with your choices: region,
    interval) runs as a script that writes to the real database `flights.duckdb`:

    ```bash
    uv run collector.py --region switzerland --interval 35 --minutes 0      # 0 = until you press Ctrl+C
    ```

    ## Think about it

    - Why double the waiting time after a 429 instead of waiting the same time again?
    - Anonymous data has a time resolution of 10 s
      ([Limitations](https://openskynetwork.github.io/opensky-api/rest.html#limitations)). What happens with
      `interval=5`? (Look at `store`'s return value.)
    - The laptop goes to sleep for an hour. What does the data look like afterwards – and what could fill the gap?
      (That is notebook 04.)

    **Next:** [04 – Backfill from R2](04_backfill_r2.py)
    """)
    return


if __name__ == "__main__":
    app.run()
