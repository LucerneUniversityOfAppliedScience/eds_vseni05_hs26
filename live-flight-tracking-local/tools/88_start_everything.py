import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium", app_title="88 – Start everything")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 88 – Start everything: the Live Radar in one click

    No exercises, just the result: this notebook starts the map app with everything the notebooks 01–20 build –
    history from the class bucket, Switzerland live from OpenSky, the world feed every 6 minutes, and all analyses
    (weather, routes, CO₂, runways, contrails, emergency squawks) in the background.

    1. Run `tools/00_setup.py` once – the R2 keys in `.env` are needed for history and the world feed.
    2. Choose how many hours of history, press **Start**, wait for the link (the backfill takes a minute or two).
    3. Leave this notebook open while you use the map. **Stop** ends the app (closing marimo stops it too).

    Same thing from a terminal: `uv run start.py --hours 6`.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    import atexit
    import os
    import re
    import subprocess
    import sys
    from pathlib import Path

    ROOT = mo.notebook_dir().parent
    LOG = ROOT / "cache" / "live_radar.log"
    RADAR = {"proc": None}                                   # the running app process, kept across reruns

    def running():
        return RADAR["proc"] is not None and RADAR["proc"].poll() is None

    def url():
        if not LOG.exists():
            return None
        m = re.search(r"Dash is running on (http://127\.0\.0\.1:\d+)", LOG.read_text(errors="replace"))
        return m.group(1) if m else None

    def _stop_on_exit():
        if running():
            RADAR["proc"].terminate()

    _ = atexit.register(_stop_on_exit)                       # closing marimo stops the app too
    return LOG, RADAR, ROOT, os, running, subprocess, sys, url


@app.cell(hide_code=True)
def _(mo):
    hours = mo.ui.slider(1, 24, value=6, step=1, label="Hours of history from the bucket", show_value=True)
    world = mo.ui.checkbox(value=True, label="World feed (≈ 10,000 aircraft every 6 minutes)")
    start = mo.ui.run_button(label="Start the Live Radar", kind="success")
    stop = mo.ui.run_button(label="Stop", kind="danger")
    mo.vstack([hours, world, mo.hstack([start, stop], justify="start")])
    return hours, start, stop, world


@app.cell(hide_code=True)
def _(LOG, RADAR, ROOT, hours, mo, os, running, start, stop, subprocess, sys, world):
    if stop.value and running():
        RADAR["proc"].terminate()
        RADAR["proc"].wait(timeout=20)
        RADAR["proc"] = None
    if start.value and not running():
        LOG.parent.mkdir(exist_ok=True)
        _args = [sys.executable, str(ROOT / "start.py"), "--hours", str(hours.value), "--no-browser"]
        if not world.value:
            _args.append("--no-world")
        RADAR["proc"] = subprocess.Popen(_args, cwd=ROOT, stdout=LOG.open("w"), stderr=subprocess.STDOUT,
                                         env={**os.environ, "PYTHONUNBUFFERED": "1"})
    mo.md("")
    return


@app.cell(hide_code=True)
def _(mo):
    status_refresh = mo.ui.refresh(options=["3s"], default_interval="3s", label="")
    status_refresh
    return (status_refresh,)


@app.cell(hide_code=True)
def _(LOG, mo, running, start, status_refresh, stop, url):
    status_refresh, start, stop                              # rerun every 3 s and after the buttons
    _url = url()
    if running() and _url:
        _head = mo.callout(mo.md(f"**Running** – open the map: [{_url}]({_url})"), kind="success")
    elif running():
        _head = mo.callout(mo.md("**Starting …** (backfill from the bucket, then the map)"), kind="warn")
    else:
        _head = mo.callout(mo.md("**Not running.** Press Start."), kind="neutral")
    _tail = LOG.read_text(errors="replace").splitlines()[-12:] if LOG.exists() else []
    mo.vstack([_head, mo.md("Last lines of the app log:"), mo.plain_text("\n".join(_tail) or "–")])
    return


if __name__ == "__main__":
    app.run()
