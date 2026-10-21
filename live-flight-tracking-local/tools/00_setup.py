import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium", app_title="00 – Setup check")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 00 – Is everything ready?

    This notebook only **checks** – it changes nothing. Every line below should turn green. Red lines tell you what
    to fix; yellow ones are optional.

    Started with `uv run marimo edit .` from the project folder (this file: `tools/00_setup.py`) – `uv` installed Python and all
    packages of `pyproject.toml` into `.venv` on the first run.

    **marimo does not run a notebook when you open it.** Press the run button at the bottom right once (it runs every
    cell); after that, every change reruns the cells that depend on it. The same in every notebook.
    """)
    return


@app.cell
def _(mo):
    import importlib
    import platform
    import sys

    def line(ok, text, optional=False):
        icon = "✅" if ok else ("🟡" if optional else "❌")
        return f"{icon} {text}"

    _rows = [line(sys.version_info >= (3, 11), f"Python {platform.python_version()} ({sys.executable})")]
    for _pkg in ["duckdb", "pandas", "requests", "boto3", "plotly", "openap", "dash", "dash_leaflet"]:
        try:
            _m = importlib.import_module(_pkg)
            _rows.append(line(True, f"{_pkg} {getattr(_m, '__version__', '')}"))
        except Exception as _e:
            _rows.append(line(False, f"{_pkg} missing – run `uv sync` in the project folder ({type(_e).__name__})"))
    mo.md("## Packages\n\n" + "\n\n".join(_rows))
    return (line,)


@app.cell
def _(line, mo):
    import requests

    def reach(url, timeout=10):
        try:
            r = requests.get(url, timeout=timeout, headers={"User-Agent": "live-flight-tracking-local setup check"})
            return r.status_code, r
        except Exception as e:
            return None, e

    _code, _r = reach("https://opensky-network.org/api/states/all?lamin=46.9&lamax=47.0&lomin=8.4&lomax=8.5")
    _credits = _r.headers.get("X-Rate-Limit-Remaining") if _code == 200 else None
    _rows = [line(_code == 200, f"OpenSky API answers (HTTP {_code}) – credits left today for your IP: {_credits}"
                  if _code == 200 else f"OpenSky API not reachable: {_r if _code is None else 'HTTP ' + str(_code)}")]
    for _name, _url in [("Open-Meteo (weather)", "https://api.open-meteo.com/v1/forecast?latitude=47&longitude=8&current=temperature_2m"),
                        ("aviationweather.gov (METAR/TAF)", "https://aviationweather.gov/api/data/metar?ids=LSZH&format=json"),
                        ("adsbdb (routes)", "https://api.adsbdb.com/v0/callsign/SWR8"),
                        ("OurAirports (airports)", "https://davidmegginson.github.io/ourairports-data/regions.csv")]:
        _c, _x = reach(_url)
        _rows.append(line(_c is not None and _c < 500, f"{_name}: HTTP {_c}" if _c else f"{_name}: {type(_x).__name__}"))
    mo.md("## Internet\n\n" + "\n\n".join(_rows) +
          "\n\n*This small box costs 1 credit (every box up to 25 square degrees does). Anonymous: 400 per day, counted "
          "**per IP address** – a classroom often shares one address. A free account (`.env`) gives 4,000. "
          "([API credits](https://openskynetwork.github.io/opensky-api/rest.html#api-credits))*")
    return


@app.cell
def _(line, mo):
    import sys as _sys

    _sys.path.insert(0, str(mo.notebook_dir().parent))
    import flightlab as fl

    _env = fl.load_env()
    _rows = []
    _has_os = bool(_env.get("OPENSKY_CLIENT_ID") and _env.get("OPENSKY_CLIENT_SECRET"))
    if _has_os:
        try:
            fl.get_token(_env["OPENSKY_CLIENT_ID"], _env["OPENSKY_CLIENT_SECRET"])
            _rows.append(line(True, "OpenSky account in `.env`: login works (4,000 credits per day)"))
        except Exception as _e:
            _rows.append(line(False, f"OpenSky account in `.env`, but the login fails: {_e}"))
    else:
        _rows.append(line(False, "no OpenSky account in `.env` – anonymous is fine to start", optional=True))
    if all(_env.get(k) for k in ("R2_ENDPOINT", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY")):
        try:
            _s3, _bucket = fl.r2_client(_env)
            _top = _s3.list_objects_v2(Bucket=_bucket, Delimiter="/").get("CommonPrefixes", [])
            _rows.append(line(True, f"R2 class bucket readable: folders {', '.join(p['Prefix'] for p in _top)}"))
        except Exception as _e:
            _rows.append(line(False, f"R2 key in `.env`, but reading fails: {type(_e).__name__}: {str(_e)[:120]}"))
    else:
        _rows.append(line(False, "no R2 key in `.env` – needed for the backfill (notebook 04)", optional=True))
    mo.md("## Keys (`.env`, optional)\n\n" + "\n\n".join(_rows) +
          "\n\nCopy `.env.template` to `.env` and fill in what you have. `.env` is never committed.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## All green?

    Start with [01 – The first message](01_first_message.py). Each notebook has `TODO` cells and a check below each one;
    the complete versions are in `solutions/`.
    """)
    return


if __name__ == "__main__":
    app.run()
