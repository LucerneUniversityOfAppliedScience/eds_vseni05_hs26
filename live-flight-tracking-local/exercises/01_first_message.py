import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium", app_title="01 – The first message")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 01 – The first message

    The [OpenSky Network](https://opensky-network.org) collects the radio messages (ADS-B) that aircraft broadcast:
    an airborne aircraft sends its position and its velocity twice per second each
    ([transmission rates](https://mode-s.org/1090mhz/content/ads-b/1-basics.html)). The REST endpoint `/states/all`
    answers **one** question – *where is every aircraft in this box right now?* – with one JSON document. In this notebook you make that call once and take the answer apart.

    - API docs: <https://openskynetwork.github.io/opensky-api/rest.html>
    - Each call costs **credits**: a box of up to 25 square degrees costs 1 (our Switzerland box: 2.4° × 4.9° ≈ 12).
      Without an account you get 400 credits per day, counted **per IP address** – in a classroom all of you share
      one address, so do not call in a loop here. With a free account: 4,000 per day.
      ([API credits](https://openskynetwork.github.io/opensky-api/rest.html#api-credits))

    Work through the cells from top to bottom. Cells marked **TODO** are yours; the green/red box below each one tells
    you whether it works.
    """)
    return


@app.cell
def _(mo):
    import requests

    def check(ok, good, bad):
        """Green box if ok, red box with a hint otherwise."""
        return mo.callout(mo.md(("✅ " + good) if ok else ("❌ " + bad)), kind="success" if ok else "danger")

    URL = "https://opensky-network.org/api/states/all"
    SWITZERLAND = {"lamin": 45.6, "lamax": 48.0, "lomin": 5.8, "lomax": 10.7}
    return SWITZERLAND, URL, check, requests


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 1.1 – One API call

    Call `GET {URL}` with the bounding box `SWITZERLAND` as query parameters plus `extended=1` (adds the aircraft
    category). Use a timeout. Store the HTTP response in `response`, the parsed JSON in `data` and the number of
    credits you have left (response header `X-Rate-Limit-Remaining`) in `credits_left`.

    /// details | Hint
    `requests.get(URL, params={**SWITZERLAND, "extended": 1}, timeout=20)`, then `response.json()` and
    `response.headers.get(...)`. `raise_for_status()` turns an HTTP error into an exception instead of a silent
    wrong result.
    ///
    """)
    return


@app.cell
def _(SWITZERLAND, URL, requests):
    # TODO 1.1: call the API once – response, data, credits_left
    response = None
    data = {}
    credits_left = None
    return credits_left, data


@app.cell(hide_code=True)
def _(check, credits_left, data, mo):
    mo.vstack([
        check(isinstance(data, dict) and "time" in data and "states" in data,
              f"Answer with `time` = {data.get('time')} and {len(data.get('states') or [])} aircraft. "
              f"Credits left: {credits_left}",
              "`data` should be the parsed JSON (a dict with the keys `time` and `states`)."),
        mo.md("The raw answer (first 3 aircraft):"),
        mo.json({**data, "states": (data.get("states") or [])[:3]}),
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 1.2 – What is in one message?

    `time` is the moment of the snapshot (unix seconds). `states` holds one **state vector** per aircraft
    ([field list](https://openskynetwork.github.io/opensky-api/rest.html#response)) – and a
    state vector is a plain **list**, not a dict. The meaning comes from the position in the list:

    | index | field | | index | field |
    |---|---|---|---|---|
    | 0 | icao24 (transponder address) | | 9 | velocity (m/s) |
    | 1 | callsign (8 chars, e.g. `SWR123  `) | | 10 | true_track (direction of travel, ° clockwise from north) |
    | 2 | origin_country (derived from icao24) | | 11 | vertical_rate (m/s) |
    | 5 / 6 | longitude / latitude | | 13 | geo_altitude (m) |
    | 7 | baro_altitude (m) | | 14 | squawk |
    | 8 | on_ground | | 17 | category |

    Turn the **first** state vector into a dict `first` with names – use the list `FIELDS` below and `zip`.
    Remove the spaces around the callsign.
    """)
    return


@app.cell
def _():
    FIELDS = ["icao24", "callsign", "origin_country", "time_position", "last_contact", "longitude", "latitude",
              "baro_altitude", "on_ground", "velocity", "true_track", "vertical_rate", "sensors", "geo_altitude",
              "squawk", "spi", "position_source", "category"]
    return (FIELDS,)


@app.cell
def _(FIELDS, data):
    # TODO 1.2: the first state vector as a dict `first`, callsign without spaces
    first = {}
    first
    return (first,)


@app.cell(hide_code=True)
def _(check, data, first):
    _v = (data.get("states") or [[None] * 18])[0]
    check(bool(first) and first.get("icao24") == _v[0] and first.get("latitude") == _v[6] and first.get("callsign") == (_v[1] or "").strip(),
          f"{first.get('callsign') or first.get('icao24')} at {first.get('latitude')}, {first.get('longitude')}",
          "`first` should map every name of FIELDS to the value at the same index – `dict(zip(...))`.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 1.3 – Units for humans

    OpenSky sends metres and m/s. Aviation uses **feet** for altitude and **knots** for speed – ICAO Annex 5 permits
    both as non-SI units (Table 3-3, reproduced e.g. in
    [this national regulation](https://caa.rks-gov.net/wp-content/uploads/2016/05/CAA-Regulation-3-2013-Units-of-Measurement-to-be-used-in-Air-and-Ground-Operations.pdf)).
    For speed we prefer **km/h**. Write `to_human(state)` that returns a *new* dict with two more keys:

    - `altitude_ft` = baro_altitude × 3.28084, rounded (1 ft = 0.3048 m exactly). None stays None – aircraft on the
      ground almost never have a barometric altitude (in a 3-hour sample over Switzerland: 99.8 % of on-ground rows)
    - `speed_kmh` = velocity × 3.6, rounded (1 m/s = 3.6 km/h)
    """)
    return


@app.cell
def _():
    # TODO 1.3: to_human(state) -> dict with altitude_ft and speed_kmh (None-safe)
    def to_human(state):
        return dict(state)
    return (to_human,)


@app.cell(hide_code=True)
def _(check, first, to_human):
    _t = to_human({"baro_altitude": 10000.0, "velocity": 250.0})
    _n = to_human({"baro_altitude": None, "velocity": None})
    _h = to_human(first)
    check(_t.get("altitude_ft") == 32808 and _t.get("speed_kmh") == 900 and _n.get("altitude_ft") is None,
          f"{_h.get('callsign')}: {_h.get('altitude_ft')} ft, {_h.get('speed_kmh')} km/h",
          "10,000 m should give 32808 ft, 250 m/s 900 km/h – and None must stay None.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 1.4 – All aircraft as a table and on a map

    Build a pandas DataFrame `aircraft` with one row per state vector (names as columns, units added with
    `to_human`). Then look at the map below.
    """)
    return


@app.cell
def _(FIELDS, data, to_human):
    import pandas as pd

    # TODO 1.4: DataFrame `aircraft`, one row per state vector, with altitude_ft and speed_kmh
    aircraft = pd.DataFrame()
    aircraft
    return aircraft, pd


@app.cell(hide_code=True)
def _(aircraft, check, data, mo):
    import plotly.express as px

    _ok = len(aircraft) > 0 and len(aircraft) == len(data.get("states") or []) and {"latitude", "longitude", "altitude_ft"} <= set(aircraft.columns)
    _out = [check(_ok, f"{len(aircraft)} aircraft over Switzerland", "One row per state vector, with the columns "
                  "latitude, longitude and altitude_ft.")]
    if _ok:
        _fig = px.scatter_map(aircraft.dropna(subset=["latitude"]), lat="latitude", lon="longitude",
                              color="altitude_ft", hover_name="callsign", hover_data=["origin_country", "speed_kmh"],
                              zoom=6, height=520, map_style="carto-darkmatter", color_continuous_scale="Turbo")
        _out.append(_fig)
    mo.vstack(_out)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Think about it

    - How old is the snapshot? Compare `data["time"]` with `time.time()`. Why is it never exactly *now*?
    - Which aircraft have no altitude? (`aircraft[aircraft.altitude_ft.isna()]`) What do they have in common?
    - `origin_country` is derived from the transponder address, and each state assigns these addresses to the aircraft
      on **its register** ([SKYbrary](https://skybrary.aero/articles/24-bit-aircraft-address)) – so it is the country
      of registration, not where the aircraft flies from. Which countries do you see?
    - How many credits per day would one call every 35 seconds cost? Is that possible without an account?

    **Next:** [02 – Bronze in DuckDB](02_bronze_duckdb.py) – store this message instead of throwing it away.
    """)
    return


if __name__ == "__main__":
    app.run()
