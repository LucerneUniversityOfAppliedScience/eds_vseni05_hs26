import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium", app_title="07 – Routes and detours")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 07 – Routes and detours: great circle vs. the route really flown

    CO₂ calculators such as myclimate's and ICAO's start from the **great-circle distance** – the shortest path on the
    globe between two airports – and add a flat **distance correction**:

    - **myclimate:** + 95 km for every flight
      ([calculation principles](https://www.myclimate.org/en/information/about-myclimate/downloads/flight-emission-calculator/),
      section 2)
    - **ICAO:** + 50 km (< 550 km), + 100 km (550–5,500 km), + 125 km (> 5,500 km) for "stacking, traffic and
      weather-driven corrections"
      ([ICAO methodology v8, p. 8](https://www.icao.int/sites/default/files/sp-files/environmental-protection/CarbonOffset/Documents/Methodology%20ICAO%20Carbon%20Calculator_v8-2015.pdf))

    Real routes follow airways, go around weather and avoid conflict zones – EASA currently warns about the airspace of
    e.g. Ukraine, Russia, Iran and Iraq ([Conflict Zone Information Bulletins](https://www.easa.europa.eu/en/domains/air-operations/czibs)).
    In this notebook you measure the difference with your own positions.

    Data: a **snapshot** of `flights.duckdb` (`fl.snapshot()` – works while the app or the collector is running).
    The scheduled route of a callsign comes from [adsbdb](https://www.adsbdb.com). Its route data is the work of David
    Taylor and Jim Mason and may not be copied into other databases without permission
    ([adsbdb README](https://github.com/mrjackwills/adsbdb)) – so we show it, we do not redistribute it.
    """)
    return


@app.cell
def _(mo):
    import math
    import sys

    import pandas as pd
    import requests

    sys.path.insert(0, str(mo.notebook_dir().parent))
    import flightlab as fl
    from enrichment import jobs

    def check(ok, good, bad):
        return mo.callout(mo.md(("✅ " + good) if ok else ("❌ " + bad)), kind="success" if ok else "danger")

    con = fl.snapshot()
    fl.ensure(con, "flight_routes", "route_efficiency")                     # tables the app computes – here if you only ran collector.py
    for _name, _table in [("flight_routes", "silver.flight_routes"), ("route_efficiency", "gold.route_efficiency")]:
        if not con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema || '.' || table_name "
                           f"= '{_table}'").fetchone()[0]:
            jobs.run_job(con, _name, log=lambda *_: None)       # the app has not run yet: compute it in the copy
    _n = con.execute("SELECT COUNT(*), COUNT(DISTINCT icao24) FROM silver.flights").fetchone()
    mo.md(f"snapshot: **{_n[0]:,} positions** of {_n[1]:,} aircraft")
    return check, con, math, pd, requests


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 7.1 – Where does a flight go?

    adsbdb answers `GET https://api.adsbdb.com/v0/callsign/<CALLSIGN>` with the scheduled route of a callsign.
    Write `lookup_route(callsign)` that returns a dict with `origin_name`, `origin_lat`, `origin_lon`,
    `destination_name`, `destination_lat`, `destination_lon` – or `None` when adsbdb does not know the callsign
    (HTTP 404, or no `flightroute` in the answer).

    /// details | Hint
    The answer looks like `{"response": {"flightroute": {"origin": {"name": ..., "latitude": ..., "longitude": ...},
    "destination": {...}}}}`. Send a `User-Agent` header and a timeout.
    ///
    """)
    return


@app.cell
def _(requests):
    # >>> TODO 7.1: lookup_route(callsign) -> dict with origin_/destination_ name, lat, lon – or None
    # ?? def lookup_route(callsign):
    # ??     return None
    def lookup_route(callsign):
        r = requests.get(f"https://api.adsbdb.com/v0/callsign/{callsign}", timeout=10,
                         headers={"User-Agent": "live-flight-tracking-local (teaching)"})
        if r.status_code == 404:
            return None
        r.raise_for_status()
        fr = (r.json().get("response") or {}).get("flightroute") if isinstance(r.json().get("response"), dict) else None
        if not fr:
            return None
        o, d = fr["origin"], fr["destination"]
        return {"origin_name": o["name"], "origin_lat": o["latitude"], "origin_lon": o["longitude"],
                "destination_name": d["name"], "destination_lat": d["latitude"], "destination_lon": d["longitude"]}
    # <<< TODO 7.1
    return (lookup_route,)


@app.cell(hide_code=True)
def _(check, lookup_route):
    _r = lookup_route("SWR8")
    check(isinstance(_r, dict) and "rich" in str(_r.get("origin_name")) + str(_r.get("destination_name"))
          and lookup_route("XXX0000") is None,
          f"SWR8: {_r and _r['origin_name']} → {_r and _r['destination_name']}; an unknown callsign gives None",
          "lookup_route('SWR8') should return a dict with a Zurich airport, an unknown callsign None.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 7.2 – The great circle

    Write `haversine_km(lat1, lon1, lat2, lon2)`: the distance on a sphere with radius 6,371 km.

    $$d = 2R \cdot \arcsin\sqrt{\sin^2\frac{\varphi_2-\varphi_1}{2} + \cos\varphi_1 \cos\varphi_2 \sin^2\frac{\lambda_2-\lambda_1}{2}}$$

    (φ = latitude, λ = longitude, both in **radians**.)
    """)
    return


@app.cell
def _(math):
    # >>> TODO 7.2: haversine_km(lat1, lon1, lat2, lon2) -> km
    # ?? def haversine_km(lat1, lon1, lat2, lon2):
    # ??     return 0.0
    def haversine_km(lat1, lon1, lat2, lon2):
        p1, p2 = math.radians(lat1), math.radians(lat2)
        dp, dl = p2 - p1, math.radians(lon2 - lon1)
        a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
        return 2 * 6371 * math.asin(math.sqrt(a))
    # <<< TODO 7.2
    return (haversine_km,)


@app.cell(hide_code=True)
def _(check, haversine_km):
    _zrh_nrt = haversine_km(47.4581, 8.5481, 35.7686, 140.3887)      # LSZH, RJAA: coordinates from OurAirports (06)
    check(9500 < _zrh_nrt < 9650, f"Zurich – Tokyo Narita: {_zrh_nrt:,.0f} km on the great circle",
          f"Zurich – Tokyo Narita should be about 9,595 km, got {_zrh_nrt:,.0f}.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 7.3 – The distance really flown

    Pick a flight from the table and compute `flown_km`: the sum of the distances between **consecutive** positions
    of its track (ordered by time). Our track only covers the part over the region – so this is a **lower bound**
    of the real flight.
    """)
    return


@app.cell
def _(con, mo):
    flights = con.sql("""
        SELECT callsign, origin_name, destination_name, ROUND(gcd_km) AS great_circle_km, ROUND(extra_km) AS extra_km,
               route_status, icao24
        FROM gold.route_efficiency WHERE route_status <> 'contradicted' ORDER BY extra_km DESC""").df()
    pick = mo.ui.table(flights, selection="single", page_size=8, label="Pick a flight (default: the first)")
    pick
    return flights, pick


@app.cell
def _(con, flights, haversine_km, pick):
    _row = (pick.value if len(pick.value) else flights).iloc[0]
    track = con.execute("SELECT snapshot_time, latitude, longitude FROM silver.flights "
                        "WHERE icao24 = ? AND trim(callsign) = ? ORDER BY snapshot_time",
                        [_row["icao24"], _row["callsign"]]).df()
    chosen = _row

    # >>> TODO 7.3: flown_km = sum of haversine_km between consecutive track positions
    # ?? flown_km = 0.0
    flown_km = sum(haversine_km(a.latitude, a.longitude, b.latitude, b.longitude)
                   for a, b in zip(track.itertuples(), track.iloc[1:].itertuples()))
    # <<< TODO 7.3
    return chosen, flown_km, track


@app.cell(hide_code=True)
def _(check, chosen, flown_km, haversine_km, track):
    _direct = haversine_km(track.latitude.iloc[0], track.longitude.iloc[0], track.latitude.iloc[-1],
                           track.longitude.iloc[-1]) if len(track) > 1 else 0
    check(len(track) > 1 and flown_km >= _direct * 0.999 and flown_km > 0,
          f"{chosen['callsign']}: {len(track)} positions, flown over our region {flown_km:,.0f} km "
          f"(straight line between first and last position: {_direct:,.0f} km)",
          "flown_km should be > 0 and at least the straight line between the first and the last position.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 7.4 – Detour vs. the calculators

    `gold.route_efficiency` has the full calculation for every flight with a known route (the app computes it): the
    great circle `gcd_km`, the estimated route `estimated_route_km` (great circle outside our region + the flown part
    inside) and `extra_km`. Add two columns to `compare`:

    - `myclimate_ok`: is the measured detour **covered** by myclimate's +95 km?
    - `icao_allowance_km`: 50 / 100 / 125 km by great-circle distance (< 550, < 5,500, longer)
    """)
    return


@app.cell
def _(con):
    compare = con.sql("""
        SELECT callsign, airline_name, origin_name, destination_name, ROUND(gcd_km) AS gcd_km,
               ROUND(extra_km) AS extra_km, ROUND(extra_pct, 1) AS extra_pct, route_status
        FROM gold.route_efficiency WHERE route_status <> 'contradicted'""").df()

    # >>> TODO 7.4: compare["myclimate_ok"] and compare["icao_allowance_km"]
    # ?? pass
    compare["myclimate_ok"] = compare["extra_km"] <= 95
    compare["icao_allowance_km"] = compare["gcd_km"].map(lambda d: 50 if d < 550 else 100 if d < 5500 else 125)
    # <<< TODO 7.4
    compare.sort_values("extra_km", ascending=False)
    return (compare,)


@app.cell(hide_code=True)
def _(check, compare, mo):
    _ok = {"myclimate_ok", "icao_allowance_km"} <= set(compare.columns)
    _out = [check(_ok and set(compare["icao_allowance_km"].unique()) <= {50, 100, 125},
                  f"{int((~compare['myclimate_ok']).sum()) if _ok else 0} of {len(compare)} flights flew a longer "
                  f"detour than myclimate assumes",
                  "Add myclimate_ok (extra_km <= 95) and icao_allowance_km (50/100/125).")]
    if _ok and len(compare):
        import plotly.express as px
        _out.append(px.scatter(compare, x="gcd_km", y="extra_km", color="route_status", hover_name="callsign",
                               hover_data=["origin_name", "destination_name"], height=360,
                               labels={"gcd_km": "great circle (km)", "extra_km": "measured detour (km)"})
                    .add_hline(y=95, line_dash="dash", annotation_text="myclimate +95 km"))
    mo.vstack(_out)
    return


@app.cell
def _(chosen, con, mo, pd, track):
    import plotly.graph_objects as go

    _r = con.execute("SELECT origin_lat, origin_lon, destination_lat, destination_lon FROM gold.route_efficiency "
                     "WHERE icao24 = ? AND callsign = ?", [chosen["icao24"], chosen["callsign"]]).fetchone()
    _fig = go.Figure()
    if _r:
        _fig.add_trace(go.Scattergeo(lat=[_r[0], _r[2]], lon=[_r[1], _r[3]], mode="lines+markers",
                                     line=dict(dash="dash", color="orange"), name="great circle"))
    _fig.add_trace(go.Scattergeo(lat=track.latitude, lon=track.longitude, mode="lines", line=dict(color="deepskyblue",
                                 width=3), name="flown (our data)"))
    _fig.update_geos(projection_type="natural earth", showcountries=True, fitbounds="locations")
    _fig.update_layout(height=420, margin=dict(l=0, r=0, t=30, b=0),
                       title=f"{chosen['callsign']}: {chosen['origin_name']} → {chosen['destination_name']}")
    mo.vstack([mo.md("## The chosen flight"), _fig])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Think about it

    - Which flights have the biggest detours? Look at origin and destination – which airspace did they avoid?
    - Status `contradicted`: our positions show the aircraft somewhere the scheduled route cannot be (callsigns are
      reused). Why must those be left out?
    - For a flight Zurich–Tokyo with 1,500 km extra: by how much does a calculator with +95 km underestimate the CO₂?

    **Next:** [08 – CO₂](08_co2.py)
    """)
    return


if __name__ == "__main__":
    app.run()
