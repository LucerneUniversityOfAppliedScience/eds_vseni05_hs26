import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium", app_title="08 – CO2")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 08 – CO₂: how much do the aircraft above us burn?

    ADS-B does not tell us the fuel flow. But we know **type** (from the transponder address and the OpenSky aircraft
    database), **altitude**, **speed** and **climb rate** – and an aircraft performance model turns those into an
    estimate: [OpenAP](https://openap.dev) from TU Delft (open source, [LGPL v3](https://github.com/junzis/openap)).

    - 1 kg of jet fuel burnt = **3.16 kg CO₂** – the factor of ICAO
      ([methodology v8, p. 6](https://www.icao.int/sites/default/files/sp-files/environmental-protection/CarbonOffset/Documents/Methodology%20ICAO%20Carbon%20Calculator_v8-2015.pdf)),
      [myclimate](https://www.myclimate.org/en/information/about-myclimate/downloads/flight-emission-calculator/)
      and OpenAP's own `Emission.co2()` (3,160 g per kg)
    - Unknown: the real mass (passengers, cargo, fuel on board) and the wind. **Our assumption** (in
      `enrichment/fuel_co2.py`, not part of OpenAP): a mass halfway between operating empty weight and maximum take-off
      weight of the type. Treat the numbers as an **order of magnitude**.
    """)
    return


@app.cell
def _(mo):
    import sys

    sys.path.insert(0, str(mo.notebook_dir().parent))
    import flightlab as fl
    from enrichment import jobs

    def check(ok, good, bad):
        return mo.callout(mo.md(("✅ " + good) if ok else ("❌ " + bad)), kind="success" if ok else "danger")

    con = fl.snapshot()
    fl.ensure(con, "aircraft_types", "fuel_burn")                     # tables the app computes – here if you only ran collector.py
    if not con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'aircraft_types'").fetchone()[0]:
        with mo.status.spinner(title="downloading the OpenSky aircraft database (~108 MB, once) ..."):
            jobs.run_job(con, "aircraft_types", log=lambda *_: None)
    _n = con.execute("SELECT COUNT(*) FROM silver.aircraft_types").fetchone()[0]
    mo.md(f"aircraft database: **{_n:,} airframes** (icao24 -> registration, type, operator)")
    return check, con, jobs


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 8.1 – Which aircraft types are flying right now?

    Join `gold.current_positions` with `silver.aircraft_types` on `icao24` and count the **airborne** aircraft per
    `typecode` (most first). Store the result in `types_now`.
    """)
    return


@app.cell
def _(con, mo):
    # >>> TODO 8.1: types_now = airborne aircraft per typecode right now (most first)
    # ?? types_now = mo.sql("SELECT 'TODO' AS typecode, 0 AS aircraft", engine=con)
    types_now = mo.sql(
        """
        SELECT t.typecode, ANY_VALUE(t.model) AS model, COUNT(*) AS aircraft
        FROM gold.current_positions p
        JOIN silver.aircraft_types t USING (icao24)
        WHERE NOT p.on_ground AND t.typecode IS NOT NULL
        GROUP BY t.typecode
        ORDER BY aircraft DESC
        """,
        engine=con,
    )
    # <<< TODO 8.1
    return (types_now,)


@app.cell(hide_code=True)
def _(check, types_now):
    _top = types_now.iloc[0] if len(types_now) else None
    check(len(types_now) > 5 and _top is not None and _top["typecode"] != "TODO" and
          types_now["aircraft"].is_monotonic_decreasing,
          f"{len(types_now)} types in the air, most common: {_top['typecode']} ({_top['aircraft']} aircraft)"
          if _top is not None else "",
          "Expected one row per typecode with the number of airborne aircraft, most first.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 8.2 – Fuel flow of one aircraft

    OpenAP models many types: `openap.FuelFlow("A320").enroute(mass=..., tas=..., alt=..., vs=...)` returns kg per
    **second** (mass in kg, true airspeed `tas` in knots, altitude `alt` in feet, vertical speed `vs` in ft/min – see
    the docstring in [openap/fuel.py](https://github.com/junzis/openap/blob/master/openap/fuel.py)).

    Compute `fuel_kg_h` and `co2_kg_h` for an **A320 in cruise**: mass 65,000 kg, 450 kt, 36,000 ft, vs 0. The inputs
    are our choice, within OpenAP's A320 data (`openap.prop.aircraft("A320")`): empty weight 42,600 kg, maximum
    take-off weight 78,000 kg, cruise Mach 0.78 – which is about 450 kt true airspeed at 36,000 ft in the standard
    atmosphere.
    """)
    return


@app.cell
def _():
    import openap

    # >>> TODO 8.2: fuel_kg_h and co2_kg_h of an A320 in cruise (65 t, 450 kt, 36,000 ft)
    # ?? fuel_kg_h = 0.0
    # ?? co2_kg_h = 0.0
    fuel_kg_h = float(openap.FuelFlow("A320").enroute(mass=65000, tas=450, alt=36000, vs=0)) * 3600
    co2_kg_h = fuel_kg_h * 3.16
    # <<< TODO 8.2
    return co2_kg_h, fuel_kg_h


@app.cell(hide_code=True)
def _(check, co2_kg_h, fuel_kg_h):
    check(1800 < fuel_kg_h < 3200 and abs(co2_kg_h - fuel_kg_h * 3.16) < 1,
          f"A320 in cruise: {fuel_kg_h:,.0f} kg fuel and {co2_kg_h:,.0f} kg CO₂ per hour – if we assume 150 "
          f"passengers, {co2_kg_h / 150:,.0f} kg CO₂ per person and hour",
          f"OpenAP gives about 2,660 kg fuel per hour for these inputs (got {fuel_kg_h:,.0f}) and CO₂ = 3.16 × fuel.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 8.3 – CO₂ by airline

    The app computes the estimate for every position of the last hour into **`gold.fuel_burn`** (same model, mass
    assumption per type; `co2_kg` = CO₂ of the interval until the next snapshot). Sum `co2_kg` per airline name
    (join `silver.airlines` on `airline_icao`; `unknown / private` when there is none) and show the top 10 in
    `by_airline`, in **tonnes**.
    """)
    return


@app.cell
def _(con, jobs, mo):
    if not con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'fuel_burn'").fetchone()[0]:
        jobs.run_job(con, "fuel_burn", log=lambda *_: None)

    # >>> TODO 8.3: by_airline = CO2 in tonnes per airline of the last hour (top 10)
    # ?? by_airline = mo.sql("SELECT 'TODO' AS airline, 0 AS co2_t", engine=con)
    by_airline = mo.sql(
        """
        SELECT COALESCE(a.airline_name, 'unknown / private') AS airline,
               ROUND(SUM(b.co2_kg) / 1000, 2)                AS co2_t,
               COUNT(DISTINCT b.icao24)                      AS aircraft
        FROM gold.fuel_burn b
        LEFT JOIN silver.airlines a USING (airline_icao)
        WHERE b.co2_kg IS NOT NULL
        GROUP BY ALL
        ORDER BY co2_t DESC
        LIMIT 10
        """,
        engine=con,
    )
    # <<< TODO 8.3
    return (by_airline,)


@app.cell(hide_code=True)
def _(by_airline, check, con, mo):
    _total = con.execute("SELECT SUM(co2_kg) / 1000 FROM gold.fuel_burn").fetchone()[0] or 0
    _ok = len(by_airline) > 3 and "co2_t" in by_airline and by_airline["co2_t"].iloc[0] > 0
    _out = [check(_ok, f"{_total:,.1f} t CO₂ over the region in the last hour – top: {by_airline['airline'].iloc[0]}"
                  if _ok else "", "Expected the top airlines with their CO₂ in tonnes (column co2_t).")]
    if _ok:
        import plotly.express as px
        _out.append(px.bar(by_airline, x="co2_t", y="airline", orientation="h", height=340,
                           labels={"co2_t": "t CO₂ (last hour, estimate)", "airline": ""})
                    .update_yaxes(autorange="reversed"))
    mo.vstack(_out)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Think about it

    - Which share of the airborne aircraft could OpenAP model at all? (`fuel_kg_per_h IS NULL` in `gold.fuel_burn`)
      Which types are missing – and what does that do to the totals?
    - We assume a mass halfway between empty and maximum. How would a full long-haul aircraft at departure
      change the result?
    - Combine with 07: `extra_co2_kg` in `gold.route_efficiency` is the CO₂ of the detour. Which flight's detour costs
      the most?

    **Next:** [09 – Weather and contrails](09_weather_contrails.py)
    """)
    return


if __name__ == "__main__":
    app.run()
