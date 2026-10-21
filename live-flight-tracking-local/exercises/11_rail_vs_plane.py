import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium", app_title="11 – Plane or train?")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 11 – Plane or train? CO₂ per person

    Many flights from Zurich go to cities you can also reach by train: Milan, Munich, Paris, Amsterdam, Vienna
    (about 3 to 8.5 hours, see `data/rail_times_zurich.csv`). In this notebook you take the routes **observed in
    your data**, add the rail journey time and compare the CO₂ **per person** for plane, train and car.

    Three data sources:

    - **Routes:** `silver.flight_routes` (adsbdb, as in 07 and 10) joined with your positions.
    - **Rail times:** `data/rail_times_zurich.csv` – the fastest connection from Zürich HB, queried once from the ÖBB
      timetable (2026-10-20, departures from 07:00 on Thu 2026-10-22). A small static file, because we know of no
      free timetable API for international trains that 25 students can call at the same time.
    - **Emission factors:** `data/mobitool_factors.csv` – mobitool factors v3.1 (EnergieSchweiz, PSI; today published
      as [Umweltrechner Verkehr](https://www.energieschweiz.ch/programme/umweltrechner-verkehr/)): g CO₂-equivalent
      per **passenger-km**, life cycle (direct emissions, energy supply, maintenance, vehicle, end of life,
      infrastructure).
    """)
    return


@app.cell
def _(mo):
    import sys

    sys.path.insert(0, str(mo.notebook_dir().parent))
    import flightlab as fl
    from enrichment import jobs
    import pandas as pd
    import plotly.express as px

    def check(ok, good, bad):
        return mo.callout(mo.md(("✅ " + good) if ok else ("❌ " + bad)), kind="success" if ok else "danger")

    con = fl.snapshot()
    fl.ensure(con, "aircraft_types", "flight_routes")                     # tables the app computes – here if you only ran collector.py
    for _job, _table in [("aircraft_types", "aircraft_types"), ("flight_routes", "flight_routes")]:
        if not con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{_table}'").fetchone()[0]:
            with mo.status.spinner(title=f"computing {_table} in your copy (once) ..."):
                jobs.run_job(con, _job, log=lambda *_: None)
    _n = con.execute("SELECT COUNT(*) FROM silver.flight_routes").fetchone()[0]
    mo.md(f"**{_n:,} callsigns** with a known route in `silver.flight_routes`")
    return check, con, pd, px


@app.cell
def _(con, mo, pd):
    # Static reference data (see the header of each CSV for source, date and licence)
    rail = pd.read_csv(mo.notebook_dir().parent / "data" / "rail_times_zurich.csv", comment="#")
    factors = pd.read_csv(mo.notebook_dir().parent / "data" / "mobitool_factors.csv", comment="#")
    con.register("rail_times", rail)                 # usable in SQL as the table rail_times
    FACTOR = dict(zip(factors["key"], factors["g_co2e_per_pkm"]))           # g CO2-eq per passenger-km
    mo.vstack([mo.md(f"**{rail.city.nunique()} cities, {len(rail)} airports** with a rail time from Zürich HB"),
               factors[["key", "description", "g_co2e_per_pkm", "direct_g_co2e_per_pkm", "load_factor"]]])
    return FACTOR, factors, rail


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 11.1 – Which observed Zurich flights have a rail alternative?

    Find the flights **from or to Zurich (LSZH)** in your data whose other airport is in `rail_times`:

    1. Different flights = distinct `icao24` + `trim(callsign)` in `silver.flights` (Swiss feed), joined with
       `silver.flight_routes` on `callsign`, where `origin_icao = 'LSZH'` or `destination_icao = 'LSZH'`.
    2. The **other** airport: `CASE WHEN origin_icao = 'LSZH' THEN destination_icao ELSE origin_icao END`.
    3. Join `rail_times` on `airport_icao` and `silver.airports` (coordinates of both airports).
    4. Per `city` and `airport_icao`: number of `flights`, great-circle `distance_km` LSZH → airport,
       `rail_minutes`, `transfers`, `country`.

    Store it in `zrh_routes`, most flights first. Both directions count.

    /// details | Hint
    The haversine formula from notebook 06/10, with LSZH = (47.4581, 8.5481) (airport reference point from
    [OurAirports](https://ourairports.com/data/), like `silver.airports`) and `a.latitude`, `a.longitude` from
    `silver.airports`:
    ```sql
    2 * 6371 * ASIN(SQRT(POWER(SIN(RADIANS(a.latitude - 47.4581) / 2), 2)
        + COS(RADIANS(47.4581)) * COS(RADIANS(a.latitude)) * POWER(SIN(RADIANS(a.longitude - 8.5481) / 2), 2)))
    ```
    `GROUP BY ALL` with `ANY_VALUE(...)` for the columns that are the same within a group.
    ///
    """)
    return


@app.cell
def _(con, mo):
    # TODO 11.1: zrh_routes = observed LSZH flights to/from a city in rail_times: city, airport_icao, flights, distance_km, rail_minutes, transfers, country
    zrh_routes = mo.sql("SELECT 'todo' AS city, 'XXXX' AS airport_icao, 0 AS flights, 0.0 AS distance_km, 0 AS rail_minutes, 0 AS transfers, 'CH' AS country", engine=con)
    return (zrh_routes,)


@app.cell(hide_code=True)
def _(check, con, zrh_routes):
    try:
        _all = con.execute("""
            WITH seen AS (SELECT DISTINCT icao24, trim(callsign) AS callsign FROM silver.flights
                          WHERE region = 'switzerland' AND callsign IS NOT NULL)
            SELECT COUNT(*) FROM seen JOIN silver.flight_routes r USING (callsign)
            WHERE r.origin_icao = 'LSZH' OR r.destination_icao = 'LSZH'""").fetchone()[0]
    except Exception:
        _all = 0
    _cols = {"city", "airport_icao", "flights", "distance_km", "rail_minutes", "transfers", "country"}
    _ok = (_cols <= set(zrh_routes.columns) and len(zrh_routes) > 0 and "todo" not in set(zrh_routes["city"])
           and zrh_routes["flights"].is_monotonic_decreasing and zrh_routes["distance_km"].between(100, 2000).all())
    _n = int(zrh_routes["flights"].sum()) if _ok else 0
    check(_ok,
          f"{_n} of {_all} observed Zurich flights with a known route ({_n / max(_all, 1):.0%}) go to "
          f"{zrh_routes['city'].nunique() if _ok else 0} cities you can reach by train – most often "
          f"{zrh_routes['city'].iloc[0] if _ok else ''} ({zrh_routes['distance_km'].iloc[0] if _ok else 0:.0f} km, "
          f"{zrh_routes['rail_minutes'].iloc[0] / 60 if _ok else 0:.1f} h by train)",
          "Expected one row per city/airport with flights, distance_km (100–2,000 km), rail_minutes, transfers, "
          "country – most flights first. Few rows? The app looks up routes every 10 minutes; let it run longer.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 11.2 – CO₂ per person: plane, train, car

    mobitool gives g CO₂-eq per **passenger-km**. Multiply by the distance each mode really travels:

    | mode | distance | factor (g/pkm) |
    |---|---|---|
    | plane | great circle × **1.05** (detour – our assumption, compare the detours you measured in 07) | `FACTOR["plane_europe"]` (319.2) |
    | train | great circle × **1.2** (tracks are not straight – our assumption) | train factor of the destination country, see `RAIL_KEY` below |
    | car | great circle × **1.2** (our assumption) | `FACTOR["car_fleet_average"]` (186.4, already per person: mobitool assumes 1.6 occupants) |

    Add the columns `plane_kg`, `train_kg` and `car_kg` (kg CO₂-eq per person, one way) to a copy of `zrh_routes`
    and store it in `co2_trip`.

    `RAIL_KEY` maps a country to a mobitool train factor. mobitool has passenger train factors for CH, DE, FR, IT
    and AT only; for
    the other countries we take the German average as a proxy. Simplification: the whole trip gets the factor of the
    destination country – the part in Switzerland (7 g/pkm) is cleaner.

    /// details | Hint
    ```python
    co2_trip = zrh_routes.copy()
    co2_trip["plane_kg"] = co2_trip["distance_km"] * 1.05 * FACTOR["plane_europe"] / 1000
    _train_g = co2_trip["country"].map(RAIL_KEY).fillna("train_DE").map(FACTOR)
    ```
    ///
    """)
    return


@app.cell
def _():
    RAIL_KEY = {"CH": "train_CH", "DE": "train_DE_highspeed", "FR": "train_FR_highspeed",
                "IT": "train_IT_highspeed", "AT": "train_AT"}           # other countries: train_DE (proxy)
    return (RAIL_KEY,)


@app.cell
def _(FACTOR, RAIL_KEY, zrh_routes):
    # TODO 11.2: co2_trip = zrh_routes + plane_kg, train_kg, car_kg (kg CO2-eq per person, one way)
    co2_trip = zrh_routes.assign(plane_kg=0.0, train_kg=0.0, car_kg=0.0)
    return (co2_trip,)


@app.cell(hide_code=True)
def _(FACTOR, check, co2_trip, mo, px):
    _out = []
    try:
        _c = co2_trip.iloc[0]
        _ok = (abs(_c["plane_kg"] - _c["distance_km"] * 1.05 * FACTOR["plane_europe"] / 1000) < 0.5
               and abs(_c["car_kg"] - _c["distance_km"] * 1.2 * FACTOR["car_fleet_average"] / 1000) < 0.5
               and (co2_trip["train_kg"] > 0).all() and (co2_trip["train_kg"] < co2_trip["plane_kg"]).all())
    except Exception:
        _ok = False
    if _ok:
        _f = co2_trip["flights"]
        _plane = (co2_trip["plane_kg"] * _f).sum()
        _train = (co2_trip["train_kg"] * _f).sum()
        _out.append(check(True, f"{_c['city']}: {_c['plane_kg']:.0f} kg by plane, {_c['train_kg']:.1f} kg by train, "
                                f"{_c['car_kg']:.0f} kg by car per person. If one passenger on each of the "
                                f"{int(_f.sum())} observed flights had taken the train: {_train:,.0f} kg "
                                f"instead of {_plane:,.0f} kg CO₂-eq – {1 - _train / _plane:.0%} less.", ""))
        _long = co2_trip.melt(id_vars="city", value_vars=["plane_kg", "car_kg", "train_kg"], var_name="mode",
                              value_name="kg").drop_duplicates(["city", "mode"])
        _out.append(px.bar(_long, x="city", y="kg", color="mode", barmode="group", height=340,
                           labels={"kg": "kg CO₂-eq per person, one way", "city": ""}))
    else:
        _out.append(check(False, "", "Expected plane_kg = distance × 1.05 × 319.2 / 1000, car_kg = distance × 1.2 × "
                                     "186.4 / 1000 and a train_kg > 0 that is smaller than plane_kg in every row."))
    mo.vstack(_out)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 11.3 – Seats and a second estimate: OpenAP

    mobitool is an **average** European flight. The aircraft in your data differ: a SWISS A220-300 has 145 seats,
    an A380 has 620 in OpenAP's `high` layout. Make your own estimate per aircraft type from the OpenAP model
    (notebook 08):

    1. **Seats.** `openap.prop.aircraft(typecode)["pax"]` returns `{'max', 'low', 'high'}` (A320: 180, 140, 170).
       OpenAP does not document what the three numbers mean ([docs](https://openap.dev/aircraft_engine.html)); we
       take `high` as a typical, fairly dense layout – our choice. OpenAP 2.4 does not know some types that fly from
       Zurich every day – `SEAT_OVERRIDES` below has seat numbers for them from operator and manufacturer pages
       (SWISS A220, Helvetic E2, turboprops; sources in the code). Write `seats(typecode)`: override first, then
       OpenAP **without** `use_synonym` (with it, OpenAP 2.4 maps the ATR 72 turboprop to an Embraer 145 jet with
       50 seats), `None` when unknown.
    2. **CO₂ per passenger-km.** `cruise_co2_kg_per_km(typecode)` (given) returns the CO₂ of one km in cruise.
       Divide by the passengers: `seats × load_factor` (slider; mobitool assumes 75.7 % for flights within Europe,
       column `load_factor`).

    Store one row per type flown from/to Zurich in `per_type` with `typecode`, `flights`, `seats`,
    `g_co2_per_pkm` (NaN when there is no seat number or fuel model). `zrh_types` (below) lists the types.
    """)
    return


@app.cell
def _(mo):
    load_factor = mo.ui.slider(0.5, 1.0, step=0.05, value=0.8, label="Load factor (share of seats occupied)")
    load_factor
    return (load_factor,)


@app.cell
def _(con, mo):
    import warnings
    import openap

    warnings.filterwarnings("ignore", module="openap")
    # Seats of types OpenAP 2.4 does not know – not from OpenAP:
    #   BCS3/BCS1 = SWISS A220-300/-100 (https://www.swiss.com/ch/en/discover/fleet/airbus-shorthaul)
    #   E290/E295 = Helvetic E190-E2/E195-E2, flying for SWISS (https://www.helvetic.com/en/fleet)
    #   DH8D = De Havilland standard layout, 82 seats at 30 in pitch (Dash 8-400 spec sheet, dehavilland.com)
    #   AT76 = ATR 72-600 typical layout, 72 seats at 29 in pitch (https://www.atr-aircraft.com/our-aircraft/atr-72-600/)
    SEAT_OVERRIDES = {"BCS3": 145, "BCS1": 125, "E290": 110, "E295": 134, "DH8D": 82, "AT76": 72}
    NO_FUEL_MODEL = {"AT76", "DH8D", "AT75", "AT72"}       # turboprops: not in OpenAP 2.4, its ATR synonym is a jet (E145)

    def cruise_co2_kg_per_km(typecode):
        """CO2 (kg) per km in cruise: OpenAP fuel flow at 36,000 ft, 450 kt, mass halfway between OEW and MTOW."""
        if typecode in NO_FUEL_MODEL:
            return None
        try:
            _ac = openap.prop.aircraft(typecode, use_synonym=True)
            _mass = _ac["oew"] + 0.5 * (_ac["mtow"] - _ac["oew"])
            _kg_s = float(openap.FuelFlow(typecode, use_synonym=True).enroute(mass=_mass, tas=450, alt=36000, vs=0))
            return _kg_s * 3.16 / (450 * 1.852 / 3600)      # kg CO2 per s / km per s (3.16 kg CO2 per kg fuel, ICAO)
        except Exception:
            return None

    zrh_types = mo.sql(
        """
        WITH seen AS (SELECT DISTINCT icao24, trim(callsign) AS callsign FROM silver.flights
                      WHERE region = 'switzerland' AND callsign IS NOT NULL)
        SELECT t.typecode, COUNT(*) AS flights
        FROM seen s JOIN silver.flight_routes r USING (callsign)
        JOIN silver.aircraft_types t USING (icao24)
        WHERE (r.origin_icao = 'LSZH' OR r.destination_icao = 'LSZH') AND t.typecode IS NOT NULL
        GROUP BY ALL ORDER BY flights DESC
        """,
        engine=con,
    )
    return SEAT_OVERRIDES, cruise_co2_kg_per_km, openap, zrh_types


@app.cell
def _(SEAT_OVERRIDES, cruise_co2_kg_per_km, load_factor, openap, pd, zrh_types):
    # TODO 11.3: seats(typecode) and per_type = typecode, flights, seats, g_co2_per_pkm
    def seats(typecode):
        return None
    per_type = zrh_types.assign(seats=None, g_co2_per_pkm=float("nan"))
    return per_type, seats


@app.cell(hide_code=True)
def _(FACTOR, check, factors, load_factor, mo, pd, per_type, px, seats):
    _out = []
    try:
        _a320 = float(per_type.set_index("typecode")["g_co2_per_pkm"].get("A320", float("nan")))
        _ok = (seats("A320") == 170 and seats("BCS3") == 145 and seats("AT76") == 72 and seats("ZZZZ") is None
               and {"typecode", "flights", "seats", "g_co2_per_pkm"} <= set(per_type.columns)
               and per_type["g_co2_per_pkm"].notna().any())
    except Exception:
        _ok, _a320 = False, float("nan")
    if _ok:
        _known = per_type.dropna(subset=["g_co2_per_pkm"])
        _mean = (_known["g_co2_per_pkm"] * _known["flights"]).sum() / _known["flights"].sum()
        _direct = float(factors.set_index("key").loc["plane_europe", "direct_g_co2e_per_pkm"])
        _out.append(check(True, f"OpenAP, cruise only, {load_factor.value:.0%} occupied: "
                                f"{_mean:.0f} g CO₂ per passenger-km (flight-weighted, {len(_known)} of "
                                f"{len(per_type)} types modelled; A320 {_a320:.0f} g). mobitool: {_direct:.0f} g "
                                f"direct, {FACTOR['plane_europe']:.0f} g life cycle – "
                                f"{_direct / _mean:.1f}× the cruise CO₂.", ""))
        _cmp = pd.DataFrame({"estimate": ["OpenAP cruise CO₂ (your data)", "mobitool direct", "mobitool life cycle"],
                             "g_per_pkm": [_mean, _direct, FACTOR["plane_europe"]]})
        _out.append(px.bar(_cmp, x="g_per_pkm", y="estimate", orientation="h", height=220,
                           labels={"g_per_pkm": "g CO₂(-eq) per passenger-km", "estimate": ""}))
        _out.append(per_type)
    else:
        _out.append(check(False, "", "Expected seats('A320') = 170 (OpenAP 'high'), seats('BCS3') = 145 and "
                                     "seats('AT76') = 72 (overrides), seats('ZZZZ') = None – and per_type with "
                                     "g_co2_per_pkm = cruise_co2_kg_per_km × 1000 / (seats × load factor)."))
    mo.vstack(_out)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Why is mobitool so much higher?

    - **Cruise only.** OpenAP above models level flight at 36,000 ft. Climbing burns much more per km: in OpenAP's
      own model an A320 climbing at 5,000–25,000 ft uses about 2 to 4 times the fuel per km of cruise (our
      computation with `openap.FuelFlow`, same mass). Taxiing is missing. On a short flight the climb is a large
      part of the trip.
    - **Real operations** (not quantified here): holding, detours, lower cruise altitudes, heavier aircraft (fuel,
      cargo), older engines. OpenAP's A320neo also looks optimistic: in cruise it burns about 45 % less per seat than
      OpenAP's A320 (our computation), Airbus states [at least 20 %](https://aircraft.airbus.com/en/aircraft/a320-family/a320neo).
    - **Life cycle:** mobitool adds the energy supply (kerosene production, 45 g) and airports, aircraft and
      maintenance (15 g) – the step from "direct" (259 g) to "life cycle" (319 g, columns of the mobitool file).
    - **Non-CO₂ effects are already in mobitool.** Burning 1 kg of kerosene gives 3.16 kg CO₂
      ([ICAO methodology](https://www.icao.int/sites/default/files/sp-files/environmental-protection/CarbonOffset/Documents/Methodology%20ICAO%20Carbon%20Calculator_v8-2015.pdf)).
      mobitool's flight factors (319 g within Europe, 237 g intercontinental) match the "mobitool 2020" values
      (318 g, 238 g), and these are based on a **factor 2.0 on the CO₂ of the kerosene burned** for non-CO₂ effects
      (contrails, NOx, see 09) – [treeze 2021, Life Cycle Inventories of Air Transport Services](https://treeze.ch/fileadmin/user_upload/downloads/Publications/Case_Studies/Mobility/735_aircraftTransport_2021_v1.2.pdf),
      section 7.4.1, Tab. 7.5. So roughly half of the 259 g direct is CO₂ – **do not multiply mobitool by an RFI
      again**. Calculators that start from CO₂ use their own factor: myclimate multiplies the CO₂ by 3 (RFI, since
      2023, [press release](https://www.myclimate.org/fileadmin/user_upload/myclimate_-_home/01_Information/02_News_Press/03_Press_releases/Documents/231127_PressRelease_New_Flight_Calculator.pdf)),
      atmosfair multiplies the CO₂ emitted above 9 km by 3
      ([method](https://www.atmosfair.de/wp-content/uploads/flight-emissionscalculator-documentation-calculationmethodology.pdf), section 4.3).
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 11.4 – The time penalty: how much CO₂ does one lost hour save?

    Door to door, the plane is not as fast as it looks. Estimate per destination in `co2_trip`:

    - `plane_minutes` = block time + **120 min** airport overhead (to the airport, security, boarding, baggage).
      Block time ≈ **45 min + distance at 800 km/h** (taxi, climb, approach – gives ZRH–Paris CDG 81 min,
      ZRH–London Heathrow 104 min). Both numbers are **our assumptions** – compare them with a timetable.
    - `extra_hours` = (`rail_minutes` − `plane_minutes`) / 60 – negative when the train is faster.
    - `co2_saved_kg` = `plane_kg` − `train_kg`.
    - `kg_per_hour` = `co2_saved_kg / extra_hours` where `extra_hours > 0`, else NaN (the train is faster: no
      trade-off at all).

    Store it in `penalty` (one row per city: drop duplicate cities, e.g. LIML and LIMC), sorted by `extra_hours`.
    """)
    return


@app.cell
def _(co2_trip):
    # TODO 11.4: penalty = co2_trip + plane_minutes, extra_hours, co2_saved_kg, kg_per_hour (one row per city)
    penalty = co2_trip.assign(plane_minutes=0.0, extra_hours=0.0, co2_saved_kg=0.0, kg_per_hour=0.0)
    return (penalty,)


@app.cell(hide_code=True)
def _(check, mo, penalty, px):
    _out = []
    try:
        _cols = {"city", "plane_minutes", "extra_hours", "co2_saved_kg", "kg_per_hour"}
        _r = penalty.iloc[0]
        _ok = (_cols <= set(penalty.columns) and penalty["city"].is_unique and penalty["extra_hours"].is_monotonic_increasing
               and abs(_r["plane_minutes"] - (165 + _r["distance_km"] * 0.075)) < 2
               and penalty.loc[penalty.extra_hours <= 0, "kg_per_hour"].isna().all()
               and penalty["co2_saved_kg"].gt(0).all())
    except Exception:
        _ok = False
    if _ok:
        _faster = penalty[penalty.extra_hours <= 0]
        _close = penalty[penalty.extra_hours.between(0, 1, inclusive="right")]
        _top = penalty.sort_values("flights", ascending=False).iloc[0]
        _out.append(check(True,
                          f"Train faster door to door: {', '.join(_faster.city) or 'none'}; at most 1 h slower: "
                          f"{', '.join(_close.city) or 'none'}. Most flown, {_top.city}: {_top.extra_hours:.1f} h "
                          f"longer, {_top.co2_saved_kg:.0f} kg saved – "
                          + (f"**{_top.kg_per_hour:.0f} kg CO₂ per hour** by train."
                             if _top.extra_hours > 0 else "and no time lost."), ""))
        _out.append(px.scatter(penalty, x="extra_hours", y="co2_saved_kg", text="city", size="flights",
                               color="transfers", height=420,
                               labels={"extra_hours": "extra time by train (h, door to door)",
                                       "co2_saved_kg": "CO₂-eq saved per person (kg)", "transfers": "transfers"})
                    .update_traces(textposition="top center")
                    .add_vline(x=0, line_dash="dot", annotation_text="same time"))
    else:
        _out.append(check(False, "", "Expected one row per city, sorted by extra_hours, with plane_minutes = 45 + "
                                     "distance/800×60 + 120, co2_saved_kg = plane_kg − train_kg and kg_per_hour NaN "
                                     "where the train is not slower."))
    mo.vstack(_out)
    return


@app.cell
def _(mo, penalty):
    _cities = list(penalty["city"]) if "city" in penalty else []
    destination = mo.ui.dropdown(options=_cities, value=_cities[0] if _cities else None, label="Destination")
    destination
    return (destination,)


@app.cell
def _(FACTOR, co2_trip, con, destination, mo, pd, penalty, per_type, px):
    mo.stop(not destination.value, mo.md("*pick a destination*"))
    _p = penalty.drop_duplicates("city").set_index("city").loc[destination.value]
    _km = float(_p["distance_km"])
    _modes = pd.DataFrame({
        "mode": ["plane (mobitool)", "car, fleet average (1.6 people)", "car, electric (1.6 people)", "train"],
        "kg": [_p["plane_kg"], _p["car_kg"], round(_km * 1.2 * FACTOR["car_bev_fleet_average"] / 1000, 1),
               _p["train_kg"]],
    })
    _icaos = list(co2_trip.loc[co2_trip.city == destination.value, "airport_icao"])
    _types = con.execute(f"""
        WITH seen AS (SELECT DISTINCT icao24, trim(callsign) AS callsign FROM silver.flights
                      WHERE region = 'switzerland' AND callsign IS NOT NULL)
        SELECT r.callsign, r.origin_icao, r.destination_icao, t.typecode
        FROM seen s JOIN silver.flight_routes r USING (callsign) LEFT JOIN silver.aircraft_types t USING (icao24)
        WHERE (r.origin_icao = 'LSZH' AND r.destination_icao IN ({",".join("?" * len(_icaos))}))
           OR (r.destination_icao = 'LSZH' AND r.origin_icao IN ({",".join("?" * len(_icaos))}))""",
                         _icaos + _icaos).df().merge(per_type[["typecode", "seats", "g_co2_per_pkm"]],
                                                    on="typecode", how="left")
    mo.vstack([
        mo.md(f"### Zurich ↔ {destination.value} ({_km:.0f} km)\n\n"
              f"Train **{_p['rail_minutes'] // 60:.0f} h {_p['rail_minutes'] % 60:02.0f} min** "
              f"({_p['transfers']:.0f} transfers) vs. plane **{_p['plane_minutes'] // 60:.0f} h "
              f"{_p['plane_minutes'] % 60:02.0f} min** door to door."),
        px.bar(_modes, x="kg", y="mode", orientation="h", height=240,
               labels={"kg": "kg CO₂-eq per person, one way", "mode": ""}),
        mo.md("Flights observed on this route:"), _types,
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Think about it

    - **Night trains.** Zurich–Amsterdam, –Hamburg, –Berlin, –Vienna and –Prague run as direct Nightjet/EuroNight
      trains (ÖBB timetable for Thu 2026-10-22; also Graz, Budapest, Zagreb). If you sleep on the train, is the
      "extra time" still lost time? Which cities move to the left of the chart?
    - **Load factors.** mobitool assumes 75.7 % of the seats occupied on a European flight, but only 29.3 % on Swiss
      trains (column `load_factor` in `factors`). Move the slider in 11.3: how much does the per-person result depend
      on this assumption – and does an empty seat on a train that runs anyway cost CO₂?
    - **RFI.** Should non-CO₂ effects count? Never add an RFI on top of a factor that already contains one
      (mobitool, see above). With myclimate's factor 3 on a CO₂-only value, how big is the gap to the train?
    - The train factor uses the destination country for the whole trip. The direct Zurich–Milan EuroCity runs about
      205 of 256 km (80 %) in Switzerland, up to Chiasso (our computation from the ÖBB timetable route geometry) –
      how much does a distance-weighted mix change `train_kg`?
    - Which share of all Zurich flights in your data could be replaced by a train trip under 6 hours?

    **Next:** [12 – Who is overflown?](12_overflown.py)
    """)
    return


if __name__ == "__main__":
    app.run()
