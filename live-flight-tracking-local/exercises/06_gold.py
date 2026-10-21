import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium", app_title="06 – Gold")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 06 – Gold: reference data and the views the map reads

    Silver knows *where* an aircraft is. To say *who* it is and *what* it does we join **reference data**:

    - **airports** – [OurAirports](https://ourairports.com/data/) (public domain, see "Terms of use" there): name,
      codes, coordinates, elevation
    - **airlines** – [OpenFlights](https://openflights.org/data) (Open Database License): ICAO code -> airline name

    **Gold** answers concrete questions. Here as SQL **views**: they are always up to date with silver, nothing to
    refresh – DuckDB computes them when you ask.
    """)
    return


@app.cell
def _(mo):
    import sys

    import duckdb
    import pandas as pd

    sys.path.insert(0, str(mo.notebook_dir().parent))
    import flightlab as fl

    def check(ok, good, bad):
        return mo.callout(mo.md(("✅ " + good) if ok else ("❌ " + bad)), kind="success" if ok else "danger")

    con = fl.practice(6)                            # practice_06.duckdb – continues from the step before
    fl.init_db(con)
    fl.ensure_sample_data(con)               # skipped 02-04? then 2 hours from R2 (or one live call)
    fl.build_silver(con)                     # silver.flights from your bronze (the finished version of 05)
    _n = con.execute("SELECT COUNT(*), COUNT(DISTINCT icao24) FROM silver.flights").fetchone()
    mo.md(f"silver.flights: **{_n[0]:,} positions** of {_n[1]:,} aircraft")
    return check, con, fl, pd


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 6.1 – Airports

    Read `airports.csv` (the path is in `airports_csv`, downloaded once into `cache/`) with pandas, keep only
    `large_airport` and `medium_airport` and store the table **`silver.airports`** with these columns:

    `airport_icao` (from `ident`), `airport_iata` (`iata_code`), `airport_name` (`name`), `airport_type` (`type`),
    `city` (`municipality`), `country` (`iso_country`), `latitude` (`latitude_deg`), `longitude` (`longitude_deg`),
    `elevation_ft`.

    /// details | Hint
    `pd.read_csv(path, keep_default_na=False, na_values=[""])` – without `keep_default_na=False` pandas turns the
    country code **NA** (Namibia) into a missing value! Then `con.execute("CREATE OR REPLACE TABLE silver.airports AS
    SELECT * FROM airports")` – DuckDB finds the DataFrame `airports` by its name.
    ///
    """)
    return


@app.cell
def _(con, fl, pd):
    airports_csv = fl.cached(fl.AIRPORTS_URL, "airports.csv")

    # TODO 6.1: silver.airports (large + medium airports, columns as listed above)
    airports = pd.DataFrame()
    return (airports,)


@app.cell(hide_code=True)
def _(airports, check, con):
    try:
        _zrh = con.execute("SELECT airport_iata, airport_name FROM silver.airports WHERE airport_icao = 'LSZH'").fetchone()
        _na = con.execute("SELECT COUNT(*) FROM silver.airports WHERE country = 'NA'").fetchone()[0]
    except Exception:
        _zrh, _na = None, 0
    check(_zrh is not None and _zrh[0] == "ZRH" and _na > 0 and len(airports) > 3000,
          f"{len(airports):,} airports, LSZH = {_zrh and _zrh[1]}, Namibia's airports kept ({_na})",
          "silver.airports should have > 3,000 large/medium airports incl. LSZH (ZRH) – and Namibia (country 'NA').")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 6.2 – Airlines

    `airlines.dat` has **no header**; the columns are `id, name, alias, iata, icao, callsign, country, active`,
    missing values are written as `\N`. Keep rows whose `icao` is three capital letters, prefer active airlines
    (`active == "Y"`) when a code appears twice, and store **`silver.airlines`** with `airline_icao`, `airline_iata`,
    `airline_name`, `callsign`, `country`.

    Careful: [OpenFlights](https://openflights.org/data) itself warns that `active` is not reliable. `SWR` appears twice,
    both marked `Y`: *Swiss International Air Lines* and *Swissair*. Sort with `kind="stable"`, so the
    file order decides between equal rows and the result does not change from run to run.
    """)
    return


@app.cell
def _(con, fl, pd):
    airlines_dat = fl.cached(fl.AIRLINES_URL, "airlines.dat")

    # TODO 6.2: silver.airlines (one row per ICAO code, active ones first)
    airlines = pd.DataFrame()
    return (airlines,)


@app.cell(hide_code=True)
def _(airlines, check, con):
    try:
        _swr = con.execute("SELECT airline_name FROM silver.airlines WHERE airline_icao = 'SWR'").fetchone()
        _dup = con.execute("SELECT COUNT(*) - COUNT(DISTINCT airline_icao) FROM silver.airlines").fetchone()[0]
    except Exception:
        _swr, _dup = None, -1
    check(len(airlines) > 1000 and _swr is not None and "Swiss International" in _swr[0] and _dup == 0,
          f"{len(airlines):,} airlines, SWR = {_swr and _swr[0]}",
          "silver.airlines should contain SWR = Swiss International Air Lines (not Swissair – stable sort?), "
          "one row per airline_icao.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 6.3 – Where is every aircraft right now?

    Create the view **`gold.current_positions`**: the **latest** row of `silver.flights` per aircraft, but only
    aircraft seen in the last **2 minutes** before the newest snapshot (otherwise aircraft that landed an hour ago
    would stay on the map forever). Add the airline name (`'unknown / private'` when there is none).

    /// details | Hint
    ```sql
    CREATE OR REPLACE VIEW gold.current_positions AS
    WITH latest AS (SELECT MAX(snapshot_time) AS max_time FROM silver.flights)
    SELECT f.*, COALESCE(a.airline_name, 'unknown / private') AS airline_name
    FROM silver.flights f CROSS JOIN latest l LEFT JOIN silver.airlines a USING (airline_icao)
    WHERE f.snapshot_time >= l.max_time - INTERVAL 2 MINUTE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY f.icao24 ORDER BY f.snapshot_time DESC) = 1
    ```
    `QUALIFY` filters on a window function – like `HAVING` for `GROUP BY`.
    ///
    """)
    return


@app.cell
def _(con):
    # TODO 6.3: view gold.current_positions (latest row per aircraft of the last 2 minutes + airline name)
    con.execute("CREATE OR REPLACE VIEW gold.current_positions AS SELECT * FROM silver.flights")   # improve me
    return


@app.cell(hide_code=True)
def _(check, con):
    try:
        _n, _ids, _oldest = con.execute(
            "SELECT COUNT(*), COUNT(DISTINCT icao24), MIN(snapshot_time) FROM gold.current_positions").fetchone()
        _newest = con.execute("SELECT MAX(snapshot_time) FROM silver.flights").fetchone()[0]
        _ok = _n == _ids and _n > 0 and (_newest - _oldest).total_seconds() <= 120
    except Exception as _e:
        _n, _ok = 0, False
    check(_ok, f"{_n} aircraft right now, one row each, none older than 2 minutes",
          "gold.current_positions should have exactly one row per aircraft, all from the last 2 minutes.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 6.4 – Who is landing or taking off?

    View **`gold.airport_approaches`**: aircraft of `gold.current_positions` that are in the air, **below 3,000 m**
    and **within 40 km** of an airport – with the nearest airport, the distance and `movement`
    (`arriving` if descending, `departing` if climbing, else `nearby`).

    The distance on a sphere is the **haversine** formula (given below). The `BETWEEN` pre-filter keeps the join
    small: without it, DuckDB would compute the distance from every aircraft to every one of the more than 5,000
    airports in `silver.airports`.
    """)
    return


@app.cell
def _(con):
    HAVERSINE_KM = """2 * 6371 * ASIN(SQRT(POWER(SIN(RADIANS(ap.latitude - p.latitude) / 2), 2)
        + COS(RADIANS(p.latitude)) * COS(RADIANS(ap.latitude)) * POWER(SIN(RADIANS(ap.longitude - p.longitude) / 2), 2)))"""

    # TODO 6.4: view gold.airport_approaches (below 3000 m, within 40 km, nearest airport, movement)
    con.execute(f"CREATE OR REPLACE VIEW gold.airport_approaches AS SELECT 1 AS todo")   # replace me
    return


@app.cell(hide_code=True)
def _(check, con, mo):
    try:
        _df = con.sql("SELECT airport_iata, airport_name, callsign, movement, altitude_ft, ROUND(distance_km, 1) AS km "
                      "FROM gold.airport_approaches ORDER BY airport_iata, km").df()
        _ok = len(_df) == 0 or (_df["km"].max() <= 40 and set(_df["movement"]) <= {"arriving", "departing", "nearby"})
    except Exception:
        _df, _ok = None, False
    mo.vstack([check(_ok and _df is not None, f"{0 if _df is None else len(_df)} aircraft near airports right now",
                     "gold.airport_approaches should list aircraft within 40 km with movement arriving/departing/"
                     "nearby."), _df if _df is not None else ""])
    return


@app.cell
def _(con, mo):
    import plotly.express as px

    _traffic = con.sql("""
        SELECT date_trunc('minute', snapshot_time) AS minute, flight_phase, COUNT(DISTINCT icao24) AS aircraft
        FROM silver.flights GROUP BY ALL ORDER BY minute""").df()
    mo.vstack([mo.md("## Aircraft per minute and flight phase"),
               px.area(_traffic, x="minute", y="aircraft", color="flight_phase", height=320)])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Think about it

    - Why a **view** for current_positions and not a table? When would a table be better?
    - Which airport has the most traffic in your data? Write the query in a new cell.
    - Some aircraft near Zurich show `nearby` although they are landing. Why? (Look at `vertical_rate` in silver.)

    That is the whole medallion pipeline: API -> bronze -> silver -> gold. `flightlab.build_gold(con)` holds the
    finished views that the app uses.

    **Next:** [07 – Routes and detours](07_routes_detours.py)
    """)
    return


if __name__ == "__main__":
    app.run()
