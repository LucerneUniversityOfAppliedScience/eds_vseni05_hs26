import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium", app_title="20 – Your dashboard")


@app.cell(hide_code=True)
def _():
    import marimo as mo

    mo.md(r"""
    # 20 – Your own dashboard: `marimo run`

    Every notebook so far was a workbench: code, output, code. A notebook can also be an **app**:

    ```bash
    uv run marimo run solutions/20_dashboard.py        # code hidden, only outputs and controls, read-only
    ```

    Same file, same reactivity – the dropdowns still recompute everything that depends on them, but nobody sees (or
    breaks) the code. That is how you hand an analysis to someone who does not want a notebook.

    Build a small airport dashboard in four steps: **KPIs**, a **control**, a **chart**, a **layout with tabs** –
    and it refreshes itself every minute while the collector or the app keeps writing.

    In `marimo run` this text, the hints and the checks disappear: `mo.app_meta().mode` is `"run"` there, and
    the cells below only return their output when `IN_APP` is False.
    """) if mo.app_meta().mode != "run" else None
    return (mo,)


@app.cell
def _(mo):
    import sys

    sys.path.insert(0, str(mo.notebook_dir().parent))
    import flightlab as fl
    import plotly.express as px

    IN_APP = mo.app_meta().mode == "run"                    # marimo run: only the dashboard, no exercise text

    def check(ok, good, bad):
        if IN_APP:
            return None
        return mo.callout(mo.md(("✅ " + good) if ok else ("❌ " + bad)), kind="success" if ok else "danger")

    # airport reference points from OurAirports (https://ourairports.com/data/, airports.csv)
    AIRPORTS = {"LSZH – Zurich": (47.4581, 8.5481), "LSGG – Geneva": (46.2381, 6.1090),
                "LFSB – Basel-Mulhouse": (47.6007, 7.5211), "LSZB – Bern": (46.9127, 7.4988)}
    refresh = mo.ui.refresh(options=["1m", "5m"], default_interval="1m", label="Refresh")
    return AIRPORTS, IN_APP, check, fl, px, refresh


@app.cell
def _(fl, refresh):
    refresh                                                  # a new copy of flights.duckdb every minute
    con = fl.snapshot()
    return (con,)


@app.cell(hide_code=True)
def _(IN_APP, mo):
    mo.md(r"""
    ## TODO 20.1 – Three KPIs

    `mo.stat(value, label=..., caption=...)` shows one number as a tile. Build `kpis`, a `mo.hstack` of three
    stats from `gold.current_positions`: aircraft **in the air**, aircraft **on the ground**, and the **time of the
    newest position** (UTC, `HH:MM`).

    /// details | Hint
    ```python
    _row = con.execute("SELECT COUNT(*) FILTER (WHERE NOT on_ground), ... FROM gold.current_positions").fetchone()
    kpis = mo.hstack([mo.stat(f"{_row[0]:,}", label="in the air"), ...], justify="start", gap=2)
    ```
    ///
    """) if not IN_APP else None
    return


@app.cell
def _(IN_APP, con, mo):
    # TODO 20.1: kpis = mo.hstack of three mo.stat tiles (in the air, on the ground, newest position)
    kpis = mo.md("TODO")
    kpis if not IN_APP else None                            # in the app it appears once, inside the page
    return (kpis,)


@app.cell(hide_code=True)
def _(check, kpis):
    _html = kpis.text if hasattr(kpis, "text") else str(kpis)
    check("in the air" in _html and "on the ground" in _html and _html.count("<marimo-stat") + _html.count("stat") >= 3,
          "Three tiles – in `marimo run` they are the top of the page.",
          "Expected an mo.hstack with three mo.stat tiles labelled 'in the air', 'on the ground', 'newest position'.")
    return


@app.cell(hide_code=True)
def _(IN_APP, mo):
    mo.md(r"""
    ## TODO 20.2 – A control: which airport?

    Create `airport`, an `mo.ui.dropdown` over the keys of `AIRPORTS`, default `"LSZH – Zurich"`, label `Airport`.
    Every cell that reads `airport.value` reruns when someone picks another airport – no callback code.
    """) if not IN_APP else None
    return


@app.cell
def _(AIRPORTS, mo):
    # TODO 20.2: airport = dropdown over AIRPORTS, default LSZH
    airport = None
    return (airport,)


@app.cell(hide_code=True)
def _(AIRPORTS, airport, check):
    _ok = airport is not None and getattr(airport, "value", None) in AIRPORTS
    check(_ok, f"Dropdown ready – selected: {airport.value}" if _ok else "",
          "Expected airport = mo.ui.dropdown(options=list(AIRPORTS), value='LSZH – Zurich', label='Airport').")
    return


@app.cell(hide_code=True)
def _(IN_APP, mo):
    mo.md(r"""
    ## TODO 20.3 – Movements per 15 minutes

    For the selected airport, count departures (`flight_phase = 'climbing'`) and arrivals (`'descending'`): different
    aircraft within **15 km** and **below 6,000 ft**, per 15 minutes in **Swiss time**, over the **last 6 hours of
    data**. Store the result in `movements` (columns `t_local`, `departures`, `arrivals`) and the figure in `chart`
    (a `px.line` with both series).

    /// details | Hint
    Same distance formula as in 10.5; the airport position: `_lat, _lon = AIRPORTS[airport.value]`.
    `px.line(movements, x="t_local", y=["departures", "arrivals"])` draws both.
    ///
    """) if not IN_APP else None
    return


@app.cell
def _(AIRPORTS, airport, con, px):
    _lat, _lon = AIRPORTS[airport.value] if airport is not None else AIRPORTS["LSZH – Zurich"]
    # TODO 20.3: movements per 15 min (t_local, departures, arrivals) for the airport, last 6 h; chart = px.line
    movements = con.execute("SELECT NULL::TIMESTAMP AS t_local, 0 AS departures, 0 AS arrivals").df()
    chart = None
    return chart, movements


@app.cell(hide_code=True)
def _(chart, check, movements):
    _ok = {"t_local", "departures", "arrivals"} <= set(movements.columns) and len(movements) > 4 and chart is not None
    check(_ok and movements[["departures", "arrivals"]].to_numpy().sum() > 0,
          f"{len(movements)} quarter hours, {int(movements.departures.sum())} departures and "
          f"{int(movements.arrivals.sum())} arrivals counted" if _ok else "",
          "Expected movements with t_local, departures, arrivals (last 6 h, several rows) and a plotly chart. "
          "No data in the last 6 hours? Start the collector or the app, or backfill (notebook 04).")
    return


@app.cell(hide_code=True)
def _(IN_APP, mo):
    mo.md(r"""
    ## TODO 20.4 – The page: header, tabs, layout

    Put it together in `page`: a `mo.vstack` with the title line (`mo.hstack([mo.md("## ✈ Airport dashboard"),
    airport, refresh], justify="space-between")`), the `kpis`, and `mo.ui.tabs` with two tabs:
    **"Movements"** → `chart`, **"Table"** → `mo.ui.table(movements)`.

    Then open a terminal in the project folder and run it as an app:

    ```bash
    uv run marimo run solutions/20_dashboard.py
    ```

    **Bonus:** in the editor, open the app preview (button at the bottom right, or the command palette), pick
    **Grid** in the layout dropdown and drag the outputs where you want them – marimo stores the arrangement in
    `layouts/20_dashboard.grid.json` next to the notebook and `marimo run` uses it
    ([marimo docs: apps](https://docs.marimo.io/guides/apps/)).
    """) if not IN_APP else None
    return


@app.cell
def _(airport, chart, kpis, mo, movements, refresh):
    # TODO 20.4: page = header (title, airport, refresh) + kpis + tabs (Movements: chart, Table: movements)
    page = mo.md("TODO")
    page
    return (page,)


@app.cell(hide_code=True)
def _(check, page):
    _html = page.text if hasattr(page, "text") else str(page)
    check("Movements" in _html and "Table" in _html and "Airport dashboard" in _html,
          "The page is ready – now `uv run marimo run solutions/20_dashboard.py` (or exercises/...) shows only this.",
          "Expected page = mo.vstack([header, kpis, mo.ui.tabs({'Movements': chart, 'Table': ...})]).")
    return


@app.cell(hide_code=True)
def _(IN_APP, mo):
    mo.md(r"""
    ## Think about it

    - `marimo run` shows **every** output – also the check cells. How would you hide them in the app? (Hint: an
      output is only shown if it is the last expression of a cell; or put them in a separate notebook.)
    - Who is the audience? A dispatcher at the airport needs other KPIs than a climate journalist – sketch both.
    - Ideas to add: the squawk alarm (`gold.squawk_alerts`), the disruption flag from notebook 17, CO₂ of the last
      hour (`gold.fuel_burn`), a map with `px.scatter_map` of `gold.current_positions`.
    - Compare with the Dash app in `app/`: about 1,500 lines in `app/app.py` with 17 callbacks vs. one reactive
      notebook. When is which right?

    **That's it** – you went from one API message (01) to your own dashboard.
    """) if not IN_APP else None
    return


if __name__ == "__main__":
    app.run()
