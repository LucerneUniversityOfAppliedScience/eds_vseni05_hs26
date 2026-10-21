import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium", app_title="12 – Who is overflown?")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 12 – Who is overflown?

    Aircraft noise is a local problem: it matters most where aircraft are **low**, and where **people live**. In this
    notebook you put the positions of `silver.flights` on the map of the Swiss municipalities and ask:

    - Which municipalities see the most low aircraft – in total, per km², per resident?
    - How many people live **under the approach and departure paths** of Zurich?
    - How does that compare with the **official noise contours** of the airport?

    Open data used here (downloaded once into `cache/`):

    | Data | Source | Licence |
    |---|---|---|
    | Municipality boundaries 1.1.2025 (generalised, WGS84) | BFS, *Generalisierte Gemeindegrenzen* gg25, K4 version (data description PDF in the zip) | BFS: open use, must provide the source |
    | Permanent resident population per municipality, 31.12.2025 | BFS STATPOP, PXWeb table px-x-0102010000_101 (municipalities as of 6.4.2025) – stored in `data/statpop_2025_municipalities.csv` (fetched 20.10.2026; the API needs ~30 s per call) | BFS: open use, must provide the source |
    | Population per hectare 2024 | BFS STATPOP via data.geo.admin.ch (100 m cells) | [opendata.swiss "terms by ask"](https://opendata.swiss/en/terms-of-use/#terms_by_ask): open use with source, commercial use needs permission – fine for teaching |
    | Noise contours Zurich airport | BAZL noise exposure cadastre (*Lärmbelastungskataster*) LSZH, valid from 3.7.2015, via the WFS of the Canton of Zurich | cite BAZL / Kanton Zürich (the WFS states no fees, no access constraints) |
    """)
    return


@app.cell
def _(mo):
    import json
    import sys
    import zipfile

    import numpy as np
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    import shapely

    sys.path.insert(0, str(mo.notebook_dir().parent))
    import flightlab as fl

    def check(ok, good, bad):
        return mo.callout(mo.md(("✅ " + good) if ok else ("❌ " + bad)), kind="success" if ok else "danger")

    GG25_URL = "https://dam-api.bfs.admin.ch/hub/api/dam/assets/34367751/master"
    GG25_MEMBER = "ag-b-00.03-875-gg25/Historized boundaries K4 20250101/Communes_K4_20250101_4326.geojson"
    NOISE_URL = ("https://maps.zh.ch/wfs/TBAFALSFluglaermZHWFS?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature"
                 "&TYPENAMES=ms:laermkurven&OUTPUTFORMAT=geojson&SRSNAME=EPSG:4326")
    GRID_URL = ("https://data.geo.admin.ch/ch.bfs.volkszaehlung-bevoelkerungsstatistik_einwohner/"
                "volkszaehlung-bevoelkerungsstatistik_einwohner_2024/"
                "volkszaehlung-bevoelkerungsstatistik_einwohner_2024_2056.csv")

    with mo.status.spinner(title="downloading boundaries, noise contours and the hectare grid (~22 MB, once) ..."):
        _geojson = fl.CACHE / "gg25_communes_4326.geojson"
        if not _geojson.exists():
            _geojson.write_bytes(zipfile.ZipFile(fl.cached(GG25_URL, "gg25.zip", 365)).read(GG25_MEMBER))
        gemeinden = json.loads(_geojson.read_text())
        noise = json.loads(fl.cached(NOISE_URL, "lszh_noise_contours.geojson", 90).read_text())
        grid = pd.read_csv(fl.cached(GRID_URL, "statpop_ha_2024.csv", 365), sep=";")

    # one row per polygon (properties) + the shapely geometries in the same order
    gg = pd.DataFrame([f["properties"] for f in gemeinden["features"]])
    geoms = shapely.from_geojson([json.dumps(f["geometry"]) for f in gemeinden["features"]])
    statpop = pd.read_csv(mo.notebook_dir().parent / "data" / "statpop_2025_municipalities.csv")

    # Mergers on 6.4.2025 (BFS municipality register, mutations 3995–3997): Bodio -> Giornico, Prato (Leventina) -> Quinto,
    # Astano, Bedigliora, Curio, Miglieglia, Novaggio -> Lema (new, 5395). gg25 (1.1.2025) still has the old ones.
    MERGERS_2025 = {5064: 5073, 5078: 5079, **{old: 5395 for old in (5146, 5149, 5181, 5200, 5207)}}

    con = fl.snapshot()
    _span = con.execute("""SELECT MIN(snapshot_time), MAX(snapshot_time), COUNT(DISTINCT icao24)
                           FROM silver.flights WHERE region = 'switzerland'""").fetchone()
    mo.md(f"**{len(gg):,} municipality polygons**, **{len(statpop):,} municipalities** with population, "
          f"**{len(grid):,} hectare cells** – flights: **{_span[2]:,} aircraft** in the Swiss feed between "
          f"{_span[0]:%d.%m. %H:%M} and {_span[1]:%d.%m. %H:%M} UTC")
    return MERGERS_2025, check, con, geoms, gg, go, grid, noise, np, pd, px, shapely, statpop


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 12.1 – Municipalities with population

    Two tables, one key: `gg` (boundaries, one row per polygon, column `GDENR`) and `statpop` (`gdenr`, `name`,
    `population`). Build `municipalities` with **one row per municipality** and the columns
    `gdenr, name, canton, area_km2, z_avg_m, population`.

    The catch: the boundaries are from 1 January 2025, the population from 31 December 2025. On 6 April 2025 seven
    Ticino municipalities merged: five into the new **Lema (5395)**, Bodio into Giornico (5073), Prato (Leventina)
    into Quinto (5079) ([BFS mutations](https://www.agvchapp.bfs.admin.ch/api/communes/mutations?startPeriod=02-01-2025&endPeriod=31-12-2025)) –
    `gg` has 2,121 polygons, `statpop` 2,115 municipalities. `MERGERS_2025` (from the setup cell) maps the old
    numbers to the new ones: map first, then **group** (area: sum, `Z_AVG` = mean elevation in metres: plain mean,
    `KTKZ` = canton: first).

    /// details | Hint
    ```python
    _geo = (gg.assign(gdenr=gg["GDENR"].replace(MERGERS_2025))
              .groupby("gdenr", as_index=False)
              .agg(canton=("KTKZ", "first"), area_ha=("AREA_HA", "sum"), z_avg_m=("Z_AVG", "mean")))
    ```
    Then merge with `statpop` on `gdenr` and turn hectares into km² (÷ 100).
    ///
    """)
    return


@app.cell
def _(MERGERS_2025, gg, pd, statpop):
    # TODO 12.1: municipalities = gdenr, name, canton, area_km2, z_avg_m, population (one row per municipality)
    municipalities = pd.DataFrame(columns=["gdenr", "name", "canton", "area_km2", "z_avg_m", "population"])
    return (municipalities,)


@app.cell(hide_code=True)
def _(check, municipalities):
    try:
        _ok = (len(municipalities) == 2115 and municipalities["gdenr"].is_unique
               and int(municipalities["population"].sum()) == 9_127_125
               and float(municipalities["area_km2"].min()) > 0 and municipalities["z_avg_m"].notna().all())
        _zh = municipalities.set_index("gdenr").loc[261] if _ok else None
        _out = check(_ok,
                     f"{len(municipalities):,} municipalities, {municipalities['population'].sum():,} residents, "
                     f"{municipalities['area_km2'].sum():,.0f} km² (Switzerland: 41,291 km² in the same BFS dataset – "
                     f"lakes larger than 5 km² are not counted in any municipality's area there). Zürich: {_zh['population']:,} residents on {_zh['area_km2']:.0f} km²"
                     if _ok else "",
                     "Expected 2,115 unique municipalities with 9,127,125 residents, area_km2 > 0 and z_avg_m – "
                     "did you map the old Ticino numbers with MERGERS_2025 before grouping?")
    except Exception as _e:
        _out = check(False, "", f"Could not check `municipalities`: {type(_e).__name__}: {_e}")
    _out
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Low positions

    All airborne positions of the Swiss feed **below 12,000 ft** (barometric altitude, above sea level). Why so high?
    "Low" means low **above ground** – an aircraft at 9,000 ft is about 7,450 ft above Kloten (mean elevation
    473 m) but only about 1,900 ft above the mean elevation of Davos (2,159 m; `Z_AVG` in gg25). We do not have a
    terrain model, so we use the **mean elevation of the municipality** (`Z_AVG`) below the aircraft:
    `agl_ft = altitude_ft − z_avg_m × 3.28084`. In the Alps that is rough (Davos spans 1,051 to 3,145 m), on the
    Plateau much better (Kloten: 416 to 601 m) – compare `Z_MIN` and `Z_MAX` in `gg`.
    """)
    return


@app.cell
def _(con, mo):
    low = con.execute("""
        SELECT icao24, trim(callsign) AS callsign, snapshot_time, latitude, longitude, altitude_ft
        FROM silver.flights
        WHERE region = 'switzerland' AND NOT on_ground AND altitude_ft < 12000""").df()
    mo.md(f"`low`: **{len(low):,} positions** of **{low['icao24'].nunique():,} aircraft** below 12,000 ft")
    return (low,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 12.2 – Which municipality is below each position?

    Point in polygon for tens of thousands of points against 2,121 polygons: a spatial index makes it fast.
    `shapely.STRtree(geoms)` builds the index; `tree.query(points, predicate="within")` returns **two arrays**: the
    index of the point and the index of the polygon it lies in. Points that lie in no polygon do not appear at all –
    abroad (the Swiss feed is a rectangle, 45.6–48.0° N, 5.8–10.7° E, that also covers parts of Germany, France,
    Italy and Austria) and **on the lakes** (in gg25, lakes larger than 5 km² are not part of any municipality
    polygon – a point in the middle of Lake Zurich matches nothing).

    Build `low_muni`: the rows of `low` that matched, with `gdenr` (mapped with `MERGERS_2025`!) and `agl_ft`
    (altitude above the municipality's mean elevation, see above).

    /// details | Hint
    ```python
    _tree = shapely.STRtree(geoms)
    _i, _j = _tree.query(shapely.points(low["longitude"], low["latitude"]), predicate="within")
    _gdenr = gg["GDENR"].replace(MERGERS_2025).to_numpy()[_j]
    low_muni = low.iloc[_i].assign(gdenr=_gdenr).merge(municipalities[["gdenr", "z_avg_m"]], on="gdenr")
    ```
    Use names with `_` for helpers – a global name may only be defined in one cell.
    ///
    """)
    return


@app.cell
def _(MERGERS_2025, geoms, gg, low, municipalities, shapely):
    # TODO 12.2: low_muni = matched positions of low with gdenr and agl_ft
    low_muni = low.assign(gdenr=0, agl_ft=low["altitude_ft"])
    return (low_muni,)


@app.cell(hide_code=True)
def _(check, low, low_muni, municipalities):
    try:
        _share = len(low_muni) / max(len(low), 1)
        _ok = ({"gdenr", "agl_ft"} <= set(low_muni.columns) and 0.1 < _share < 1
               and low_muni["gdenr"].isin(municipalities["gdenr"]).all()
               and (low_muni["agl_ft"] < low_muni["altitude_ft"]).mean() > 0.95)
        _out = check(_ok,
                     f"{len(low_muni):,} of {len(low):,} positions ({_share:.0%}) lie over a Swiss municipality, "
                     f"{(low_muni['agl_ft'] < 3000).mean():.0%} of them less than 3,000 ft above ground. "
                     f"The rest: abroad or over a lake." if _ok else "",
                     f"Expected only the matched positions (got {_share:.0%} of `low`), each with a gdenr that "
                     f"exists in `municipalities` and agl_ft = altitude_ft − z_avg_m × 3.28084.")
    except Exception as _e:
        _out = check(False, "", f"Could not check `low_muni`: {type(_e).__name__}: {_e}")
    _out
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TODO 12.3 – The ranking

    Count per municipality the **different aircraft** (`icao24`, not rows – the collector stores a position of
    each aircraft every 35 s) that flew over it **less than 3,000 ft above ground**. Add `name`, `canton`, `population`, `area_km2` and
    two normalised numbers:

    - `aircraft_per_km2` = aircraft ÷ area – a big municipality catches more tracks just by being big
    - `aircraft_per_1000` = aircraft ÷ population × 1000

    Store the result in `ranking`, most aircraft first (only municipalities with at least one aircraft).
    """)
    return


@app.cell
def _(low_muni, municipalities, pd):
    # TODO 12.3: ranking = aircraft < 3000 ft AGL per municipality, with aircraft_per_km2 and aircraft_per_1000
    ranking = pd.DataFrame({"gdenr": [0], "name": ["TODO"], "aircraft": [0], "aircraft_per_km2": [0.0], "aircraft_per_1000": [0.0]})
    return (ranking,)


@app.cell(hide_code=True)
def _(MERGERS_2025, check, gemeinden_map, mo, px, ranking):
    try:
        _ok = ({"gdenr", "name", "aircraft", "aircraft_per_km2", "aircraft_per_1000", "area_km2"} <= set(ranking.columns)
               and len(ranking) > 20 and ranking["aircraft"].is_monotonic_decreasing and ranking["aircraft"].min() > 0
               and ((ranking["aircraft_per_km2"] - ranking["aircraft"] / ranking["area_km2"]).abs() < 1e-6).all())
    except Exception as _e:
        _ok = False
    if _ok:
        _top = ranking.head(3)
        _dense = ranking.sort_values("aircraft_per_km2", ascending=False).iloc[0]
        _fig = px.choropleth_map(
            ranking, geojson=gemeinden_map(set(ranking["gdenr"]), MERGERS_2025), locations="gdenr", color="aircraft",
            hover_name="name", hover_data={"gdenr": False, "population": ":,", "aircraft_per_km2": ":.1f"},
            color_continuous_scale="OrRd", map_style="carto-positron", zoom=7, center={"lat": 46.8, "lon": 8.2},
            opacity=0.75, height=480, labels={"aircraft": "aircraft < 3,000 ft AGL"})
        _out = mo.vstack([
            check(True, f"{len(ranking)} municipalities saw at least one aircraft below 3,000 ft above ground. Most: "
                        + ", ".join(f"{r['name']} ({r['aircraft']})" for _, r in _top.iterrows())
                        + f" – densest: {_dense['name']} ({_dense['aircraft_per_km2']:.1f} aircraft per km²)", ""),
            _fig, ranking.head(15)])
    else:
        _out = check(False, "", "Expected one row per municipality with aircraft (> 0, different icao24 below "
                                "3,000 ft AGL), aircraft_per_km2, aircraft_per_1000 and area_km2, most aircraft first.")
    _out
    return


@app.cell
def _(geoms, gg, shapely):
    def gemeinden_map(keep, mapping):
        """Light GeoJSON for plotly: only the municipalities in `keep`, simplified to ~100 m, id = new GDENR."""
        _ids = gg["GDENR"].replace(mapping).to_numpy()
        _idx = [k for k, g in enumerate(_ids) if g in keep]
        _simple = shapely.simplify(geoms[_idx], 0.001, preserve_topology=True)
        return {"type": "FeatureCollection",
                "features": [{"type": "Feature", "id": int(_ids[k]), "properties": {},
                              "geometry": shapely.geometry.mapping(g)} for k, g in zip(_idx, _simple)]}
    return (gemeinden_map,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## People under the tracks

    Municipalities are coarse: Kloten has 22,017 residents, but not all of them live under the runway axis. The BFS
    publishes the population per **100 m cell** in Swiss coordinates (**LV95**, metres east/north, EPSG:2056). That
    is handy: in LV95, "within 500 m" is plain Pythagoras. swisstopo publishes
    [approximate formulas](https://www.swisstopo.admin.ch/dam/en/sd-web/KLRCX9XIdXDu/ch1903wgs84-EN.pdf) for the
    conversion (better than 1 m from WGS84 to LV95, about 0.1" back; no extra library):
    """)
    return


@app.cell
def _(grid, noise, np, shapely):
    def wgs84_to_lv95(lon, lat):
        """swisstopo approximate formulas, WGS84 degrees -> LV95 metres (E, N)."""
        p = (np.asarray(lat) * 3600 - 169028.66) / 10000
        l = (np.asarray(lon) * 3600 - 26782.5) / 10000          # noqa: E741
        e = 2600072.37 + 211455.93 * l - 10938.51 * l * p - 0.36 * l * p**2 - 44.54 * l**3
        n = 1200147.07 + 308807.95 * p + 3745.25 * l**2 + 76.63 * p**2 - 194.56 * l**2 * p + 119.79 * p**3
        return e, n

    def lv95_to_wgs84(e, n):
        """Inverse: LV95 metres -> WGS84 degrees (lon, lat)."""
        y = (np.asarray(e) - 2600000) / 1e6
        x = (np.asarray(n) - 1200000) / 1e6
        lon = 2.6779094 + 4.728982 * y + 0.791484 * y * x + 0.1306 * y * x**2 - 0.0436 * y**3
        lat = 16.9023892 + 3.238272 * x - 0.270978 * y**2 - 0.002528 * x**2 - 0.0447 * y**2 * x - 0.0140 * x**3
        return lon * 100 / 36, lat * 100 / 36

    LSZH_E, LSZH_N = wgs84_to_lv95(8.5481, 47.4581)          # airport reference point, OurAirports

    # hectare cells within 30 km of Zurich airport; E/N_KOORD is the south-west (lower-left) corner -> +50 m = centre.
    # Values 1–3 are published as 3 (data protection) – both: BFS geodata page "Population and households from 2010".
    grid_zh = (grid.assign(e=grid["E_KOORD"] + 50, n=grid["N_KOORD"] + 50)
               .rename(columns={"NUMMER": "population"})[["e", "n", "population"]])
    grid_zh = grid_zh[np.hypot(grid_zh["e"] - LSZH_E, grid_zh["n"] - LSZH_N) <= 30_000].reset_index(drop=True)

    # official day-time noise contours of LSZH (Lr 06–22 h, total civil traffic, LSV annex 5) as polygons in LV95.
    # The WFS layer also has other airfields (e.g. Dübendorf) -> keep only the LSZH cadastre (ch.admin.bazl.lszh.*)
    def is_lszh_day(f, level):
        return (f["properties"]["identifier"].startswith("ch.admin.bazl.lszh.")
                and f["properties"]["exposuretype"] == "OverallTrafficDay_Lr" and f["properties"]["level_db"] == level)

    def _contour(level):
        _rings = [f["geometry"]["coordinates"] for f in noise["features"] if is_lszh_day(f, level)]
        return shapely.union_all([shapely.Polygon(np.column_stack(wgs84_to_lv95(*np.array(r).T))) for r in _rings])

    noise_day = {57: _contour(57), 60: _contour(60)}
    return LSZH_E, LSZH_N, grid_zh, is_lszh_day, lv95_to_wgs84, noise_day, wgs84_to_lv95


@app.cell(hide_code=True)
def _(grid_zh, mo):
    mo.md(rf"""
    `grid_zh`: **{len(grid_zh):,} inhabited cells** with **{grid_zh['population'].sum():,} residents** within 30 km of
    LSZH. `noise_day[57]` and `noise_day[60]`: the areas inside the official 57 and 60 dB day contours of the BAZL
    noise cadastre (valid from 3.7.2015). Noise Abatement Ordinance (LSV/NAO, SR 814.41)
    [annex 5, no. 221](https://www.fedlex.admin.ch/eli/cc/1987/338_338_338/en): day (06–22 h) limits for civil
    aerodromes with large aircraft, sensitivity level II (zones without disturbing businesses, notably residential
    zones): **57 dB planning value, 60 dB impact threshold (Immissionsgrenzwert), 65 dB alarm value**. For mixed
    zones (level III) 60 dB is the planning value.

    ## TODO 12.4 – How many people live under the tracks?

    Take the positions of `low_muni` **less than 3,000 ft above ground** and **within 30 km of LSZH**
    (`LSZH_E`, `LSZH_N`), convert them to LV95 and find for every hectare cell the **different aircraft** that passed
    within **500 m** (horizontal distance). A cell is "under the tracks" if **at least 10 different aircraft** passed.
    Store those cells in `under` (columns of `grid_zh` + `aircraft`).

    /// details | Hint
    The same STRtree as in 12.2, now over the cell centres and with a distance predicate:
    ```python
    _tree = shapely.STRtree(shapely.points(grid_zh["e"], grid_zh["n"]))
    _i, _j = _tree.query(shapely.points(_near["e"], _near["n"]), predicate="dwithin", distance=500)
    ```
    `_i` = position, `_j` = cell. A DataFrame `{{"icao24": ..., "cell": _j}}` → `groupby("cell")["icao24"].nunique()`.
    ///
    """)
    return


@app.cell
def _(LSZH_E, LSZH_N, grid_zh, low_muni, np, pd, shapely, wgs84_to_lv95):
    # TODO 12.4: under = hectare cells with >= 10 different aircraft < 3000 ft AGL within 500 m (+ column aircraft)
    under = grid_zh.head(0).assign(aircraft=0)
    return (under,)


@app.cell(hide_code=True)
def _(check, go, grid_zh, is_lszh_day, lv95_to_wgs84, mo, noise, noise_day, np, px, shapely, under):
    try:
        _ok = ({"e", "n", "population", "aircraft"} <= set(under.columns) and len(under) > 20
               and under["aircraft"].min() >= 10 and under[["e", "n"]].isin(grid_zh[["e", "n"]]).all().all())
    except Exception:
        _ok = False
    if _ok:
        _people = int(under["population"].sum())
        _in = {lvl: shapely.contains_xy(poly, grid_zh["e"].to_numpy(), grid_zh["n"].to_numpy())
               for lvl, poly in noise_day.items()}
        _pop_in = {lvl: int(grid_zh["population"].to_numpy()[m].sum()) for lvl, m in _in.items()}
        _under_in60 = int(under["population"].to_numpy()[
            shapely.contains_xy(noise_day[60], under["e"].to_numpy(), under["n"].to_numpy())].sum())
        _lon, _lat = lv95_to_wgs84(under["e"], under["n"])
        _fig = px.scatter_map(under.assign(lon=_lon, lat=_lat), lat="lat", lon="lon", color="aircraft",
                              size="population", size_max=12, color_continuous_scale="OrRd", opacity=0.8,
                              map_style="carto-positron", zoom=9.3, center={"lat": 47.47, "lon": 8.55}, height=520,
                              hover_data={"population": True, "aircraft": True, "lat": False, "lon": False})
        for _lvl, _color in [(57, "#2b8cbe"), (60, "#08306b")]:
            _xs, _ys = [], []
            for _f in noise["features"]:
                if is_lszh_day(_f, _lvl):
                    _c = np.array(_f["geometry"]["coordinates"])
                    _xs += list(_c[:, 0]) + [None]
                    _ys += list(_c[:, 1]) + [None]
            _fig.add_trace(go.Scattermap(lon=_xs, lat=_ys, mode="lines", line={"color": _color, "width": 2},
                                         name=f"{_lvl} dB day contour"))
        _fig.update_layout(legend={"orientation": "h", "y": -0.05})
        _out = mo.vstack([
            check(True,
                  f"**{_people:,} people** live in {len(under):,} hectare cells under the tracks (≥ 10 aircraft below "
                  f"3,000 ft AGL within 500 m). The official contours: {_pop_in[57]:,} residents inside 57 dB, "
                  f"{_pop_in[60]:,} inside 60 dB (day). {_under_in60:,} of the people under the tracks live inside "
                  f"the 60 dB contour, {_people - _under_in60:,} outside.", ""),
            _fig])
    else:
        _out = check(False, "", "Expected the cells of grid_zh with a column aircraft (>= 10 different aircraft "
                                "within 500 m, below 3,000 ft AGL, within 30 km of LSZH).")
    _out
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Think about it

    - **Lakes**: positions over Lake Zurich or Lake Geneva match no municipality. Who hears an aircraft over the lake
      – nobody, or both shores? How would you assign them (nearest municipality, a buffer)?
    - **Which runways?** Your data covers one or two days. Zurich changes its runway configuration with the time of
      day and the wind ([Flughafen Zürich, operating concepts](https://www.flughafen-zuerich.ch/de/unternehmen/verantwortung/laerm-und-schallschutz/flugbetrieb/betriebskonzepte)):
      landings from the south on runway 34 early in the morning (06–07 h on weekdays, 06–09 h on weekends –
      German rules for flights over its territory), from the north on 14/16 during the day, from the east on
      runway 28 in the evening (from 21 h on weekdays, 20 h on weekends); with bise (north-east wind) other runways.
      Which municipalities in the ranking would swap places with a week of bise?
    - **Night vs day**: the contours you compared with are for the day (06–22 h). Regular flights run from 06:00
      to 23:30; 23:00–23:30 is for delays only
      ([Flughafen Zürich](https://www.flughafen-zuerich.ch/de/unternehmen/magazin/2025/politik_betriebszeiten)).
      Filter `low_muni` to 22:00–06:00 Swiss time (`timezone('Europe/Zurich', ...)` or pandas
      `tz_localize("UTC").tz_convert("Europe/Zurich")`) – who is overflown in the late evening, and how does it
      compare with the `FirstNightHour_Lr` (22–23 h) and `SecondNightHour_Lr` (23–24 h) contours?
    - **Above ground vs above sea**: compare the ranking with a plain `altitude_ft < 5000` filter. Which Alpine
      municipalities (helicopters, small aircraft) appear only with the AGL version – and where is the mean
      elevation of the municipality a bad proxy?
    - **Fairness**: per km², per resident or per aircraft? A small village with 2,000 residents under the approach
      and a city district with 50,000 at the edge of a contour – who should get noise protection first? And what does
      "10 aircraft" mean without knowing whether it was an A220 or a Boeing 777 (noise depends on type and thrust)?

    **Next:** [13 – Ground weather at Kloten](13_meteoswiss.py) – or back to [10](10_analysis.py) for your own
    question, e.g. "which municipality is overflown most during the curfew".
    """)
    return


if __name__ == "__main__":
    app.run()
