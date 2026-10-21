"""Live Radar – a Databricks App (Dash) on top of the flight_tracking pipeline.

A dark "control room" view that refreshes every 15 seconds: full-screen map (colour = altitude, recorded tracks,
SIGMET areas, planned routes), details of a clicked aircraft, airports, alerts, CO2 – and start/stop of the pipeline.
Local development without Databricks:  USE_MOCK=true python app.py   (replays recorded OpenSky snapshots)
"""
import json
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor

import dash
import dash_bootstrap_components as dbc
import dash_leaflet as dl
import pandas as pd
import plotly.graph_objects as go
from dash import ALL, Input, Output, State, dash_table, dcc, html
from flask import request

import radar
from backend import GOLD, SILVER, MockBackend, TableMissing, get_backend

REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "15"))
TRACK_MINUTES = int(os.getenv("TRACK_MINUTES", "60"))               # how far back the recorded tracks reach
TRACK_RESOLUTION = int(os.getenv("TRACK_RESOLUTION_SECONDS", "0"))  # 0 = every snapshot; auto 60 s for > 1000 aircraft
TRACKS_MAX = int(os.getenv("TRACKS_MAX_AIRCRAFT", "500"))   # "all tracks" draws at most this many (closest to center)
BIG = int(os.getenv("BIG_REGION_AIRCRAFT", "1500"))  # more aircraft in the view: tracks/routes only for the closest
DENSITY_ABOVE = int(os.getenv("DENSITY_ABOVE_AIRCRAFT", "3000"))  # more in the view -> density map instead of points
# map views (lamin, lamax, lomin, lomax) – the app only loads aircraft inside the visible part of the map
VIEWS = {"auto": None, "switzerland": (45.6, 48.0, 5.8, 10.7), "europe": (34.0, 72.0, -25.0, 45.0),
         "middle_east": (12.0, 42.0, 25.0, 63.0), "north_america": (14.0, 72.0, -170.0, -50.0),
         "south_america": (-56.0, 14.0, -92.0, -30.0), "africa": (-36.0, 38.0, -20.0, 52.0),
         "asia": (-10.0, 60.0, 60.0, 150.0), "oceania": (-50.0, 0.0, 110.0, 180.0), "world": (-90.0, 90.0, -180.0, 180.0)}
VIEW_LABELS = {"auto": "Auto", "switzerland": "Switzerland", "europe": "Europe", "middle_east": "Middle East",
               "north_america": "North America", "south_america": "South America", "africa": "Africa", "asia": "Asia",
               "oceania": "Oceania", "world": "World"}
MAP_STYLE = os.getenv("MAP_STYLE", "carto-darkmatter")               # needs no map token
ALTITUDE_SCALE = [[0, "#22d3ee"], [0.25, "#34d399"], [0.5, "#facc15"], [0.75, "#fb923c"], [1, "#f43f5e"]]
ACCENT, ORANGE, MUTED = "#38bdf8", "#f97316", "#8b98a5"
CONTRAIL_COLORS = {"persistent": "rgba(232,121,249,0.55)", "short-lived": "rgba(186,230,253,0.45)"}

backend = get_backend()
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY], title="Live Radar",
                suppress_callback_exceptions=True)          # the ✕ button only exists while an aircraft is selected
server = app.server


# ------------------------------------------------------------------------------------------------ helpers
def user_token():
    try:
        return request.headers.get("x-forwarded-access-token")
    except RuntimeError:                                  # outside a request (startup)
        return None


def q(statement, token=None):
    return backend.query(statement, token or user_token())


POOL = ThreadPoolExecutor(max_workers=8)                  # persistent: its threads keep their warehouse connections


def parallel(*calls, token=None):
    """Run independent lookups at the same time (each one may hit the warehouse or an external API). A failing call
    returns None. Calls get the viewer's token, which is only readable in the request thread."""
    token = token or user_token()

    def run(fn):
        try:
            return fn(token)
        except Exception:
            return None
    return list(POOL.map(run, calls))


_photo_cache = {}                                         # icao24 -> (looked_up_at, photo dict or None)


def viewer_contact():
    try:
        return request.headers.get("x-forwarded-email") or os.getenv("PHOTO_CONTACT", "")
    except RuntimeError:
        return os.getenv("PHOTO_CONTACT", "")


def photo_on_demand(icao24, contact=None):
    """planespotters.net photo for the clicked aircraft, when the pipeline table has none (it only covers aircraft
    near airports). Terms: User-Agent with a contact (the viewer's Databricks e-mail or PHOTO_CONTACT), cache at most
    24 h, show the photographer and link the thumbnail to the photo page. One call per click, never in bulk."""
    import time

    import requests
    hit = _photo_cache.get(icao24)
    if hit and time.time() - hit[0] < 24 * 3600:
        return hit[1]
    contact = contact or viewer_contact()
    if not contact:
        return None
    try:
        r = requests.get(f"https://api.planespotters.net/pub/photos/hex/{icao24.upper()}", timeout=5,
                         headers={"User-Agent": f"databricks-cookbook-flight-tracking/1.0 (teaching demo; +{contact})"})
        photos = r.json().get("photos") if r.ok else None
    except Exception:                                     # not reachable from this workspace -> no photo
        return None
    p = photos[0] if photos else None
    photo = p and {"thumbnail_url": (p.get("thumbnail_large") or p.get("thumbnail") or {}).get("src"),
                   "photo_link": p.get("link"), "photographer": p.get("photographer")}
    _photo_cache[icao24] = (time.time(), photo)
    return photo


def haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(float(lat1)), math.radians(float(lat2))
    a = (math.sin((p2 - p1) / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(math.radians(float(lon2) - float(lon1)) / 2) ** 2)
    return 2 * 6371 * math.asin(math.sqrt(a))


_route_cache = {}                                         # callsign -> (looked_up_at, route dict or None)


def route_on_demand(callsign):
    """Scheduled route of one callsign from adsbdb (https://www.adsbdb.com), when the pipeline's table has none (it
    only looks up a limited number of callsigns). Cached 30 minutes; route data © David Taylor & Jim Mason – shown,
    never stored or exported."""
    import requests
    cs = "".join(c for c in (callsign or "").strip().upper() if c.isalnum())
    if len(cs) < 4:
        return None
    hit = _route_cache.get(cs)
    if hit and time.time() - hit[0] < 1800:
        return hit[1]
    route = None
    try:
        r = requests.get(f"https://api.adsbdb.com/v0/callsign/{cs}", timeout=5,
                         headers={"User-Agent": "databricks-cookbook-flight-tracking/1.0 (teaching demo)"})
        fr = (r.json().get("response") or {}).get("flightroute") if r.ok else None
        if isinstance(fr, dict) and fr.get("origin") and fr.get("destination"):
            o, d = fr["origin"], fr["destination"]
            route = {"has_route": True, "callsign": cs, "airline_name": (fr.get("airline") or {}).get("name"),
                     **{f"{k}_{f}": ap.get(src) for k, ap in (("origin", o), ("destination", d))
                        for f, src in (("icao", "icao_code"), ("iata", "iata_code"), ("name", "name"),
                                       ("lat", "latitude"), ("lon", "longitude"))}}
    except Exception:                                     # not reachable / unknown callsign -> no route
        pass
    _route_cache[cs] = (time.time(), route)
    return route


def bearing_deg(lat1, lon1, lat2, lon2):
    p1, p2, dl_ = math.radians(lat1), math.radians(lat2), math.radians(lon2 - lon1)
    x = math.sin(dl_) * math.cos(p2)
    y = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl_)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def route_misfit(route, now):
    """Why a scheduled route cannot be what this aircraft flies right now – or None. Callsigns are reused, so adsbdb
    sometimes returns an old route (e.g. Lyon -> Naples for an aircraft over Lake Constance heading north)."""
    try:
        o, d = (route["origin_lat"], route["origin_lon"]), (route["destination_lat"], route["destination_lon"])
        ac = (float(now["latitude"]), float(now["longitude"]))
        gcd, via = haversine_km(*o, *d), haversine_km(*o, *ac) + haversine_km(*ac, *d)
        if via > 1.3 * gcd + 150:
            return f"it is {via - gcd:,.0f} km off the direct line"
        left, heading = haversine_km(*ac, *d), now.get("heading")
        if heading is not None and heading == heading and left > 150 and not now.get("on_ground"):
            off = abs((bearing_deg(*ac, *d) - float(heading) + 180) % 360 - 180)
            if off > 100:
                return f"it flies away from {route.get('destination_iata') or 'the destination'} (heading {float(heading):.0f}°)"
    except (KeyError, TypeError, ValueError):
        pass
    return None


ESTIMATE_AFTER_S = 45                                     # older positions are projected forward (world: every 6 min)
MAX_AGE_S = int(os.getenv("MAX_POSITION_AGE_SECONDS", "600"))


def estimate_positions(df):
    """Switzerland arrives every 35 s, the world only every 6 minutes. An aircraft over the Middle East would jump 80 km
    at every world file – so older positions are moved along speed and heading to the newest snapshot time
    (dead reckoning) and flagged as estimated. Aircraft on the ground stay where they are."""
    if df.empty:
        df = df.copy()
        df["position_age_s"], df["estimated"] = [], []
        return df
    import numpy as np
    df = df.copy()
    if "position_age_s" not in df or df["position_age_s"].isna().all():      # local mock / older pipeline
        t = pd.to_datetime(df["snapshot_time"])
        df["position_age_s"] = (t.max() - t).dt.total_seconds()
    df = df[df["position_age_s"].fillna(0) <= MAX_AGE_S]
    age = df["position_age_s"].fillna(0).astype(float)
    move = ((age > ESTIMATE_AFTER_S) & ~df["on_ground"].fillna(False).astype(bool)
            & df["speed_kmh"].notna() & df["heading"].notna())
    km = df["speed_kmh"].fillna(0).astype(float) * age / 3600
    h = np.radians(df["heading"].fillna(0).astype(float))
    lat = df["latitude"].astype(float)
    df.loc[move, "latitude"] = (lat + km * np.cos(h) / 111.2)[move]
    df.loc[move, "longitude"] = (df["longitude"].astype(float)
                                 + km * np.sin(h) / (111.2 * np.cos(np.radians(lat)).clip(lower=0.1)))[move]
    df["longitude"] = (df["longitude"] + 180) % 360 - 180
    df["estimated"] = move
    return df


ENRICH_SECONDS = int(os.getenv("ENRICH_CACHE_SECONDS", "60"))   # enrichment tables change once per pipeline update


class Snapshot:
    """The small gold tables in app memory, refreshed at most every few seconds by ONE thread. Clicks read from here
    instead of the SQL warehouse (each warehouse query costs ~0.4 s even when trivial). Switzerland: ~100 rows per
    table; world: ~13,000 – a few MB."""

    def __init__(self):
        import threading
        self.lock, self.loaded, self.frames = threading.Lock(), {}, {}

    def _sql(self, table):
        history = table.endswith(("fuel_burn", "navaid_passages"))       # several rows per aircraft -> latest
        return (f"SELECT * FROM {table}"
                + (" QUALIFY ROW_NUMBER() OVER (PARTITION BY icao24 ORDER BY snapshot_time DESC) = 1" if history else ""))

    def _refresh(self, tables, max_age, token):
        stale = [t for t in tables if time.time() - self.loaded.get(t, 0) > max_age]
        if not stale:
            return
        with self.lock:
            stale = [t for t in stale if time.time() - self.loaded.get(t, 0) > max_age]   # another thread did it
            frames = parallel(*[lambda tok, t=t: q(self._sql(t), tok) for t in stale], token=token)
            for t, df in zip(stale, frames):
                if df is None:
                    if t == f"{GOLD}.current_positions":
                        raise RuntimeError("current_positions not readable")
                    df = pd.DataFrame(columns=["icao24"])
                if "snapshot_time" in df and len(df):
                    df = df.sort_values("snapshot_time", ascending=False)
                df = df.drop_duplicates("icao24") if "icao24" in df else df
                if t == f"{GOLD}.current_positions":
                    df = estimate_positions(df)
                self.frames[t], self.loaded[t] = df, time.time()

    def positions(self, token=None):
        self._refresh([f"{GOLD}.current_positions"], max(REFRESH_SECONDS - 2, 3), token)
        return self.frames[f"{GOLD}.current_positions"]

    def rows(self, icao24, token=None):
        """{table: row dict} of one aircraft over the enrichment tables that exist (+ its current position)."""
        have = existing_tables(token)
        self._refresh([t for t, _, _ in ENRICH if t in have or isinstance(backend, MockBackend)], ENRICH_SECONDS, token)
        out = {}
        for t in [f"{GOLD}.current_positions"] + [t for t, _, _ in ENRICH]:
            df = self.frames.get(t)
            if df is not None and "icao24" in df:
                hit = df[df["icao24"] == icao24]
                if len(hit):
                    out[t] = {k: (None if isinstance(v, float) and v != v else v) for k, v in hit.iloc[0].to_dict().items()}
        return out


SNAP = Snapshot()


def section(title, text):
    """Section heading plus a short explanation: what you see and why it matters."""
    return html.Div([html.Div(title, className="section-title"), html.Div(text, className="section-help")])


def hint(text):
    return html.Div(text, className="hint")


def dark_layout(fig, height=260, title=None):
    fig.update_layout(template="plotly_dark", height=height, title=title, paper_bgcolor="rgba(0,0,0,0)",
                      plot_bgcolor="rgba(0,0,0,0)", margin={"l": 40, "r": 10, "t": 36 if title else 10, "b": 30},
                      font={"family": "Inter, sans-serif", "size": 12}, legend={"orientation": "h", "y": -0.2})
    return fig


def table(df, page_size=10):
    if df is None or df.empty:
        return hint("Nothing right now.")
    df = df.copy()
    for c in df.columns:                                  # timestamps and floats readable
        if pd.api.types.is_datetime64_any_dtype(df[c]):
            df[c] = df[c].dt.strftime("%H:%M:%S")
        elif pd.api.types.is_float_dtype(df[c]):
            df[c] = df[c].round(1)
    return dash_table.DataTable(
        df.to_dict("records"), [{"name": c.replace("_", " "), "id": c} for c in df.columns],
        page_size=page_size, sort_action="native",
        style_table={"overflowX": "auto"},
        style_header={"backgroundColor": "#16212c", "color": MUTED, "fontWeight": "600", "border": "none",
                      "textTransform": "uppercase", "fontSize": 11},
        style_cell={"backgroundColor": "#111922", "color": "#e6edf3", "border": "none",
                    "borderBottom": "1px solid rgba(255,255,255,.06)", "fontFamily": "Inter", "fontSize": 13,
                    "padding": "6px 10px", "textAlign": "left"},
        style_data_conditional=[{"if": {"row_index": "odd"}, "backgroundColor": "#0f1720"}])


def optional_table(statement, missing_text, page_size=10):
    try:
        return table(q(statement), page_size)
    except TableMissing:
        return hint(missing_text)
    except Exception as e:
        return hint(f"Query failed: {str(e)[:200]}")


def optional_count(statement):
    try:
        return len(q(statement))
    except Exception:
        return None


def kpi_card(value, label, alert=False):
    return dbc.Col(html.Div([html.B("–" if value is None else value), html.Small(label)],
                            className="card-kpi" + (" alert" if alert and value else "")), md=3, className="mb-2")


def map_view(now, relayout):
    """Center and zoom: the user's view if they moved the map, otherwise fitted to all aircraft."""
    if relayout and "map.center" in relayout:             # keep the user's zoom and position between refreshes
        return relayout["map.center"], relayout.get("map.zoom", 6.6)
    lat, lon = now["latitude"].dropna(), now["longitude"].dropna()
    if lat.empty:
        return {"lat": 46.8, "lon": 8.2}, 6.6
    span = max(lat.quantile(.98) - lat.quantile(.02), (lon.quantile(.98) - lon.quantile(.02)) / 2, 0.5)
    zoom = max(1.0, min(7.5, math.log2(180 / span) - 0.3))
    return {"lat": float(lat.median() if span > 60 else lat.mean()), "lon": float(lon.median() if span > 60 else lon.mean())}, zoom


def fit_box(box):
    """Center and zoom that show the box (lamin, lamax, lomin, lomax) on a ~2:1 map."""
    lamin, lamax, lomin, lomax = box
    span = max(lamax - lamin, (lomax - lomin) / 2, 0.5)
    return {"lat": (lamin + lamax) / 2, "lon": (lomin + lomax) / 2}, max(0.8, min(7.5, math.log2(180 / span) - 0.2))


def view_box(relayout, center, zoom):
    """Visible map area (lamin, lamax, lomin, lomax): exact corners from Plotly if it reports them, else estimated."""
    corners = (relayout or {}).get("map._derived", {}).get("coordinates")
    if corners:
        lons, lats = [c[0] for c in corners], [c[1] for c in corners]
        box = (min(lats), max(lats), min(lons), max(lons))
    else:
        half_lon = 360 / 2 ** zoom * 0.75
        box = (center["lat"] - half_lon / 2, center["lat"] + half_lon / 2, center["lon"] - half_lon, center["lon"] + half_lon)
    lamin, lamax, lomin, lomax = max(box[0], -90), min(box[1], 90), box[2], box[3]
    if lomax - lomin >= 360 or lomin < -180 or lomax > 180:  # whole world or across the date line: no lon filter
        lomin, lomax = -180.0, 180.0
    return lamin, lamax, lomin, lomax


def box_sql(box):
    return (f"latitude BETWEEN {box[0]:.3f} AND {box[1]:.3f} AND longitude BETWEEN {box[2]:.3f} AND {box[3]:.3f}")


def in_box(df, box):
    """Same filter in pandas (the local mock ignores SQL WHERE clauses)."""
    return df[df["latitude"].between(box[0], box[1]) & df["longitude"].between(box[2], box[3])]


def in_view(df, center, zoom, margin=1.3):
    """Rows of df (latitude/longitude) inside the visible map area (rough, for a ~2:1 map)."""
    half_lon = min(180.0, 360 / 2 ** zoom * margin)
    half_lat = min(90.0, half_lon / 2)
    dlon = (df["longitude"] - center["lon"] + 180) % 360 - 180
    return df[(df["latitude"] - center["lat"]).abs().le(half_lat) & dlon.abs().le(half_lon)]


def great_circle(lat1, lon1, lat2, lon2, n=30):
    """Points on the shortest path over the globe – a long-haul route is a curve on a flat map, not a straight line."""
    p1, l1, p2, l2 = map(math.radians, (lat1, lon1, lat2, lon2))
    d = 2 * math.asin(math.sqrt(math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin((l2 - l1) / 2) ** 2))
    if d < 1e-6:
        return [lat1, lat2], [lon1, lon2]
    lats, lons = [], []
    for i in range(n + 1):
        f = i / n
        a, b = math.sin((1 - f) * d) / math.sin(d), math.sin(f * d) / math.sin(d)
        x = a * math.cos(p1) * math.cos(l1) + b * math.cos(p2) * math.cos(l2)
        y = a * math.cos(p1) * math.sin(l1) + b * math.cos(p2) * math.sin(l2)
        z = a * math.sin(p1) + b * math.sin(p2)
        lats.append(math.degrees(math.atan2(z, math.sqrt(x * x + y * y))))
        lons.append(math.degrees(math.atan2(y, x)))
    return lats, lons


def lines_by_aircraft(df):
    """One map trace for many lines: the points of each aircraft, separated by None."""
    lat, lon = [], []
    for _, g in df.sort_values(["icao24", "snapshot_time"]).groupby("icao24"):
        lat += g["latitude"].tolist() + [None]
        lon += g["longitude"].tolist() + [None]
    return lat, lon


# ------------------------------------------------------------------------------------------------- layout
LAYER_OPTIONS = [{"label": "Off", "value": "off"}, {"label": "Selected", "value": "selected"},
                 {"label": "All", "value": "all"}]


# hover help for the controls: (id, text) – shown as tooltips, so the panel itself stays short
HELP = {
    "filter": "Show only aircraft in these flight phases or of these airlines. The counts at the top always cover "
              "all aircraft.",
    "altitude": "Show only aircraft between these altitudes (barometric, in feet). 0 = on the ground; airliners "
                "cruise at 30,000–41,000 ft, light aircraft and helicopters mostly below 10,000 ft.",
    "track": "The path each aircraft has flown, from our own recorded positions. Off: none. Selected: only the "
             "clicked aircraft (last 3 hours). All: every aircraft in view (last 60 minutes; at most the 500 "
             "closest to the map centre).",
    "route": "Where the aircraft comes from and goes to – the scheduled route of its callsign (adsbdb). Grey = "
             "already flown, orange dashed = still ahead. Off: none. Selected: only the clicked aircraft, with "
             "airport names. All: every aircraft in view. Routes that cannot fit (callsign reused for another "
             "flight) are not drawn.",
    "restricted": "Flight information regions (FIR) with restrictions: closed or 'do not operate' airspace from "
                  "conflict-zone bulletins (EASA CZIB). Red = closed, orange = do not operate, yellow = caution. "
                  "The reason for many detours – compare the Detours tab.",
    "contrails": "A white trail behind aircraft that leave a contrail right now, from temperature and humidity at "
                 "their altitude (Schmidt-Appleman criterion). Long pink trail = persistent (can spread into cirrus "
                 "clouds and warm the climate), short light trail = short-lived (vanishes within a minute or two). "
                 "Only in the focus region.",
    "navaids": "Radio beacons aircraft navigate by: VOR (direction), DME (distance), NDB (old, simple beacon). "
               "Airways run from beacon to beacon, which is why traffic does not follow the great circle. "
               "Visible from zoom 8, names from zoom 9.",
    "pireps": "Pilot reports: turbulence or icing reported by crews, with altitude. Red = severe, orange = "
              "moderate, yellow = light, green = none. Mostly North America and oceanic routes – few in Europe.",
}


def with_help(text, key):
    """A label with a dotted underline; hovering shows the HELP text."""
    return html.Span([html.Span(text, id=f"help-{key}", className="has-help"),
                      dbc.Tooltip(HELP[key], target=f"help-{key}", placement="right", className="help-tip")])


LAYER_CHOICES = [{"label": with_help("Restricted airspace (FIR)", "restricted"), "value": "restricted"},
                 {"label": with_help("Contrails", "contrails"), "value": "contrails"},
                 {"label": with_help("Navaids (VOR/NDB, zoomed in)", "navaids"), "value": "navaids"},
                 {"label": with_help("Pilot reports (PIREP)", "pireps"), "value": "pireps"}]
FIR_ATTRIBUTION = "FIR boundaries: VATSIM VAT-Spy Data Project, CC BY-SA 4.0 · restrictions: curated list (EASA CZIBs)"


def geojson_rings(text):
    """GeoJSON Polygon / MultiPolygon text -> list of outer rings [(lons, lats), ...]."""
    try:
        g = json.loads(text) if isinstance(text, str) else text
    except Exception:
        return []
    polys = [g["coordinates"]] if g.get("type") == "Polygon" else g.get("coordinates", []) if g.get("type") == "MultiPolygon" else []
    return [([p[0] for p in poly[0]], [p[1] for p in poly[0]]) for poly in polys if poly]


def restricted_traces(alpha_fill=0.12):
    """Map traces for the restricted FIRs (gold.restricted_fir_shapes): red = closed, orange = do not operate."""
    shapes = q(f"SELECT fir_icao, fir_name, status, severity, restriction_ids, valid_until, confidence, geometry_geojson "
               f"FROM {GOLD}.restricted_fir_shapes")
    traces = []
    for r in shapes.itertuples():
        sev = f"{r.status} {r.severity}".lower().replace("_", " ")
        color = ("rgba(239,68,68,{a})" if "closed" in sev else "rgba(249,115,22,{a})" if "do not operate" in sev
                 else "rgba(250,204,21,{a})")      # red = closed, orange = do not operate, yellow = advisory only
        for lons, lats in geojson_rings(r.geometry_geojson):
            traces.append(go.Scattermap(lon=lons, lat=lats, mode="lines", fill="toself", showlegend=False,
                                        fillcolor=color.format(a=alpha_fill), line={"width": 1, "color": color.format(a=0.7)},
                                        hoverinfo="text", text=f"{r.fir_name} ({r.fir_icao}) – {r.status}, {r.severity}<br>"
                                                               f"{r.restriction_ids} · valid until {r.valid_until} · "
                                                               f"confidence {r.confidence}"))
    return traces


PIREP_COLORS = {"severe": "#ef4444", "moderate": "#f97316", "light": "#facc15", "none": "#22c55e",
                "not reported": "rgba(148,163,184,0.5)"}


def segmented(id_, value):
    return dbc.RadioItems(id=id_, value=value, options=LAYER_OPTIONS, className="btn-group btn-group-sm w-100",
                          inputClassName="btn-check", labelClassName="btn btn-outline-secondary flex-fill",
                          labelCheckedClassName="active")


topbar = html.Div([
    html.Div(["✈ Live ", html.Span("Radar")], className="brand"),
    *[html.Div([html.B("–", id=f"kpi-{k}"), html.Small(label)], className="kpi")
      for k, label in [("air", "in the air"), ("ground", "on ground"), ("airport", "near airports"),
                       ("snapshot", "last update UTC")]],
    html.Div(className="spacer"),
    dcc.Dropdown(id="f-view", value="auto", clearable=False, searchable=False, className="view-select",
                 options=[{"label": v, "value": k} for k, v in VIEW_LABELS.items()]),
    dcc.Dropdown(id="f-interval", value=int(getattr(backend, "interval", 35)), clearable=False, searchable=False,
                 className="view-select interval-select",
                 options=[{"label": f"Switzerland every {s} s", "value": s} for s in (10, 15, 35, 60)]),
    html.Span(id="credits", className="pill credits"),
    html.Span(id="pipeline-state", className="pill idle"),
    html.Button("▶ Start", id="btn-start", className="btn-ghost start"),
    html.Button("■ Stop", id="btn-stop", className="btn-ghost stop"),
], className="topbar")

controls = html.Div([
    html.H6(with_help("Filter", "filter")),
    dcc.Dropdown(id="f-phase", multi=True, placeholder="Flight phase",
                 options=["climbing", "cruise", "descending", "on ground"], className="mb-2"),
    dcc.Dropdown(id="f-airline", multi=True, placeholder="Airline", className="mb-2"),
    html.H6(with_help("Altitude (ft)", "altitude")),
    dcc.RangeSlider(id="f-alt", min=0, max=45000, step=1000, value=[0, 45000],
                    marks={0: "0", 15000: "15k", 30000: "30k", 45000: "45k"}),
    html.H6(with_help("Recorded track", "track")),
    segmented("f-track", "all"),
    html.H6(with_help("Planned route", "route")),
    segmented("f-routes", "selected"),         # the clicked aircraft shows where it comes from and goes to
    html.H6("Layers"),
    dbc.Checklist(id="f-layers", value=[], switch=True, className="layers", options=LAYER_CHOICES),
], className="controls glass")

legend = html.Div([
    html.Div([html.I(style={"background": "linear-gradient(90deg,#22d3ee,#34d399,#facc15,#fb923c,#f43f5e)"}),
              "aircraft · altitude low → high"]),
    html.Div([html.I(style={"background": ACCENT}), "recorded track"]),
    html.Div([html.I(style={"background": ORANGE}), "planned route ahead"]),
    html.Div([html.I(style={"background": "rgba(239,68,68,.6)"}), "SIGMET warning area"]),
    html.Div([html.I(style={"background": CONTRAIL_COLORS["persistent"]}), "persistent contrail (layer)"]),
], className="legend glass")

detours_page = html.Div(className="page", children=[
    html.Div([
        html.H5("Great circle vs. the route really flown", className="mb-1"),
        html.Div("CO₂ calculators start from the great-circle distance between the airports and add a flat detour "
                 "allowance (myclimate +95 km, ICAO +50/100/125 km). Closed airspace, weather and airway structure "
                 "make real routes longer – here measured with our own recorded positions.", className="hint"),
    ], className="mb-3"),
    html.Div(id="d-kpis"),
    dbc.Row([
        dbc.Col([
            dbc.Row([
                dbc.Col(dcc.Dropdown(id="d-flight", placeholder="Pick a flight (default: biggest detour)"), md=7),
                dbc.Col(dbc.RadioItems(id="d-status", value="ok", options=[
                    {"label": "Confirmed", "value": "confirmed"}, {"label": "Not contradicted", "value": "ok"},
                    {"label": "All routes", "value": "all"}],
                    className="btn-group btn-group-sm w-100", inputClassName="btn-check",
                    labelClassName="btn btn-outline-secondary flex-fill", labelCheckedClassName="active"), md=5),
            ], className="mb-2 g-2"),
            dcc.Graph(id="d-map", style={"height": "460px"}, config={"scrollZoom": True, "displaylogo": False}),
        ], md=8),
        dbc.Col(html.Div(id="d-compare"), md=4),
    ]),
    section('Biggest detours',
                'Flights whose recorded path is longest compared with the great circle between their airports. Extra km = flown distance minus great-circle distance (a lower bound: outside our region we assume a direct flight). Compare the extra km with the flat allowances of the CO₂ calculators on the right.'),
    html.Div(id="d-table"),
    html.Div("Lower bound: outside the region we assume the aircraft flies directly, so the real detour is at least "
             "this big. Routes come from adsbdb per callsign and can be wrong; implausible ones are left out. Extra "
             "fuel: OpenAP estimate for this aircraft type (mass and wind unknown).", className="hint mt-2"),
])


replay_bar = html.Div([
    html.Button("▶", id="rp-play", className="btn-ghost rp-btn", title="Play the recorded history"),
    html.Div(dcc.Slider(id="rp-time", min=0, max=1, value=1, step=1, marks=None, updatemode="mouseup",
                        tooltip=None), className="rp-slider"),
    html.Span("live", id="rp-label", className="rp-label"),
    dbc.RadioItems(id="rp-speed", value=1, inline=True, className="rp-speed",
                   options=[{"label": "1 min", "value": 1}, {"label": "5 min", "value": 5},
                            {"label": "15 min", "value": 15}]),
    html.Button("● Live", id="rp-live", className="btn-ghost rp-btn", title="Back to now"),
], className="replay glass")

radar_page = html.Div([
    dl.Map(id="map", center=[46.8, 8.2], zoom=7, minZoom=2, worldCopyJump=True, trackViewport=True, zoomControl=False,
           style={"position": "absolute", "inset": 0, "background": "#0b1117"}, children=[
        dl.ZoomControl(position="bottomright"),         # top-left is taken by the filter panel
        dl.TileLayer(url=radar.BASE_TILES, attribution=radar.ATTRIBUTION, maxZoom=16),
        dl.TileLayer(url=radar.LABEL_TILES, maxZoom=16),
        dl.LayerGroup(id="lyr-areas"),          # restricted airspace, SIGMETs
        dl.LayerGroup(id="lyr-lines"),          # tracks, routes
        dl.LayerGroup(id="lyr-points"),         # navaids, PIREPs
        # the aircraft: ONE GeoJSON layer (~150 bytes per aircraft); assets/radar_layer.js draws the symbols in the browser
        dl.GeoJSON(id="aircraft-geo", data=None, pointToLayer={"variable": "radarJs.pointToLayer"}),
        dl.LayerGroup(id="lyr-selected"),       # the clicked aircraft: halo, 3 h track, route – own callback, fast
    ]),
    html.Div(id="map-note"),
    html.Div(id="squawk-alarm", className="squawk-alarm"),
    replay_bar,
    controls,
    html.Div([html.Div(id="details-head"), html.Div(id="details-extra")], id="details", className="details glass"),
    legend,
], className="radar")

app.layout = html.Div([
    topbar,
    dcc.Interval(id="tick", interval=REFRESH_SECONDS * 1000),
    dcc.Store(id="selected"),
    dcc.Store(id="now-store"),              # basic data of the aircraft on the map -> instant details on click
    dcc.Store(id="rp-state"),               # replay: {"t": unix seconds, "playing": bool} – None = live
    dcc.Interval(id="rp-tick", interval=900, disabled=True),
    dbc.Tabs(id="tabs", active_tab="radar", children=[
        dbc.Tab(radar_page, label="Radar", tab_id="radar"),
        dbc.Tab(html.Div(id="tab-airports", className="page"), label="Airports", tab_id="airports"),
        dbc.Tab(html.Div(id="tab-alerts", className="page"), label="Alerts", tab_id="alerts"),
        dbc.Tab(html.Div(id="tab-aircraft", className="page"), label="Aircraft & CO₂", tab_id="aircraft"),
        dbc.Tab(detours_page, label="Detours", tab_id="detours"),
    ]),
    html.Div(["Data: ", html.A("The OpenSky Network", href="https://opensky-network.org", target="_blank"),
              " · Open-Meteo · aviationweather.gov · OurAirports · OpenFlights · adsbdb · planespotters.net · "
              "OpenAP (CO₂ is an estimate)"], className="footer"),
])


# ---------------------------------------------------------------------------------------------- callbacks
@app.callback(Output("rp-state", "data"), Output("rp-tick", "disabled"), Output("rp-play", "children"),
              Output("rp-time", "min"), Output("rp-time", "max"), Output("rp-time", "value"), Output("rp-time", "marks"),
              Output("rp-label", "children"),
              Input("tick", "n_intervals"), Input("rp-play", "n_clicks"), Input("rp-live", "n_clicks"),
              Input("rp-tick", "n_intervals"), Input("rp-time", "value"),
              State("rp-state", "data"), State("rp-speed", "value"))
def replay(_, play, live, step, slider, state, speed):
    """The time slider: live (state None) or a moment of the recorded history, optionally playing forward."""
    try:
        lo, hi = q(f"SELECT epoch(MIN(snapshot_time)), epoch(MAX(snapshot_time)) FROM {SILVER}.flights").iloc[0]
        lo, hi = int(lo), int(hi)
    except Exception:
        return None, True, "▶", 0, 1, 1, None, "no data yet"
    trig = dash.ctx.triggered_id
    t, playing = (state or {}).get("t"), bool((state or {}).get("playing"))
    if trig == "rp-live":
        t, playing = None, False
    elif trig == "rp-play":
        if playing:
            playing = False
        else:
            playing = True
            if t is None or t >= hi:                       # from live: start 3 hours back (or at the beginning)
                t = max(lo, hi - 3 * 3600)
    elif trig == "rp-tick" and t is not None and playing:
        t += int(speed or 1) * 60
        if t >= hi:
            t, playing = None, False                       # caught up with now -> live
    elif trig == "rp-time" and slider is not None and slider < hi - 30:
        t = int(slider)
    marks, h = {}, (lo // 3600 + 1) * 3600
    every = 3600 * max(1, (hi - lo) // (3600 * 12))        # at most ~12 labels
    while h < hi:
        marks[h] = pd.Timestamp(h, unit="s").strftime("%d.%m %H:%M" if h % 86400 == 0 else "%H:%M")
        h += every
    label = "live" if t is None else pd.Timestamp(t, unit="s").strftime("%a %d.%m %H:%M UTC")
    new_state = None if t is None else {"t": t, "playing": playing}
    return (new_state, not playing, "⏸" if playing else "▶", lo, hi, hi if t is None else t, marks, label)


def positions_at(t, window_minutes):
    """Where was every aircraft at unix time t? – current_positions, but for a moment of the past."""
    ts = pd.Timestamp(int(t), unit="s").strftime("%Y-%m-%d %H:%M:%S")
    return q(f"""
        SELECT f.snapshot_time, f.icao24, f.callsign, f.airline_icao,
               COALESCE(a.airline_name, 'unknown / private') AS airline_name, f.origin_country, f.latitude,
               f.longitude, f.altitude_ft, f.altitude_m, f.speed_kmh, f.heading, f.flight_phase, f.on_ground,
               f.category, f.region AS source_region,
               epoch(TIMESTAMP '{ts}') - epoch(f.snapshot_time) AS position_age_s
        FROM {SILVER}.flights f LEFT JOIN {SILVER}.airlines a USING (airline_icao)
        WHERE f.snapshot_time BETWEEN TIMESTAMP '{ts}' - INTERVAL {int(window_minutes)} MINUTE AND TIMESTAMP '{ts}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY f.icao24 ORDER BY f.snapshot_time DESC) = 1""")


@app.callback(Output("f-interval", "title"), Input("f-interval", "value"))
def set_interval(seconds):
    """How often the collector calls OpenSky – takes effect with the next call."""
    if seconds and hasattr(backend, "interval"):
        backend.interval = float(seconds)
    return f"one OpenSky call every {seconds} s = {86400 // int(seconds or 35):,} credits per day"


@app.callback(Output("credits", "children"), Output("pipeline-state", "children"), Output("pipeline-state", "className"),
              Input("tick", "n_intervals"), Input("btn-start", "n_clicks"), Input("btn-stop", "n_clicks"))
def status(_, start, stop):
    trigger = dash.ctx.triggered_id
    try:
        if trigger == "btn-start":
            backend.start_pipeline()
        elif trigger == "btn-stop":
            backend.stop_pipeline()
        st = backend.pipeline_status()
    except Exception as e:
        st = {"state": f"no access ({type(e).__name__})", "update": None}
    try:
        api = q(f"SELECT credits_remaining, access FROM (SELECT credits_remaining, "
                f"CASE WHEN authenticated THEN 'account' ELSE 'anonymous' END AS access, snapshot_time "
                f"FROM {GOLD}.api_usage ORDER BY snapshot_time DESC LIMIT 1)")
        left = int(api['credits_remaining'].iloc[0])
        hours = left * float(getattr(backend, "interval", 35)) / 3600      # 1 credit per call (Switzerland)
        credits = f"{left} credits · {api['access'].iloc[0]} · ≈ {hours:.1f} h at this rate"
    except Exception:
        credits = "credits –"
    running = st["update"] in ("RUNNING", "INITIALIZING", "SETTING_UP_TABLES", "WAITING_FOR_RESOURCES", "CREATED")
    return credits, f"● collector {(st['state'] if running else (st['update'] or st['state'])).lower()}", "pill ok" if running else "pill idle"


LEAFLET_VIEWS = {k: [[v[0], v[2]], [v[1], v[3]]] for k, v in VIEWS.items() if v}   # Leaflet bounds [[S, W], [N, E]]


@app.callback(Output("map", "bounds"), Input("f-view", "value"), prevent_initial_call=True)
def jump_to_view(view):
    """A chosen view (Europe, Middle East, ...) fits the map; 'auto' leaves it where it is."""
    return LEAFLET_VIEWS.get(view, dash.no_update)


def bounds_box(bounds, total):
    """(lamin, lamax, lomin, lomax) of the visible map – before Leaflet reports its bounds: Switzerland or the world."""
    if bounds:
        (s_, w_), (n_, e_) = bounds
        lomin, lomax = (w_, e_) if e_ - w_ < 360 and -180 <= w_ and e_ <= 180 else (-180.0, 180.0)
        return max(s_, -90.0), min(n_, 90.0), lomin, lomax
    return VIEWS["world"] if total > BIG else VIEWS["switzerland"]


@app.callback(Output("lyr-areas", "children"), Output("lyr-lines", "children"), Output("lyr-points", "children"),
              Output("aircraft-geo", "data"), Output("map-note", "children"), Output("now-store", "data"),
              Output("f-airline", "options"),
              Output("kpi-air", "children"), Output("kpi-ground", "children"), Output("kpi-snapshot", "children"),
              Output("kpi-airport", "children"),
              Input("tick", "n_intervals"), Input("f-phase", "value"), Input("f-airline", "value"), Input("f-alt", "value"),
              Input("f-track", "value"), Input("f-routes", "value"), Input("f-layers", "value"),
              Input("map", "bounds"), Input("map", "zoom"), Input("rp-state", "data"), State("selected", "data"))
def update_map(_, phases, airlines, alt, track_mode, route_mode, layers, bounds, zoom, replay_state, selected):
    """Everything on the map except the selection. A click does NOT run this (selected is State, not Input) – the
    selection has its own small callback below, so a click costs two quick queries instead of a full redraw."""
    layers, zoom = layers or [], zoom or 7
    # all positions come from the in-memory snapshot (one warehouse query per refresh, shared by all viewers)
    replay_t = (replay_state or {}).get("t")
    try:
        cp = SNAP.positions() if replay_t is None else estimate_positions(
            positions_at(replay_t, 8 if getattr(backend, "world", False) else 2))
    except Exception as e:
        return [], [], [], None, radar.map_note(f"No data yet – is the pipeline running? ({str(e)[:80]})"), {}, [], "–", "–", "–", "–"
    total = len(cp)
    ground = int(cp["on_ground"].fillna(False).astype(bool).sum()) if total else 0
    kpis = (total - ground, ground, pd.to_datetime(cp["snapshot_time"].max()).strftime("%H:%M:%S") if total else "–")
    near = optional_count(f"SELECT icao24 FROM {GOLD}.airport_approaches")
    box = bounds_box(bounds, total)

    areas, lines, points, aircraft, note = [], [], [], [], None
    now = in_box(cp, box)
    in_area = len(now)
    dots = in_area > DENSITY_ABOVE                        # zoomed out: every aircraft as a small dot (canvas), no tracks
    if dots:
        note = radar.map_note(f"{in_area:,} aircraft in view – zoom in for symbols and tracks")
    names = now[["airline_name", "airline_icao"]].dropna(subset=["airline_name"]).drop_duplicates("airline_name")
    airline_options = [{"label": f"{n} ({c})" if isinstance(c, str) and c and n != c else n, "value": n}
                       for n, c in sorted(names.itertuples(index=False, name=None), key=lambda t: str(t[0]).lower())]

    df = now
    if phases:
        df = df[df["flight_phase"].isin(phases)]
    if airlines:
        df = df[df["airline_name"].isin(airlines)]
    df = df[(df["altitude_ft"].fillna(0) >= alt[0]) & (df["altitude_ft"].fillna(0) <= alt[1])]
    big = len(df) > BIG                                   # many aircraft: smaller symbols, tracks only for the closest
    visible = df
    if big:
        clat, clon = (box[0] + box[1]) / 2, (box[2] + box[3]) / 2
        visible = df.loc[((df["latitude"] - clat) ** 2 + (df["longitude"] - clon) ** 2).nsmallest(BIG).index]
    if len(visible) > TRACKS_MAX:                         # recorded tracks / routes only for the closest few hundred
        clat, clon = (box[0] + box[1]) / 2, (box[2] + box[3]) / 2
        visible = visible.loc[((visible["latitude"] - clat) ** 2
                               + (visible["longitude"] - clon) ** 2).nsmallest(TRACKS_MAX).index]

    # restricted / closed airspace (src_geo/07–08) – the reason for many detours
    if "restricted" in layers:
        try:
            shapes = q(f"SELECT fir_icao, fir_name, status, severity, restriction_ids, valid_until, confidence, "
                       f"geometry_geojson FROM {GOLD}.restricted_fir_shapes")
            for r in shapes.itertuples():
                sev = f"{r.status} {r.severity}".lower().replace("_", " ")
                color = "#ef4444" if "closed" in sev else "#f97316" if "do not operate" in sev else "#facc15"
                for ring in radar.geojson_rings(r.geometry_geojson):
                    areas.append(radar.polygon(ring, color, 0.12, f"{r.fir_name} ({r.fir_icao}) – {r.status}, {r.severity} · "
                                                                  f"{r.restriction_ids} · valid until {r.valid_until}"))
            note = note or radar.map_note(FIR_ATTRIBUTION)
        except Exception:
            pass
    # SIGMET areas (thunderstorm / turbulence / icing)
    try:
        sig = q(f"SELECT sigmet_id, hazard_description, base_ft, top_ft, polygon_json FROM {SILVER}.sigmets "
                f"WHERE in_region AND current_timestamp() BETWEEN valid_from AND valid_to")
        for sg in sig[sig["polygon_json"].notna()].itertuples():
            ring = [[p[1], p[0]] for p in json.loads(sg.polygon_json)]
            areas.append(radar.polygon(ring, "#ef4444", 0.08, f"SIGMET {sg.sigmet_id}: {sg.hazard_description} "
                                                              f"{sg.base_ft or 'SFC'}–{sg.top_ft or '?'} ft"))
    except Exception:
        pass

    # recorded tracks of the aircraft in view over the last TRACK_MINUTES (one point per minute for big regions)
    if dots:
        visible = visible.iloc[0:0]
    if track_mode == "all" and len(visible):
        resolution = TRACK_RESOLUTION or (60 if len(visible) > 1000 or big else 0)
        _end = ("(SELECT MAX(snapshot_time) FROM {0}.flights)".format(SILVER) if replay_t is None else
                f"TIMESTAMP '{pd.Timestamp(int(replay_t), unit='s'):%Y-%m-%d %H:%M:%S}'")
        _minutes = TRACK_MINUTES if replay_t is None else 15        # replay: short trails, they move
        window = (f"snapshot_time BETWEEN {_end} - INTERVAL {_minutes} MINUTES AND {_end}"
                  f" AND region <> 'world'"               # world feed: one point per 6 min = long straight lines
                  f" AND latitude BETWEEN {visible['latitude'].min() - 5:.2f} AND {visible['latitude'].max() + 5:.2f}"
                  f" AND longitude BETWEEN {visible['longitude'].min() - 8:.2f} AND {visible['longitude'].max() + 8:.2f}")
        if resolution:
            ts = f"TIMESTAMP_SECONDS(FLOOR(UNIX_TIMESTAMP(snapshot_time) / {resolution}) * {resolution})"
            track_sql = (f"SELECT icao24, {ts} AS snapshot_time, MAX_BY(latitude, snapshot_time) AS latitude, "
                         f"MAX_BY(longitude, snapshot_time) AS longitude FROM {SILVER}.flights WHERE {window} "
                         f"GROUP BY icao24, {ts}")
        else:
            track_sql = f"SELECT icao24, snapshot_time, latitude, longitude FROM {SILVER}.flights WHERE {window}"
        try:
            trail = q(track_sql)
            trail = trail[trail["icao24"].isin(visible["icao24"])].sort_values(["icao24", "snapshot_time"])
            tip = visible.set_index("icao24")[["latitude", "longitude"]]
            groups = []
            for ic, g in trail.groupby("icao24"):           # each track ends at the symbol (estimated for world data)
                groups += radar.split_jumps(g[["latitude", "longitude"]].values.tolist() + [tip.loc[ic].tolist()])
            lines += radar.multi_line(groups, ACCENT, 1.3, 0.45)
        except Exception:
            pass

    # planned routes (adsbdb): all aircraft in view, or only the selected one
    if route_mode == "all":
        try:
            routes = q(f"SELECT * FROM {GOLD}.flights_with_routes WHERE has_route AND route_plausible")
            routes = routes[routes["icao24"].isin(visible["icao24"])]
            try:                                          # routes our own positions contradict are not drawn
                bad = q(f"SELECT icao24 FROM {GOLD}.route_efficiency WHERE route_status = 'contradicted'")
                routes = routes[~routes["icao24"].isin(bad["icao24"])]
            except Exception:
                pass
            done = [radar.great_circle(r.origin_lat, r.origin_lon, r.latitude, r.longitude, 20) for r in routes.itertuples()]
            ahead = [radar.great_circle(r.latitude, r.longitude, r.destination_lat, r.destination_lon, 20)
                     for r in routes.itertuples()]
            lines += radar.multi_line(done, "#8b98a5", 1.2, 0.45) + radar.multi_line(ahead, ORANGE, 1.4, 0.6, "4 6")
        except Exception:
            pass

    sel = df[df["icao24"] == selected] if selected else df.iloc[0:0]

    # contrails (src_weather/03)
    if "contrails" in layers:
        # a white trail behind each aircraft that leaves a contrail right now: short-lived ~2 min of its path,
        # persistent ~10 min (magenta tint – those can spread into cirrus clouds)
        try:
            con = q(f"SELECT icao24, contrail_class FROM {GOLD}.contrails "
                    f"WHERE contrail_class IN ('persistent', 'short-lived')")
            con = con[con["icao24"].isin(visible["icao24"])]
            if len(con):
                ids = "', '".join("".join(ch for ch in i if ch.isalnum()) for i in con["icao24"])
                path = q(f"SELECT icao24, snapshot_time, latitude, longitude FROM {SILVER}.flights "
                         f"WHERE icao24 IN ('{ids}') AND snapshot_time >= "
                         f"(SELECT MAX(snapshot_time) FROM {SILVER}.flights) - INTERVAL 10 MINUTES")
                path = path[path["icao24"].isin(con["icao24"])]
                tip = visible.set_index("icao24")[["latitude", "longitude"]]
                newest = pd.to_datetime(path["snapshot_time"]).max() if len(path) else None
                for kind, minutes, color, width in (("persistent", 10, "#f0abfc", 7), ("short-lived", 2, "#e0f2fe", 5)):
                    groups = []
                    for ic in con.loc[con["contrail_class"] == kind, "icao24"]:
                        g = path[(path["icao24"] == ic)
                                 & (pd.to_datetime(path["snapshot_time"]) >= newest - pd.Timedelta(minutes=minutes))]
                        pts = g.sort_values("snapshot_time")[["latitude", "longitude"]].values.tolist()
                        groups += radar.split_jumps(pts + [tip.loc[ic].tolist()])
                    lines += radar.multi_line(groups, color, width, 0.35)
        except Exception:
            pass
    # navaids (src_airport_ops/06) – only when zoomed in, labels from zoom 9
    if "navaids" in layers and zoom >= 8:
        try:
            nav = in_box(q(f"SELECT navaid_ident, label, frequency_label, latitude, longitude FROM {SILVER}.navaids "
                           f"WHERE {box_sql(box)}"), box)
            for n in nav.itertuples():
                points.append(radar.circle(n.latitude, n.longitude, 4, "#94a3b8",
                                           n.navaid_ident if zoom >= 9 else f"{n.label} {n.frequency_label or ''}",
                                           permanent=zoom >= 9))
        except Exception:
            pass
    # pilot reports (src_airport_ops/09): turbulence / icing reported by crews – mostly North America and oceans
    if "pireps" in layers:
        try:
            pr = in_box(q(f"SELECT latitude, longitude, altitude_ft, severity, hazard_text, raw_text FROM {SILVER}.pireps "
                          f"WHERE {box_sql(box)}"), box)
            for p_ in pr.itertuples():
                points.append(radar.circle(p_.latitude, p_.longitude, 4 if p_.severity == "not reported" else 7,
                                           PIREP_COLORS.get(p_.severity, MUTED),
                                           f"PIREP {p_.severity} · {radar._num(p_.altitude_ft):,} ft · {radar._text(p_.hazard_text)}"))
        except Exception:
            pass

    aircraft = radar.aircraft_geojson(df, small=big, dots=dots)     # the selection is a halo in its own layer
    store = {r.icao24: {"callsign": r.callsign, "airline_name": r.airline_name, "origin_country": r.origin_country,
                        "altitude_ft": r.altitude_ft, "speed_kmh": r.speed_kmh, "heading": r.heading,
                        "flight_phase": r.flight_phase, "latitude": r.latitude, "longitude": r.longitude,
                        "estimated": bool(r.estimated), "position_age_s": r.position_age_s}
             for r in (sel if big else df).itertuples()}     # world: only the selected one (keeps the page light)
    return areas, lines, points, aircraft, note, store, airline_options, *kpis, "–" if near is None else near


@app.callback(Output("lyr-selected", "children"), Input("selected", "data"), Input("f-track", "value"),
              Input("f-routes", "value"), Input("tick", "n_intervals"), Input("rp-state", "data"))
def draw_selected(selected, track_mode, route_mode, _, replay_state):
    """The clicked aircraft: white halo, its whole track (3 h) and its route with labelled airports – in parallel."""
    if not selected or (replay_state or {}).get("t"):          # replay: the halo would sit at today's position
        return []
    safe = "".join(c for c in selected if c.isalnum())

    rows = SNAP.rows(safe)
    now = rows.get(f"{GOLD}.current_positions")
    rt = rows.get(f"{GOLD}.flights_with_routes") if route_mode != "off" else None
    if rt is not None and not (rt.get("has_route") and rt.get("origin_lat") is not None):
        rt = None
    need_lookup = route_mode != "off" and rt is None and now
    def track(token):                                     # fixed time window (no MAX over silver): ~3x faster
        if track_mode == "off" or not now:
            return None
        end = pd.to_datetime(now["snapshot_time"])
        return q(f"SELECT snapshot_time, latitude, longitude FROM {SILVER}.flights WHERE icao24 = '{safe}' "
                 f"AND snapshot_time BETWEEN TIMESTAMP'{end - pd.Timedelta(hours=3):%Y-%m-%d %H:%M:%S}' "
                 f"AND TIMESTAMP'{end:%Y-%m-%d %H:%M:%S}' ORDER BY snapshot_time", token)

    tr, looked_up = parallel(track, lambda t: route_on_demand(now.get("callsign")) if need_lookup else None)
    rt = rt or looked_up
    out = []
    if tr is not None and len(tr) and now:
        # silver is often a few seconds ahead of the gold positions: cut the track at the symbol's time, end at the symbol
        tr = tr[pd.to_datetime(tr["snapshot_time"]) <= pd.to_datetime(now["snapshot_time"])][["latitude", "longitude"]].dropna()
        out += radar.multi_line(radar.split_jumps(tr.values.tolist() + [[now["latitude"], now["longitude"]]]),
                                ACCENT, 4, 0.9)
    if now:
        if rt and not route_misfit(rt, now):
            out += radar.route_layers({**rt, "latitude": now["latitude"], "longitude": now["longitude"]},
                                      label_airports=True)
        out.append(dl.CircleMarker(center=[now["latitude"], now["longitude"]], radius=17, color="#ffffff", weight=2,
                                   fill=False, interactive=False))
    return out


@app.callback(Output("selected", "data"), Input("aircraft-geo", "clickData"), prevent_initial_call=True)
def select(feature):
    """A click on an aircraft symbol selects it (clickData = the clicked GeoJSON feature)."""
    icao = ((feature or {}).get("properties") or {}).get("id")
    return icao or dash.no_update


@app.callback(Output("selected", "data", allow_duplicate=True), Input("map", "clickData"), prevent_initial_call=True)
def click_empty_map(_):
    return None                                           # a click into the map (not on an aircraft) deselects


@app.callback(Output("selected", "data", allow_duplicate=True), Input("deselect", "n_clicks"), prevent_initial_call=True)
def deselect(n):
    return None if n else dash.no_update



# ------------------------------------------------------------------------------------------------ squawk alarm
EMERGENCY = {"7500": "UNLAWFUL INTERFERENCE", "7700": "EMERGENCY", "7600": "RADIO FAILURE"}


def active_emergencies(t=None):
    """Aircraft squawking 7500/7700/7600 at the newest snapshot of their feed (or at replay time t). A single
    snapshot can be a dialling glitch (7100 -> 7700 passes 7700) – at least 2 snapshots in a row count."""
    at = (f"TIMESTAMP '{pd.Timestamp(int(t), unit='s'):%Y-%m-%d %H:%M:%S}'" if t else
          f"(SELECT MAX(snapshot_time) FROM {SILVER}.flights f2 WHERE f2.region = f.region)")
    return q(f"""
        SELECT icao24, ANY_VALUE(callsign) AS callsign, squawk, region, MIN(snapshot_time) AS first_seen,
               MAX(snapshot_time) AS last_seen, COUNT(*) AS snapshots, arg_max(altitude_ft, snapshot_time) AS altitude_ft,
               arg_max(latitude, snapshot_time) AS latitude, arg_max(longitude, snapshot_time) AS longitude,
               ANY_VALUE(origin_country) AS origin_country
        FROM {SILVER}.flights f
        WHERE squawk IN ('7500', '7700', '7600')
          AND snapshot_time BETWEEN {at} - INTERVAL 15 MINUTE AND {at}
        GROUP BY icao24, squawk, region
        HAVING COUNT(*) >= 2 AND MAX(snapshot_time) >= {at} - INTERVAL 7 MINUTE   -- world feed: every 6 minutes
        ORDER BY squawk = '7600', last_seen DESC""")


@app.callback(Output("squawk-alarm", "children"), Input("tick", "n_intervals"), Input("rp-state", "data"))
def squawk_alarm(_, replay_state):
    try:
        em = active_emergencies((replay_state or {}).get("t"))
    except Exception:
        return None
    return [html.Button([html.B(f"SQUAWK {r.squawk} · {EMERGENCY[r.squawk]}"),
                         html.Span(f" {(r.callsign or r.icao24).strip()} · {r.origin_country} · "
                                   f"{'' if pd.isna(r.altitude_ft) else f'{r.altitude_ft:,.0f} ft · '}"
                                   f"since {r.first_seen:%H:%M} UTC")],
                        id={"type": "sq-btn", "index": f"{r.icao24}|{r.latitude}|{r.longitude}"}, n_clicks=0,
                        className="sq-btn critical" if r.squawk != "7600" else "sq-btn warning",
                        title="Show this aircraft on the map") for r in em.itertuples()] or None


@app.callback(Output("selected", "data", allow_duplicate=True), Output("map", "viewport"),
              Input({"type": "sq-btn", "index": ALL}, "n_clicks"), prevent_initial_call=True)
def squawk_select(clicks):
    trig = dash.ctx.triggered_id
    if not trig or not any(clicks or []):
        return dash.no_update, dash.no_update
    icao, lat, lon = trig["index"].split("|")            # the position shown in the alarm (live or replay)
    return icao, {"center": [float(lat), float(lon)], "zoom": 7, "transition": "flyTo"}


def kv_rows(pairs):
    return [html.Div([html.Span(k), html.Span(v)], className="kv") for k, v in pairs if v not in (None, "", "nan")]


@app.callback(Output("details-head", "children"), Input("selected", "data"), Input("now-store", "data"))
def details_head(icao24, store):
    """Instant part – no SQL: the map already knows callsign, altitude, speed and heading of every aircraft."""
    if not icao24:
        return [html.H6("Selected aircraft"), html.Div("Click an aircraft on the map to see its details, "
                                                       "its whole track and its route.", className="empty")]
    a = (store or {}).get(icao24)
    if not a:
        try:
            a = SNAP.rows(icao24).get(f"{GOLD}.current_positions")
        except Exception:
            a = None
    if not a:
        return [html.Button("✕", id="deselect", className="close-x"),
                html.Div(icao24, className="callsign"), html.Div("This aircraft has left the view.", className="empty")]
    return [
        html.Button("✕", id="deselect", className="close-x", title="Deselect (or click into the map / Esc)"),
        html.Div(a.get("callsign") or icao24, className="callsign"),
        html.Div(f"{a.get('airline_name') or ''} · registered in {a.get('origin_country') or '?'}", className="sub"),
        html.Div(f"Last position {radar._num(a.get('position_age_s')) // 60} min {radar._num(a.get('position_age_s')) % 60} s "
                 f"old (world data every 6 min) – shown where it should be now", className="sub estimated")
        if a.get("estimated") else None,
        html.Div([html.Div([html.Small(k), html.B(v)], className="stat") for k, v in [
            ("Altitude", f"{a.get('altitude_ft') or 0:,.0f} ft"), ("Speed", f"{a.get('speed_kmh') or 0:,.0f} km/h"),
            ("Heading", f"{a.get('heading') or 0:.0f}°"), ("Phase", a.get("flight_phase"))]], className="stat-grid"),
    ]


# (table, title, fields as (label, column, unit)) – shown when the matching part of the pipeline is running
ENRICH = [
    (f"{GOLD}.route_efficiency", "Route check (our own positions)",
     [("Status", "route_status", ""), ("Why", "route_check", ""), ("Detour", "extra_km", " km")]),
    (f"{GOLD}.aircraft_now", "Aircraft",
     [("Type", "model", ""), ("Typecode", "typecode", ""), ("Registration", "registration", ""), ("Operator", "operator", "")]),
    (f"{GOLD}.flights_with_weather", "Weather at its altitude",
     [("Wind", "wind_kmh", " km/h"), ("From", "wind_from_deg", "°"), ("Outside temp", "outside_temp_c", " °C"),
      ("Wind effect", "wind_effect", "")]),
    (f"{GOLD}.fuel_burn", "CO₂ estimate", [("Fuel", "fuel_kg_per_h", " kg/h"), ("CO₂", "co2_kg_per_h", " kg/h")]),
    (f"{GOLD}.current_firs", "Airspace",
     [("FIR", "fir_name", ""), ("Restricted", "restricted_fir_name", ""), ("Status", "restriction_status", ""),
      ("Applies to it", "restriction_applies", "")]),
    (f"{GOLD}.navaid_passages", "Navaid", [("Passing", "label", ""), ("Distance", "closest_km", " km")]),
    (f"{GOLD}.contrails", "Contrail",
     [("Contrail", "contrail_class", ""), ("Outside temp", "ambient_temp_c", " °C"), ("Forms below", "sac_threshold_c", " °C"),
      ("Humidity over ice", "rh_ice_pct", " %"), ("Why", "reason", "")]),
    (f"{SILVER}.aircraft_photos", None, []),
    (f"{GOLD}.flights_with_routes", None, []),
]
_tables_cache = {"t": 0, "names": set()}


def existing_tables(token=None):
    """Fully qualified names of the tables that exist (cached 5 minutes) – the combined query only uses those."""
    if time.time() - _tables_cache["t"] > 300:
        cat, gold, silver = GOLD.split(".")[0], GOLD.split(".")[1], SILVER.split(".")[1]
        try:
            t = q(f"SELECT table_schema, table_name FROM {cat}.information_schema.tables "
                  f"WHERE table_schema IN ('{gold}', '{silver}')", token)
            _tables_cache.update(t=time.time(), names={f"{cat}.{r.table_schema}.{r.table_name}" for r in t.itertuples()})
        except Exception:
            pass
    return _tables_cache["names"]


@app.callback(Output("details-extra", "children"), Input("selected", "data"))
def details_extra(icao24):
    """Slower part – runs once per click (not on every refresh): route, photo, aircraft, weather, CO₂, airspace, ..."""
    if not icao24:
        return []
    safe = "".join(c for c in icao24 if c.isalnum())
    rows = SNAP.rows(safe)
    cur = rows.get(f"{GOLD}.current_positions")
    route = rows.get(f"{GOLD}.flights_with_routes")
    if route and not route.get("has_route"):
        route = None
    contact = viewer_contact()
    photo, looked_up = parallel(lambda t: None if rows.get(f"{SILVER}.aircraft_photos") else photo_on_demand(safe, contact),
                                lambda t: route_on_demand(cur.get("callsign")) if cur and not route else None)
    blocks = []
    # route: from -> to with progress – the pipeline's table or looked up on demand (adsbdb)
    if not route and looked_up and cur:
        try:
            route = looked_up
            if route.get("origin_lat") is not None:          # progress: flown / (flown + remaining)
                la, lo = float(cur["latitude"]), float(cur["longitude"])
                flown = haversine_km(route["origin_lat"], route["origin_lon"], la, lo)
                left = haversine_km(la, lo, route["destination_lat"], route["destination_lon"])
                route = {**route, "progress_pct": 100 * flown / (flown + left) if flown + left else None}
        except Exception:
            route = None
    airline = (route or {}).get("airline_name")
    if airline and cur and str(cur.get("airline_name") or "").startswith("unknown"):
        blocks.append(html.Div([html.Span("Airline"), html.Span(f"{airline} (from the callsign, adsbdb)")], className="kv"))
    if route and route.get("origin_name"):
        o = route.get("origin_iata") or route.get("origin_icao") or ""
        d = route.get("destination_iata") or route.get("destination_icao") or ""
        prog = route.get("progress_pct")
        check = rows.get(f"{GOLD}.route_efficiency") or {}
        misfit = route_misfit(route, cur) if cur else None
        if not misfit and check.get("route_status") == "contradicted":
            misfit = f"our own positions contradict it ({check.get('route_check') or 'route check'})"
        blocks.append(html.Div([
            html.Div([html.Span(o, className="code"), html.Span("→", className="arrow"), html.Span(d, className="code")],
                     className="route-codes"),
            html.Div([html.Span(route.get("origin_name") or ""), html.Span(route.get("destination_name") or "")],
                     className="route-names"),
            html.Div(html.Div(style={"width": f"{min(100, max(0, prog)):.0f}%"}), className="route-bar")
            if isinstance(prog, (int, float)) and prog == prog and not misfit else None,
            html.Small(f"Probably an old route of this callsign – {misfit}. Not drawn on the map.", className="route-hint warn")
            if misfit else html.Small("scheduled route (adsbdb) – may be wrong for reused callsigns", className="route-hint"),
        ], className="route-box misfit" if misfit else "route-box"))
    # photo (planespotters.net – photographer must be credited)
    p = rows.get(f"{SILVER}.aircraft_photos") or photo
    if p and p.get("thumbnail_url"):
        blocks.append(html.A([html.Img(src=p["thumbnail_url"], className="photo"),
                              html.Small(f"Photo © {p.get('photographer')} – planespotters.net", className="photo-credit")],
                             href=p.get("photo_link"), target="_blank"))
    for table, title, fields in ENRICH:
        r = rows.get(table)
        if not title or not r:
            continue
        pairs = []
        for label, col, unit in fields:
            v = r.get(col)
            if v is not None and v == v and v != "":
                v = f"{v:,.0f}" if isinstance(v, (int, float)) and not isinstance(v, bool) else str(v)
                pairs.append((label, v + unit))
        if pairs:
            blocks += [html.H6(title)] + kv_rows(pairs)
    return blocks


@app.callback(Output("tab-airports", "children"), Input("tick", "n_intervals"), Input("tabs", "active_tab"))
def airports(_, tab):
    if tab != "airports":                                     # only the visible tab is computed (world: ~13,000 aircraft)
        return dash.no_update
    blocks = [section('Runway in use',
                'Which runway each airport is using right now, detected from our own positions of the last 15 minutes: aircraft within 25 km and below 4,000 ft above the airport, flying along the extended centreline of a runway – descending before the threshold (arrival) or climbing beyond it (departure). Aircraft take off and land into the wind, so a wind change flips the runway; the card shows the head- or tailwind from the METAR.')]
    try:                                                  # one card per airport: "Zurich lands on 28 – wind …"
        rw = q(f"SELECT airport_name, movement, summary FROM {GOLD}.runway_in_use WHERE in_use "
               f"ORDER BY airport_icao, movement")
        blocks.append(dbc.Row([dbc.Col(html.Div([html.Small(r.movement), html.Div(r.summary)], className="card-kpi"), md=4,
                                       className="mb-2") for r in rw.itertuples()]) if not rw.empty else hint("No runway movements detected yet."))
    except Exception:
        blocks.append(hint("Runway detection is not running (src_airport_ops)."))
    blocks += [
        section('Aircraft near airports – below 3,000 m within 40 km',
                'Aircraft that are arriving or departing right now: lower than 3,000 m and closer than 40 km to an airport. Movement = climbing (departure) or descending (arrival); distance = straight line to the airport reference point.'),
        optional_table(f"SELECT airport_iata, airport_name, movement, callsign, airline_name, altitude_ft, speed_kmh, "
                       f"ROUND(distance_km, 1) AS distance_km FROM {GOLD}.airport_approaches ORDER BY airport_iata, distance_km",
                       "No approach data yet."),
        section('Forecast (TAF) – what is expected in the next hours',
                'Terminal Aerodrome Forecast: the official weather forecast for an airport, issued every few hours for the next 24–30 h. BECMG = becoming (gradual change), TEMPO = temporarily, PROB30/40 = 30/40 % probability. Flight category: VFR good visibility, MVFR marginal, IFR/LIFR low cloud or fog – then only instrument approaches are possible and capacity drops.'),
        taf_block(),
        section('Busiest navaids – traffic follows the airway structure, not the great circle',
                'Radio beacons (VOR, DME, NDB) that most aircraft passed in the last minutes (closer than a few km). Aircraft fly along airways from beacon to beacon and around restricted areas – one reason why real routes are longer than the great circle (see Detours).'),
        optional_table(f"SELECT navaid_ident, navaid_name, navaid_type, aircraft_passing, aircraft_now, "
                       f"median_altitude_ft, recent_callsigns FROM {GOLD}.navaid_traffic WHERE aircraft_passing > 0 "
                       f"ORDER BY aircraft_passing DESC LIMIT 15", "Navaid passages are not running (src_airport_ops)."),
        section('Airport weather (METAR)',
                'The latest routine weather report of each airport (every 30 or 60 minutes): wind direction and speed, gusts, visibility, cloud ceiling, temperature and air pressure (QNH). raw_text is the original coded report, as pilots read it.'),
        optional_table(f"SELECT station, station_name, flight_category, observed_at, wind_dir_deg, wind_speed_kt, wind_gust_kt, "
                       f"visibility_m, ceiling_ft, temperature_c, qnh_hpa, raw_text FROM {SILVER}.metar ORDER BY station",
                       "METAR is not running (src_airport_ops)."),
    ]
    return blocks


def taf_block(hours=6):
    """TAF periods that are valid now or start within the next hours, per station (largest airports first)."""
    try:
        taf = q(f"SELECT station, station_name, change_indicator, probability, period_from, period_to, wind_dir_deg, "
                f"wind_speed_kt, wind_gust_kt, visibility_m, weather, cloud_layers, ceiling_ft, flight_category, raw_text "
                f"FROM {SILVER}.taf")
    except Exception:
        return hint("TAF is not running (src_airport_ops/08_silver_taf.py).")
    if taf.empty:
        return hint("No TAF for this region.")
    now = pd.Timestamp.now(tz="UTC")
    for c in ("period_from", "period_to"):
        taf[c] = pd.to_datetime(taf[c], utc=True)
    soon = taf[(taf["period_to"] >= now) & (taf["period_from"] <= now + pd.Timedelta(hours=hours))].copy()
    soon["change"] = soon["change_indicator"].where(soon["probability"].isna(),
                                                    "PROB" + soon["probability"].astype("Int64").astype(str))
    soon["wind"] = (soon["wind_dir_deg"].astype("Int64").astype(str) + "° " + soon["wind_speed_kt"].astype("Int64").astype(str)
                    + " kt" + soon["wind_gust_kt"].map(lambda g: f" G{int(g)}" if pd.notna(g) else ""))
    soon = soon.sort_values(["station", "period_from"])
    soon["airport"] = soon["station"] + soon["station_name"].map(lambda n: f" · {n}" if isinstance(n, str) and n else "")
    for c in ("period_from", "period_to"):                    # "Mon 20:00" – periods often run past midnight
        soon[c] = soon[c].dt.strftime("%a %H:%M")
    cols = ["airport", "change", "period_from", "period_to", "wind", "visibility_m", "weather", "cloud_layers",
            "ceiling_ft", "flight_category"]
    return table(soon[cols], page_size=12)


def restricted_section():
    """Who flies through closed / avoided airspace right now? (src_geo/08_gold_fir_traffic.py)"""
    try:
        per_fir = q(f"SELECT * FROM {GOLD}.flights_per_fir WHERE restricted ORDER BY aircraft_inside_restricted DESC")
        rev = q(f"SELECT MIN(days_since_review) AS d, MAX(last_reviewed) AS last FROM {SILVER}.restricted_airspace")
    except Exception:
        return [hint("Restricted airspace is not running (src_geo/06–08).")]
    inside = int(per_fir["aircraft_inside_restricted"].fillna(0).sum()) if len(per_fir) else 0
    applies = int(per_fir["aircraft_restriction_applies"].fillna(0).sum()) if len(per_fir) else 0
    if "d" not in rev:                                    # local mock returns the whole table
        rev = pd.DataFrame([{"d": rev["days_since_review"].min(), "last": rev["last_reviewed"].max()}])
    days = rev["d"].iloc[0]
    return [
        section('Restricted airspace – closed or avoided FIRs',
                "Flight information regions that are closed or should not be overflown, from conflict-zone bulletins (EASA CZIB). 'Applies to it' means the restriction covers this aircraft (e.g. by operator country). The list is curated by hand and has to be reviewed regularly – see 'since the list was reviewed'."),
        dbc.Row([kpi_card(inside, "aircraft inside restricted airspace", alert=True),
                 kpi_card(applies, "… for which the restriction applies", alert=True),
                 kpi_card(int((per_fir["aircraft_inside_restricted"].fillna(0) > 0).sum()) if len(per_fir) else 0,
                          "restricted FIRs with traffic"),
                 kpi_card(f"{int(days)} days" if pd.notna(days) else None, "since the list was reviewed", alert=bool(
                     pd.notna(days) and days > 30))]),
        optional_table(f"SELECT restricted_fir_icao, fir_name, restriction_status, airline_name, origin_country, "
                       f"restriction_applies, aircraft, below_min_fl, suspect_positions, callsigns "
                       f"FROM {GOLD}.restricted_fir_traffic ORDER BY aircraft DESC", "Nobody in restricted airspace."),
        html.Div("The list of restrictions is a curated snapshot (EASA Conflict Zone Information Bulletins, national "
                 "bans) – it must be reviewed regularly. 'Applies' uses the registration country as a stand-in for the "
                 "airline's nationality. Suspect positions = implausible speeds (bad ADS-B data). " + FIR_ATTRIBUTION,
                 className="hint mt-2"),
    ]


@app.callback(Output("tab-alerts", "children"), Input("tick", "n_intervals"), Input("tabs", "active_tab"))
def alerts(_, tab):
    if tab != "alerts":                                     # only the visible tab is computed (world: ~13,000 aircraft)
        return dash.no_update
    counts = {
        "sigmet": optional_count(f"SELECT DISTINCT icao24 FROM {GOLD}.flights_in_sigmet"),
        "squawk": optional_count(f"SELECT icao24 FROM {GOLD}.squawk_alerts WHERE severity <> 'info'"),
        "go": optional_count(f"SELECT icao24 FROM {GOLD}.go_arounds"),
        "hold": optional_count(f"SELECT icao24 FROM {GOLD}.holding_patterns"),
    }
    return [
        dbc.Row([kpi_card(counts["sigmet"], "aircraft in a SIGMET area", alert=True),
                 kpi_card(counts["squawk"], "emergency squawks", alert=True),
                 kpi_card(counts["go"], "go-arounds (last hour)"),
                 kpi_card(counts["hold"], "holding patterns")]),
        section('Aircraft inside a SIGMET – thunderstorm, turbulence, icing',
                'SIGMETs are official warnings of significant weather for aviation (thunderstorms, severe turbulence, icing, volcanic ash). Listed are aircraft whose position lies inside a valid SIGMET area and altitude band right now – usually they are flying around or above it.'),
        optional_table(f"SELECT callsign, airline, altitude_ft, hazard_description, qualifier, fir_name, base_ft, top_ft, valid_to "
                       f"FROM {GOLD}.flights_in_sigmet ORDER BY hazard_description, callsign", "SIGMET check is not running (src_geo)."),
        section('Emergency squawks – 7700 emergency · 7600 radio failure · 7500 unlawful interference',
                'The squawk is the 4-digit transponder code. 7700, 7600 and 7500 are emergency codes; 7000, 2000 and 1200 are only informational (VFR / no code assigned). A single snapshot can be a dialling glitch – real cases last several snapshots and minutes.'),
        optional_table(f"SELECT severity, squawk, meaning, callsign, origin_country, first_seen, last_seen, altitude_ft "
                       f"FROM {GOLD}.squawk_alerts WHERE severity <> 'info' ORDER BY last_seen DESC",
                       "Squawk alerts are not running (src_geo)."),
        section('Go-arounds',
                'An aborted landing in the last hour: the aircraft came down to below 1,500 ft within 10 km of an airport and then climbed more than 800 ft again without touching the ground. Reasons: runway still occupied, unstable approach, wind shear, no runway in sight. Rare – about 1 to 3 in 1,000 approaches.'),
        optional_table(f"SELECT callsign, airport_name, lowest_at, min_height_ft, max_climb_ft, distance_km "
                       f"FROM {GOLD}.go_arounds ORDER BY lowest_at DESC", "Go-around detection is not running (src_airport_ops)."),
        section('Holding patterns',
                'Aircraft parked in a racetrack over a fixed point, waiting for their turn to land (weather, runway change, peak hour). Detected over the last 20 minutes: heading turned through more than a full circle, all positions within 15 km, altitude nearly constant, near a large airport. Many holdings = congestion at the destination.'),
        optional_table(f"SELECT callsign, nearest_airport_name, laps, turn_direction, altitude_band, duration_min, last_seen "
                       f"FROM {GOLD}.holding_patterns ORDER BY last_seen DESC", "Holding detection is not running (src_airport_ops)."),
        *restricted_section()
    ]


@app.callback(Output("tab-aircraft", "children"), Input("tick", "n_intervals"), Input("tabs", "active_tab"))
def aircraft(_, tab):
    if tab != "aircraft":                                     # only the visible tab is computed (world: ~13,000 aircraft)
        return dash.no_update
    try:
        burn = q(f"SELECT * FROM {GOLD}.fuel_burn")
        co2 = q(f"SELECT * FROM {GOLD}.co2_per_minute")
    except TableMissing:
        return hint("The CO₂ estimate is not running (src_aircraft).")
    except Exception as e:
        return hint(f"Query failed: {str(e)[:200]}")
    last = burn[burn["snapshot_time"] == burn["snapshot_time"].max()]
    airborne = last[last["flight_phase"] != "on ground"]
    covered = airborne["co2_kg_per_h"].notna()
    cards = dbc.Row([
        kpi_card(f"{last['co2_kg_per_h'].sum() / 1000:,.0f} t/h", "CO₂ right now (estimate)"),
        kpi_card(f"{last['fuel_kg_per_h'].sum() / 1000:,.1f} t/h", "fuel burn right now"),
        kpi_card(f"{covered.mean() * 100:.0f} %" if len(airborne) else "–", "of airborne aircraft modelled"),
        kpi_card(int(last["typecode"].nunique()), "aircraft types"),
    ])
    per_min = co2.groupby("minute", as_index=False)["co2_kg"].sum()
    fig_min = go.Figure(go.Scatter(x=per_min["minute"], y=per_min["co2_kg"] / 1000, mode="lines", fill="tozeroy",
                                   line={"color": ORANGE, "width": 2}, fillcolor="rgba(249,115,22,0.15)"))
    by_airline = co2.groupby("airline", as_index=False)["co2_kg"].sum().nlargest(10, "co2_kg").sort_values("co2_kg")
    fig_air = go.Figure(go.Bar(x=by_airline["co2_kg"] / 1000, y=by_airline["airline"], orientation="h", marker_color=ACCENT))
    by_type = co2.groupby("typecode", as_index=False)["co2_kg"].sum().nlargest(10, "co2_kg").sort_values("co2_kg")
    fig_type = go.Figure(go.Bar(x=by_type["co2_kg"] / 1000, y=by_type["typecode"], orientation="h", marker_color="#a78bfa"))
    top = (last[last["co2_kg_per_h"].notna()].nlargest(10, "co2_kg_per_h")
           [["callsign", "typecode", "altitude_ft", "speed_kt", "fuel_kg_per_h", "co2_kg_per_h"]])
    return [
        cards,
        dbc.Row([
            dbc.Col(dcc.Graph(figure=dark_layout(fig_min, 280, "CO₂ per minute over the region (t)"),
                              config={"displayModeBar": False}), md=12),
            dbc.Col(dcc.Graph(figure=dark_layout(fig_air, 320, "CO₂ by airline (t, last hour)"),
                              config={"displayModeBar": False}), md=6),
            dbc.Col(dcc.Graph(figure=dark_layout(fig_type, 320, "CO₂ by aircraft type (t, last hour)"),
                              config={"displayModeBar": False}), md=6),
        ]),
        section('Biggest emitters right now',
                'Fuel burn and CO₂ per hour for each aircraft, estimated with the OpenAP model from type, altitude, speed and climb rate (mass and wind are unknown – treat as an order of magnitude). 1 kg of jet fuel gives 3.16 kg CO₂. Big, heavy aircraft and climbing aircraft burn the most.'),
        table(top),
        html.Div("Estimate with OpenAP (TU Delft): mass assumed between empty and max. take-off weight, ground speed "
                 "instead of airspeed; aircraft types without an OpenAP model are not counted.", className="hint mt-2"),
        *contrail_section(),
    ]


def contrail_section():
    """Contrails: the non-CO₂ part of aviation's climate effect that CO₂ calculators leave out."""
    try:
        summ = q(f"SELECT * FROM {GOLD}.contrails_summary ORDER BY band_order")
    except Exception:
        return [hint("Contrail estimate is not running (src_weather/03_gold_contrails.py).")]
    if summ.empty:
        return []
    total = summ[summ["altitude_band"] == "all airborne"]
    bands = summ[(summ["altitude_band"] != "all airborne") & (summ["evaluated_aircraft"].fillna(0) > 0)]
    t = total.iloc[0] if len(total) else None
    cards = dbc.Row([
        kpi_card(int(t["persistent"]) if t is not None else None, "aircraft leaving persistent contrails"),
        kpi_card(f"{t['forming_pct']:.0f} %" if t is not None and pd.notna(t["forming_pct"]) else None,
                 "of evaluated aircraft form contrails"),
        kpi_card(int(t["evaluated_aircraft"]) if t is not None else None, "aircraft evaluated (≥ 20,000 ft, weather known)"),
        kpi_card(f"{t['avg_rh_ice_pct']:.0f} %" if t is not None and pd.notna(t["avg_rh_ice_pct"]) else None,
                 "avg. humidity over ice at flight level"),
    ])
    fig = go.Figure([
        go.Bar(x=bands["altitude_band"], y=bands["persistent"], name="persistent", marker_color="#e879f9"),
        go.Bar(x=bands["altitude_band"], y=bands["forming"] - bands["persistent"], name="short-lived",
               marker_color="#bae6fd"),
        go.Bar(x=bands["altitude_band"], y=bands["evaluated_aircraft"] - bands["forming"], name="none",
               marker_color="#334155"),
    ]).update_layout(barmode="stack")
    return [
        section('Contrails – the climate effect CO₂ calculators leave out',
                "Whether an aircraft leaves a contrail right now, from temperature and humidity at its altitude (Schmidt-Appleman criterion). Persistent contrails in ice-supersaturated air can spread into cirrus clouds and trap heat – estimated to warm the climate about as much as aviation's CO₂, but not included in CO₂ calculators."),
        cards,
        dcc.Graph(figure=dark_layout(fig, 260, "Aircraft by altitude band"), config={"displayModeBar": False}),
        html.Div("Schmidt-Appleman criterion (Schumann 1996) with Open-Meteo temperature and humidity on pressure "
                 "levels; persistent = air supersaturated with respect to ice. Contrail cirrus warms roughly as much "
                 "as aviation's CO₂ (Lee et al. 2021) – but model humidity at cruise level is uncertain.",
                 className="hint mt-2"),
    ]


@app.callback(Output("d-kpis", "children"), Output("d-flight", "options"), Output("d-map", "figure"),
              Output("d-compare", "children"), Output("d-table", "children"),
              Input("tick", "n_intervals"), Input("d-flight", "value"), Input("tabs", "active_tab"),
              Input("d-status", "value"))
def detours(_, picked, tab, status="ok"):
    if tab != "detours":
        raise dash.exceptions.PreventUpdate
    empty = go.Figure().update_layout(template="plotly_dark", paper_bgcolor="#0b1117", plot_bgcolor="#0b1117",
                                      xaxis={"visible": False}, yaxis={"visible": False},
                                      margin={"l": 0, "r": 0, "t": 0, "b": 0})
    try:
        eff = q(f"SELECT * FROM {GOLD}.route_efficiency")
    except TableMissing:
        msg = hint("The detour analysis is not running (src_aircraft/06_gold_route_efficiency.py).")
        return msg, [], empty, None, None
    except Exception as e:
        return hint(f"Query failed: {str(e)[:200]}"), [], empty, None, None
    if eff.empty:
        return (hint("No flights with a known route and at least 20 km of recorded track yet – give the pipeline a "
                     "few minutes (with region = world: the longer it runs, the more of each flight we see)."),
                [], empty, None, None)
    eff = eff.sort_values("extra_km", ascending=False)
    try:                                                  # why? restricted airspace on the great circle (src_geo/08)
        rr = q(f"SELECT icao24, gc_crosses_restricted, restricted_firs_on_gc, restriction_applies, track_avoids, "
               f"avoidance_status, explanation FROM {GOLD}.route_restrictions")
        keep = ["icao24", "gc_crosses_restricted", "restricted_firs_on_gc", "restriction_applies", "track_avoids",
                "avoidance_status", "explanation"]
        eff = eff.merge(rr[[c for c in keep if c in rr]].drop_duplicates("icao24"), on="icao24", how="left")
    except Exception:
        pass
    st = eff["route_status"] if "route_status" in eff else pd.Series("unverified", index=eff.index)
    quality = (f"{int((st == 'confirmed').sum())} confirmed · {int((st == 'unverified').sum())} unverified · "
               f"{int((st == 'contradicted').sum())} contradicted")
    if status == "confirmed":
        eff = eff[st == "confirmed"]
    elif status == "ok":
        eff = eff[st != "contradicted"]
    if eff.empty:
        return [hint(f"Routes: {quality}. No flight matches this filter yet."), [], empty, None, None]
    beyond = eff["beyond_calculators"].fillna(False).astype(bool)
    cards = dbc.Row([
        kpi_card(len(eff), f"flights shown ({quality})"),
        kpi_card(f"{eff['extra_pct'].median():.1f} %", "median detour (lower bound)"),
        kpi_card(f"{eff['extra_co2_kg'].sum() / 1000:,.1f} t", "extra CO₂ vs. great circle"),
        kpi_card(f"{int(beyond.sum())} ({beyond.mean() * 100:.0f} %)", "beyond the calculators' allowance", alert=True),
    ])
    def label(r):
        return (f"{r.callsign} · {r.origin_icao}→{r.destination_icao} · +{r.extra_km:,.0f} km "
                f"({r.extra_pct:.1f} %)")
    options = [{"label": label(r), "value": r.icao24} for r in eff.head(300).itertuples()]
    sel = eff[eff["icao24"] == picked].iloc[0] if picked in set(eff["icao24"]) else eff.iloc[0]

    # map: great circle origin -> destination (grey) vs. the recorded track (blue) of the picked flight
    fig = go.Figure()
    try:                                                  # restricted airspace in the background
        for tr in restricted_traces(alpha_fill=0.10):
            fig.add_trace(tr)
    except Exception:
        pass
    la, lo = great_circle(sel["origin_lat"], sel["origin_lon"], sel["destination_lat"], sel["destination_lon"], n=80)
    fig.add_trace(go.Scattermap(lat=la, lon=lo, mode="lines", name="great circle (calculator)",
                                line={"width": 2, "color": "rgba(139,152,165,0.8)"}, hoverinfo="name"))
    if la:
        fig.add_trace(go.Scattermap(lat=[la[0], la[-1]], lon=[lo[0], lo[-1]], mode="markers+text", showlegend=False,
                                    text=[sel["origin_icao"], sel["destination_icao"]], textposition="top right",
                                    textfont={"color": "#fdba74", "size": 13}, marker={"size": 9, "color": ORANGE},
                                    hoverinfo="text"))
    safe = "".join(c for c in sel["icao24"] if c.isalnum())
    try:
        tr = q(f"SELECT snapshot_time, latitude, longitude FROM {SILVER}.flights WHERE icao24 = '{safe}' "
               f"AND snapshot_time >= (SELECT MAX(snapshot_time) FROM {SILVER}.flights) - INTERVAL 12 HOURS "
               f"ORDER BY snapshot_time")
        fig.add_trace(go.Scattermap(lat=tr["latitude"], lon=tr["longitude"], mode="lines", name="flown (recorded)",
                                    line={"width": 4, "color": ACCENT}, hoverinfo="name"))
    except Exception:
        tr = pd.DataFrame(columns=["latitude", "longitude"])
    pts = pd.DataFrame({"latitude": list(la) + list(tr["latitude"]), "longitude": list(lo) + list(tr["longitude"])})
    center, zoom = map_view(pts, None) if len(pts) else ({"lat": 45, "lon": 20}, 2)
    fig.update_layout(map={"style": MAP_STYLE, "center": center, "zoom": zoom - 0.4}, paper_bgcolor="#0b1117",
                      margin={"l": 0, "r": 0, "t": 0, "b": 0}, uirevision=sel["icao24"],
                      legend={"x": 0.01, "y": 0.99, "bgcolor": "rgba(17,25,34,0.8)", "font": {"color": "#e6edf3"}})

    # comparison: what the calculators assume vs. what we measured
    gcd = sel["gcd_km"]
    bars = pd.DataFrame({"what": ["great circle", "myclimate (+95 km)", f"ICAO (+{sel['icao_extra_km']:.0f} km)",
                                  "measured (lower bound)"],
                         "km": [gcd, gcd + sel["myclimate_extra_km"], gcd + sel["icao_extra_km"], sel["estimated_route_km"]]})
    bar = go.Figure(go.Bar(x=bars["km"], y=bars["what"], orientation="h", text=bars["km"].map("{:,.0f} km".format),
                           textposition="auto", marker_color=[MUTED, "#a78bfa", "#a78bfa", ORANGE]))
    bar.update_xaxes(range=[gcd * 0.9, max(bars["km"]) * 1.03])
    co2 = sel.get("extra_co2_kg")
    compare = [
        html.Div(sel["callsign"], className="callsign"),
        html.Div(" · ".join(str(v) for v in (sel.get("airline_name"), sel.get("typecode") or "type unknown")
                            if pd.notna(v) and v), className="sub"),
        html.Div(f"{sel.get('origin_name') or sel['origin_icao']} → {sel.get('destination_name') or sel['destination_icao']}",
                 className="sub"),
        dcc.Graph(figure=dark_layout(bar, 220), config={"displayModeBar": False}),
        *kv_rows([("Why (airspace)", sel.get("explanation") if pd.notna(sel.get("explanation", None)) else None),
                  ("Route check", f"{sel.get('route_status')} – {sel.get('route_check')}" if "route_status" in sel else None),
                  ("Extra distance", f"+{sel['extra_km']:,.0f} km ({sel['extra_pct']:.1f} %)"),
                  ("Max. away from great circle", f"{sel['max_cross_track_km']:,.0f} km"),
                  ("Recorded track", f"{sel['observed_km']:,.0f} km, {int(sel['positions'])} positions"),
                  ("Fuel per km (OpenAP)", f"{sel['fuel_kg_per_km']:.1f} kg" if pd.notna(sel.get("fuel_kg_per_km")) else None),
                  ("Extra fuel", f"{sel['extra_fuel_kg']:,.0f} kg" if pd.notna(sel.get("extra_fuel_kg")) else None),
                  ("Extra CO₂", f"{co2:,.0f} kg" if pd.notna(co2) else "no OpenAP model for this type")]),
    ]
    cols = ["callsign", "airline_name", "typecode", "origin_icao", "destination_icao", "gcd_km", "estimated_route_km",
            "extra_km", "extra_pct", "max_cross_track_km", "extra_co2_kg", "beyond_calculators", "route_status",
            "gc_crosses_restricted", "restricted_firs_on_gc", "route_check"]
    return cards, options, fig, compare, table(eff[[c for c in cols if c in eff.columns]], page_size=12)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8050"))
    print(f"Live Radar: http://127.0.0.1:{port}  (Ctrl+C to stop)")
    app.run(host="127.0.0.1", port=port, debug=False)
