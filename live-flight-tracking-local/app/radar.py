"""Leaflet building blocks of the radar map: aircraft symbols (rotated by heading, coloured by altitude), lines,
areas and point layers. Pure functions – the callbacks in app.py decide WHAT to draw, this module HOW."""
import json
import math
from urllib.parse import quote

import dash_leaflet as dl
from dash import html

# base map: Esri dark grey canvas (CARTO raster tiles now need an API key); labels as a separate layer on top
BASE_TILES = "https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}"
LABEL_TILES = ("https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Reference/MapServer/tile/"
               "{z}/{y}/{x}")
ATTRIBUTION = "Tiles © Esri — Esri, HERE, Garmin, © OpenStreetMap contributors"

ACCENT, ORANGE, MUTED = "#38bdf8", "#f97316", "#8b98a5"
STOPS = [(0, (34, 211, 238)), (10000, (52, 211, 153)), (20000, (250, 204, 21)), (30000, (251, 146, 60)),
         (40000, (244, 63, 94))]

# silhouettes in a 32x32 box, nose up (heading 0 = north); chosen by the OpenSky aircraft category
SHAPES = {
    "jet": "M16 2 L17.6 11 L29 17.5 L29 19.5 L17.6 16.5 L17 25 L20.5 27.5 L20.5 29 L16 28 L11.5 29 L11.5 27.5 L15 25 "
           "L14.4 16.5 L3 19.5 L3 17.5 L14.4 11 Z",
    "heavy": "M16 1 L17.8 10 L31 18 L31 20.5 L17.8 16.8 L17.3 25.5 L21.5 28.5 L21.5 30 L16 29 L10.5 30 L10.5 28.5 "
             "L14.7 25.5 L14.2 16.8 L1 20.5 L1 18 L14.2 10 Z",
    "light": "M16 4 L17.2 10 L29 11.5 L29 14 L17.2 14.5 L16.8 23 L21 24.5 L21 26.5 L16 26 L11 26.5 L11 24.5 L15.2 23 "
             "L14.8 14.5 L3 14 L3 11.5 L14.8 10 Z",
    "heli": "M16 7 a5 6 0 1 1 0.01 0 Z M15.2 18 L16.8 18 L16.8 28 L19.5 28 L19.5 29.5 L12.5 29.5 L12.5 28 L15.2 28 Z "
            "M4 12.2 L28 12.2 L28 13.4 L4 13.4 Z",
    "glider": "M16 6 L16.8 12 L31 13 L31 14.2 L16.8 14.8 L16.5 25 L19 26 L19 27 L13 27 L13 26 L15.5 25 L15.2 14.8 "
              "L1 14.2 L1 13 L15.2 12 Z",
}
SIZES = {"heavy": 30, "jet": 24, "light": 18, "heli": 20, "glider": 20}


def altitude_color(ft):
    """Same scale as the legend: cyan (ground) -> green -> yellow -> orange -> red (40,000 ft)."""
    try:
        ft = max(0.0, min(40000.0, float(ft)))
    except (TypeError, ValueError):
        ft = 0.0
    for (a0, c0), (a1, c1) in zip(STOPS, STOPS[1:]):
        if ft <= a1:
            f = (ft - a0) / (a1 - a0)
            return "#%02x%02x%02x" % tuple(round(x + (y - x) * f) for x, y in zip(c0, c1))
    return "#f43f5e"


def shape_for(category):
    """OpenSky category (extended=1): 2 light, 3 small, 4 large, 5 high vortex, 6 heavy, 8 rotorcraft, 9 glider."""
    try:
        c = int(category)
    except (TypeError, ValueError):
        c = 0
    return {8: "heli", 9: "glider", 2: "light", 5: "heavy", 6: "heavy"}.get(c, "jet")


def aircraft_icon(heading, altitude_ft, category=None, on_ground=False, selected=False, small=False, estimated=False):
    shape = shape_for(category)
    size = SIZES[shape] - (6 if small else 0) + (8 if selected else 0)
    fill = "#94a3b8" if on_ground else altitude_color(altitude_ft)
    stroke, width = ("#ffffff", 2.2) if selected else ("#0b1117", 1.4)
    try:
        rot = float(heading or 0)
    except (TypeError, ValueError):
        rot = 0.0
    svg = (f"<svg xmlns='http://www.w3.org/2000/svg' width='{size}' height='{size}' viewBox='0 0 32 32'>"
           f"<g transform='rotate({rot:.0f} 16 16)' opacity='{0.5 if estimated else 1}'>"
           f"<path d='{SHAPES[shape]}' fill='{fill}' stroke='{stroke}' "
           f"stroke-width='{width}' stroke-linejoin='round'/></g></svg>")
    return {"iconUrl": "data:image/svg+xml;utf8," + quote(svg), "iconSize": [size, size],
            "iconAnchor": [size / 2, size / 2]}


def _num(v):
    """Missing values from the warehouse arrive as None or NaN (and NaN is truthy) -> 0."""
    try:
        return 0 if v is None or v != v else int(float(v))
    except (TypeError, ValueError):
        return 0


def _text(v):
    return v.strip() if isinstance(v, str) else ""


def aircraft_markers(df, selected=None, small=False):
    """One clickable marker per aircraft (pattern-matching id -> the click selects it)."""
    out = []
    df = df.dropna(subset=["latitude", "longitude"])
    for r in df.itertuples():
        sel = r.icao24 == selected
        tip = (f"{_text(r.callsign) or r.icao24} · {_text(r.airline_name)} · {_num(r.altitude_ft):,} ft · "
               f"{_num(r.speed_kmh)} km/h · {_text(r.flight_phase)}")
        est = bool(getattr(r, "estimated", False))
        if est:
            tip += f" · position {_num(r.position_age_s) // 60} min old, estimated"
        out.append(dl.Marker(
            id={"type": "ac", "index": r.icao24}, position=[r.latitude, r.longitude],
            icon=aircraft_icon(_num(r.heading), _num(r.altitude_ft), getattr(r, "category", None),
                               r.on_ground is True or r.on_ground == 1, sel, small, est),
            zIndexOffset=1000 if sel else 0, children=dl.Tooltip(tip, direction="top", offset=[0, -8])))
    return out


def aircraft_geojson(df, small=False, dots=False):
    """All aircraft as ONE GeoJSON FeatureCollection – a few short properties per aircraft; the browser
    (assets/radar_layer.js) turns them into rotated silhouettes. ~150 bytes per aircraft instead of ~1 KB per marker."""
    feats = []
    df = df.dropna(subset=["latitude", "longitude"])
    if dots:                                              # zoomed out: id, colour, position only (~60 bytes each)
        for r in df.itertuples():
            ground = r.on_ground is True or r.on_ground == 1
            feats.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": [round(float(r.longitude), 2),
                                                                                         round(float(r.latitude), 2)]},
                          "properties": {"id": r.icao24, "c": "#94a3b8" if ground else altitude_color(_num(r.altitude_ft))}})
        return {"type": "FeatureCollection", "features": feats}
    for r in df.itertuples():
        est = bool(getattr(r, "estimated", False))
        tip = (f"{_text(r.callsign) or r.icao24} · {_text(r.airline_name)} · {_num(r.altitude_ft):,} ft · "
               f"{_num(r.speed_kmh)} km/h · {_text(r.flight_phase)}")
        if est:
            tip += f" · position {_num(r.position_age_s) // 60} min old, estimated"
        ground = r.on_ground is True or r.on_ground == 1
        feats.append({"type": "Feature",
                      "geometry": {"type": "Point", "coordinates": [round(float(r.longitude), 4), round(float(r.latitude), 4)]},
                      "properties": {"id": r.icao24, "h": _num(r.heading) // 5 * 5,
                                     "c": "#94a3b8" if ground else altitude_color(_num(r.altitude_ft)),
                                     "s": shape_for(getattr(r, "category", None)), "e": int(est), "k": int(small),
                                     "tip": tip}})
    return {"type": "FeatureCollection", "features": feats, "small": small}


def multi_line(groups, color, weight=1.4, opacity=0.5, dash=None):
    """Many lines as ONE Leaflet layer (fast): groups = list of [[lat, lon], ...]."""
    groups = [g for g in groups if len(g) >= 2]
    if not groups:
        return []
    return [dl.Polyline(positions=groups, color=color, weight=weight, opacity=opacity, dashArray=dash,
                        interactive=False)]


def split_jumps(points, max_km=200):
    """Split a track where two consecutive points are implausibly far apart (data gap, reused icao24)."""
    out, cur = [], []
    for p in points:
        if cur:
            la1, lo1, la2, lo2 = map(math.radians, (cur[-1][0], cur[-1][1], p[0], p[1]))
            d = 2 * 6371 * math.asin(math.sqrt(math.sin((la2 - la1) / 2) ** 2
                                               + math.cos(la1) * math.cos(la2) * math.sin((lo2 - lo1) / 2) ** 2))
            if d > max_km:
                out.append(cur)
                cur = []
        cur.append(p)
    return out + [cur]


def great_circle(lat1, lon1, lat2, lon2, n=40):
    """Points on the shortest path over the globe – a curve on the flat map."""
    p1, l1, p2, l2 = map(math.radians, (lat1, lon1, lat2, lon2))
    d = 2 * math.asin(math.sqrt(math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2)
                                * math.sin((l2 - l1) / 2) ** 2))
    if d < 1e-6:
        return [[lat1, lon1], [lat2, lon2]]
    pts = []
    for i in range(n + 1):
        f = i / n
        a, b = math.sin((1 - f) * d) / math.sin(d), math.sin(f * d) / math.sin(d)
        x = a * math.cos(p1) * math.cos(l1) + b * math.cos(p2) * math.cos(l2)
        y = a * math.cos(p1) * math.sin(l1) + b * math.cos(p2) * math.sin(l2)
        z = a * math.sin(p1) + b * math.sin(p2)
        pts.append([math.degrees(math.atan2(z, math.sqrt(x * x + y * y))), math.degrees(math.atan2(y, x))])
    return pts


def route_layers(route, label_airports=False):
    """origin -> aircraft -> destination: grey = flown, orange = still ahead, airports as dots (labelled if asked)."""
    out = multi_line([great_circle(route["origin_lat"], route["origin_lon"], route["latitude"], route["longitude"])],
                     "#8b98a5", 2, 0.7)
    out += multi_line([great_circle(route["latitude"], route["longitude"], route["destination_lat"],
                                    route["destination_lon"])], ORANGE, 2.4, 0.9, "6 6")
    for key in ("origin", "destination"):
        code = route.get(f"{key}_iata") or route.get(f"{key}_icao") or ""
        children = [dl.Tooltip(f"{code} {route.get(f'{key}_name') or ''}", permanent=label_airports,
                               direction="right", className="airport-label")]
        out.append(dl.CircleMarker(center=[route[f"{key}_lat"], route[f"{key}_lon"]], radius=6, color=ORANGE,
                                   fillColor=ORANGE, fillOpacity=1, weight=2, children=children))
    return out


def geojson_rings(text):
    """GeoJSON Polygon / MultiPolygon text -> list of outer rings as [[lat, lon], ...]."""
    try:
        g = json.loads(text) if isinstance(text, str) else text
    except Exception:
        return []
    polys = ([g["coordinates"]] if g.get("type") == "Polygon" else
             g.get("coordinates", []) if g.get("type") == "MultiPolygon" else [])
    return [[[p[1], p[0]] for p in poly[0]] for poly in polys if poly]


def polygon(ring, color, fill_opacity, tooltip):
    return dl.Polygon(positions=ring, color=color, weight=1, opacity=0.7, fillColor=color,
                      fillOpacity=fill_opacity, children=dl.Tooltip(tooltip, sticky=True))


def circle(lat, lon, radius, color, tooltip=None, opacity=0.9, permanent=False):
    kids = [dl.Tooltip(tooltip, permanent=permanent, direction="bottom", className="navaid-label")] if tooltip else []
    return dl.CircleMarker(center=[lat, lon], radius=radius, color=color, fillColor=color, fillOpacity=opacity,
                           weight=0, children=kids)


def map_note(text):
    return html.Div(text, className="map-note") if text else None
