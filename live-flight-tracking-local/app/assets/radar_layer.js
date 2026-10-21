// Aircraft symbols for the GeoJSON layer of the map (app.py: dl.GeoJSON(pointToLayer={"variable": "radarJs.pointToLayer"})).
// Python sends a few short properties per aircraft; the silhouettes are built here and cached, so a refresh with
// thousands of aircraft stays light. Shapes and sizes are the same as in radar.py.
window.radarJs = (function () {
  const SHAPES = {
    "jet": "M16 2 L17.6 11 L29 17.5 L29 19.5 L17.6 16.5 L17 25 L20.5 27.5 L20.5 29 L16 28 L11.5 29 L11.5 27.5 L15 25 L14.4 16.5 L3 19.5 L3 17.5 L14.4 11 Z",
    "heavy": "M16 1 L17.8 10 L31 18 L31 20.5 L17.8 16.8 L17.3 25.5 L21.5 28.5 L21.5 30 L16 29 L10.5 30 L10.5 28.5 L14.7 25.5 L14.2 16.8 L1 20.5 L1 18 L14.2 10 Z",
    "light": "M16 4 L17.2 10 L29 11.5 L29 14 L17.2 14.5 L16.8 23 L21 24.5 L21 26.5 L16 26 L11 26.5 L11 24.5 L15.2 23 L14.8 14.5 L3 14 L3 11.5 L14.8 10 Z",
    "heli": "M16 7 a5 6 0 1 1 0.01 0 Z M15.2 18 L16.8 18 L16.8 28 L19.5 28 L19.5 29.5 L12.5 29.5 L12.5 28 L15.2 28 Z M4 12.2 L28 12.2 L28 13.4 L4 13.4 Z",
    "glider": "M16 6 L16.8 12 L31 13 L31 14.2 L16.8 14.8 L16.5 25 L19 26 L19 27 L13 27 L13 26 L15.5 25 L15.2 14.8 L1 14.2 L1 13 L15.2 12 Z"
};
  const SIZES = {"heavy": 30, "jet": 24, "light": 18, "heli": 20, "glider": 20};
  const cache = {};
  let canvas = null;                                  // one shared canvas: thousands of dots stay fast
  function icon(shape, color, heading, estimated, small) {
    const key = [shape, color, heading, estimated, small].join("|");
    if (!cache[key]) {
      const size = (SIZES[shape] || 24) - (small ? 6 : 0);
      const svg = "<svg xmlns='http://www.w3.org/2000/svg' width='" + size + "' height='" + size + "' viewBox='0 0 32 32'>" +
        "<g transform='rotate(" + heading + " 16 16)' opacity='" + (estimated ? 0.5 : 1) + "'>" +
        "<path d='" + (SHAPES[shape] || SHAPES.jet) + "' fill='" + color + "' stroke='#0b1117' stroke-width='1.4' " +
        "stroke-linejoin='round'/></g></svg>";
      cache[key] = L.icon({iconUrl: "data:image/svg+xml;utf8," + encodeURIComponent(svg),
                           iconSize: [size, size], iconAnchor: [size / 2, size / 2]});
    }
    return cache[key];
  }
  return {
    pointToLayer: function (feature, latlng, context) {
      const p = feature.properties || {};
      if (!p.s) {                                     // zoomed out: a small dot in the altitude colour
        canvas = canvas || L.canvas({padding: 0.3, tolerance: 6});
        return L.circleMarker(latlng, {renderer: canvas, radius: 2.6, weight: 0, fillColor: p.c, fillOpacity: 0.9,
                                         bubblingMouseEvents: false});
      }
      const small = !!p.k;
      return L.marker(latlng, {icon: icon(p.s, p.c, p.h, p.e, small), keyboard: false})
        .bindTooltip(p.tip || "", {direction: "top", offset: [0, -8]});
    }
  };
})();
