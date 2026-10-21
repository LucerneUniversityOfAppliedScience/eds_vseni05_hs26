// Escape deselects the aircraft (clicks are handled by the Leaflet map itself: a click on an aircraft symbol selects
// it, a click into the map deselects).
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape" && window.dash_clientside && dash_clientside.set_props) {
    dash_clientside.set_props("selected", {data: null});
  }
});

// Leaflet measures its container once at start – before the stylesheet has sized the radar page – and then only
// draws tiles for the upper part. A resize event makes it measure again (also after switching back to the tab).
function nudgeLeaflet() { window.dispatchEvent(new Event("resize")); }
[300, 1000, 2500].forEach((ms) => setTimeout(nudgeLeaflet, ms));
document.addEventListener("click", (e) => { if (e.target.closest(".nav-link")) setTimeout(nudgeLeaflet, 200); });
