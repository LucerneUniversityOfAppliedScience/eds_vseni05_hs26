# Ported from the Databricks recipe (src_aircraft/05_silver_aircraft_photos.py) – the pure pandas logic, without the Spark/pipeline part.
# Settings come from enrichment/config.py (CONF) instead of the pipeline configuration.
# Silver: a photo of the aircraft that is landing or taking off right now – from planespotters.net, by transponder
# code: https://api.planespotters.net/pub/photos/hex/<icao24>. Only for a small subset: aircraft currently near an
# airport (gold.airport_approaches), at most aircraft.max_photo_lookups (default 30) per refresh, refreshed every
# aircraft.photos_interval (default 30 minutes).
#
# planespotters.net Photo API Terms of Use (https://www.planespotters.net/photo/api, read 2026-10-18) and how we
# follow them:
#   * server clients must send a unique, descriptive User-Agent WITH A CONTACT URL OR EMAIL ("MyFlightTracker/1.2
#     (+https://example.com/contact)"); library defaults are rejected (HTTP 403).
#     -> pipeline configuration aircraft.contact (URL or e-mail); empty = the e-mail of the Databricks account
#        running the pipeline (current_user()). Without any contact NO calls are made.
#   * every photo must show the photographer's name as visible text next to the image, and the thumbnail must link
#     to the photo page (the "link" field, unchanged, no rel="nofollow").
#     -> this table stores photographer and photo_link; the dashboard/app MUST show both with the thumbnail.
#   * API JSON responses may be cached for up to 24 hours -> the table is rebuilt every 30 minutes, looked_up_at
#     tells the age. We store only the URLs, never the image: images must be loaded by the browser that shows them,
#     straight from the thumbnail URL (unchanged, no proxy, no re-hosting).
#   * photos must be freely available to all viewers of the app (no paid/premium feature), no AI training,
#     no re-exposing the data through an own API, feed, bulk export or dataset -> keep this table internal.
#   * reasonable volume only (sustained/bursty traffic may be blocked) -> 30 lookups per 30 minutes, 1 per second.
#
# Network: api.planespotters.net is NOT verified on the Free Edition allow-list. On a connection error the lookups
# stop at once, the table is created EMPTY (with schema) and a message is printed – the pipeline goes on.
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from enrichment.config import CONF

CATALOG = CONF.get("flights.catalog", "opensky")
SILVER = CONF.get("flights.silver_schema", "silver")
GOLD = CONF.get("flights.gold_schema", "gold")
MAX_LOOKUPS = int(CONF.get("aircraft.max_photo_lookups", "30"))
CONTACT = CONF.get("aircraft.contact", "").strip()
# planespotters asks for a contact in the User-Agent: PHOTO_CONTACT in .env (your e-mail)

URL = "https://api.planespotters.net/pub/photos/hex/{}"
PAUSE_S = 1.0
SCHEMA = ("icao24 STRING, callsign STRING, airport_icao STRING, photo_id STRING, thumbnail_url STRING, "
          "thumbnail_large_url STRING, photo_link STRING, photographer STRING, attribution STRING, "
          "looked_up_at TIMESTAMP")


def user_agent(contact):
    return f"databricks-cookbook-flight-tracking/1.0 (teaching demo; +{contact})"


def photo_candidates(pdf_approaches, max_n=MAX_LOOKUPS):
    """Aircraft near an airport, closest first, at most max_n."""
    df = pdf_approaches.sort_values("distance_km").drop_duplicates("icao24")
    return df[["icao24", "callsign", "airport_icao"]].head(max_n)


def parse_photo(payload):
    """planespotters JSON -> dict with the first photo, or None if there is none."""
    photos = (payload or {}).get("photos") or []
    if not photos:
        return None
    p = photos[0]
    return {"photo_id": p.get("id"), "thumbnail_url": (p.get("thumbnail") or {}).get("src"),
            "thumbnail_large_url": (p.get("thumbnail_large") or {}).get("src"), "photo_link": p.get("link"),
            "photographer": p.get("photographer"),
            "attribution": f"Photo: {p.get('photographer')} / planespotters.net"}


def lookup_photos(candidates, contact, session=None, pause_s=PAUSE_S):
    """One API call per aircraft. No contact -> no calls (terms). Connection error / 403 / 429 -> stop."""
    cols = [c.split()[0] for c in SCHEMA.split(", ")]
    if not contact:
        print("aircraft_photos: pipeline configuration aircraft.contact is empty – planespotters.net requires a "
              "User-Agent with a contact URL or e-mail, so no photos are looked up")
        return pd.DataFrame(columns=cols)
    session = session or requests.Session()
    session.headers["User-Agent"] = user_agent(contact)
    rows, now = [], datetime.now(timezone.utc)
    for i, c in enumerate(candidates.itertuples(index=False)):
        try:
            r = session.get(URL.format(c.icao24.upper()), timeout=10)
        except requests.RequestException as e:
            print(f"aircraft_photos: planespotters.net not reachable ({type(e).__name__}) after {i} lookups – stopping")
            break
        if r.status_code in (403, 429):
            print(f"aircraft_photos: planespotters.net answered HTTP {r.status_code} after {i} lookups – stopping")
            break
        if r.ok:
            photo = parse_photo(r.json())
            if photo:
                rows.append({"icao24": c.icao24, "callsign": c.callsign, "airport_icao": c.airport_icao,
                             **photo, "looked_up_at": now})
        time.sleep(pause_s)
    print(f"aircraft_photos: {len(candidates)} aircraft near airports -> {len(rows)} photos")
    return pd.DataFrame(rows, columns=cols)
