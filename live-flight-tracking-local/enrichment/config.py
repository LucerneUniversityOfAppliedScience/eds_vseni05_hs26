"""Settings of the analyses – the local replacement for the Databricks pipeline configuration. Every module asks
CONF.get(key, default); only keys that differ from the defaults need to be here. Override with environment variables:
FLIGHTS_REGION (switzerland | world ...) and FLIGHTS_FOCUS_REGION (where weather, METAR, go-arounds ... run)."""
import os
from pathlib import Path

_env = {}
_dotenv = Path(__file__).resolve().parent.parent / ".env"
if _dotenv.exists():
    for _line in _dotenv.read_text().splitlines():
        if "=" in _line and not _line.lstrip().startswith("#"):
            _k, _v = _line.split("=", 1)
            _env[_k.strip()] = _v.strip().strip('"').strip("'")

CONF = {
    "flights.region": os.getenv("FLIGHTS_REGION", "switzerland"),
    "flights.focus_region": os.getenv("FLIGHTS_FOCUS_REGION", ""),
    "aircraft.contact": os.getenv("PHOTO_CONTACT", _env.get("PHOTO_CONTACT", "")),
}
