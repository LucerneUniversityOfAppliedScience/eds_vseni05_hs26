"""Local backend of the Live Radar app: flights.duckdb instead of a Databricks SQL warehouse.

The app process owns the database (DuckDB allows only ONE writing process) and runs two background threads:
- collector: one OpenSky call every INTERVAL seconds -> bronze -> silver (flightlab.py, notebooks 01–05)
- enrichment: the analyses (enrichment/jobs.py) – weather, METAR/TAF, routes, CO2, detours, ... on their own rhythm
Every app query gets its own cursor, so reading and writing in the same process work side by side.

- r2 mirror (optional): the class bucket's files of R2_REGIONS (e.g. "world": 13,000 aircraft every 6 minutes) –
  first the last BACKFILL_HOURS (at least 2), then every 5 minutes the new ones. No OpenSky credits needed.

Settings (environment or .env): REGION (switzerland, called live), INTERVAL (35), BACKFILL_HOURS (0),
R2_REGIONS (empty | world), AUTOSTART (1).
"""
import os
import re
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import flightlab as fl  # noqa: E402

for _k, _v in fl.load_env().items():                      # PHOTO_CONTACT, OPENSKY_*, R2_* from .env
    os.environ.setdefault(_k, _v)
if (ROOT / ".env").exists():
    for _line in (ROOT / ".env").read_text().splitlines():
        if "=" in _line and not _line.lstrip().startswith("#"):
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

R2_REGIONS = [r.strip() for r in os.getenv("R2_REGIONS", "").split(",") if r.strip()]
WORLD = os.getenv("REGION", "switzerland") == "world" or "world" in R2_REGIONS
if WORLD:                                     # the analyses: world windows, heavy ones only over Switzerland
    os.environ.setdefault("FLIGHTS_REGION", "world")
    os.environ.setdefault("FLIGHTS_FOCUS_REGION", "switzerland")

from enrichment import jobs  # noqa: E402

CATALOG = fl.DB_PATH.stem                                  # DuckDB names the catalog after the file: "flights"
SILVER, GOLD, BRONZE = f"{CATALOG}.silver", f"{CATALOG}.gold", f"{CATALOG}.bronze"
REGION = os.getenv("REGION", "switzerland")
INTERVAL = float(os.getenv("INTERVAL", "35"))
BACKFILL_HOURS = float(os.getenv("BACKFILL_HOURS", "0"))


class TableMissing(Exception):
    """The table does not exist (yet) – e.g. an analysis that has not run."""


def translate(sql):
    """The few Databricks SQL functions the app uses -> DuckDB."""
    sql = re.sub(r"\bTIMESTAMP_SECONDS\(", "to_timestamp(", sql, flags=re.I)
    sql = re.sub(r"\bUNIX_TIMESTAMP\(", "epoch(", sql, flags=re.I)
    sql = re.sub(r"\bcurrent_timestamp\(\)", "current_timestamp", sql, flags=re.I)
    sql = re.sub(r"\b\w+\.information_schema\.", "information_schema.", sql)
    return sql


def utc_cursor(con):
    """A cursor is a new connection: it does NOT inherit SET TimeZone – without this, timestamps shift by 1–2 h."""
    cur = con.cursor()
    cur.execute("SET TimeZone = 'UTC'")
    return cur


class LocalBackend:
    local = True
    world = WORLD

    def __init__(self):
        self.con = fl.connect()
        fl.load_reference(self.con)
        fl.build_silver(self.con)
        fl.build_gold(self.con, window_minutes=8 if WORLD else 2)     # world files arrive every 6 minutes
        self.interval = INTERVAL                         # seconds between OpenSky calls – changeable in the app
        self.credits = None                              # last X-Rate-Limit-Remaining
        self._stop = threading.Event()
        self._threads = []
        self.last_error = None
        if os.getenv("AUTOSTART", "1") == "1":
            self.start_pipeline()

    # ------------------------------------------------------------- queries
    def query(self, statement, user_token=None):
        import duckdb
        cur = utc_cursor(self.con)
        try:
            return cur.execute(translate(statement)).df()
        except duckdb.CatalogException as e:
            raise TableMissing(str(e).split("\n")[0]) from e
        finally:
            cur.close()

    # ------------------------------------------------------------- background work ("pipeline")
    def _collector(self):
        cur = utc_cursor(self.con)
        env = fl.load_env()
        token = None
        if env.get("OPENSKY_CLIENT_ID") and env.get("OPENSKY_CLIENT_SECRET"):
            try:
                token, token_at = fl.get_token(env["OPENSKY_CLIENT_ID"], env["OPENSKY_CLIENT_SECRET"]), time.time()
            except Exception as e:
                self.last_error = f"OpenSky login failed: {e}"
        if BACKFILL_HOURS and env.get("R2_ACCESS_KEY_ID"):
            since = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=BACKFILL_HOURS)
            fl.backfill(cur, REGION, since=since, env=env, log=lambda m: print("[backfill]", m))
            fl.build_silver(cur)
        backoff = self.interval
        while not self._stop.is_set():
            t0 = time.time()
            try:
                if token and time.time() - token_at > 1500:     # tokens expire after ~30 minutes
                    token, token_at = fl.get_token(env["OPENSKY_CLIENT_ID"], env["OPENSKY_CLIENT_SECRET"]), time.time()
                data, credits = fl.fetch_states(fl.REGIONS.get(REGION), token)
                fl.store_snapshot(cur, data, REGION, "api", credits)
                fl.build_silver(cur)
                backoff, self.last_error, self.credits = self.interval, None, credits
                if credits is not None and credits < 20:
                    self.last_error = f"only {credits} OpenSky credits left – collector paused"
                    self._stop.wait(1800)
                    continue
            except Exception as e:                       # 429, network: wait, never crash the app
                wait = fl.retry_after(e)                 # 429: as long as OpenSky says
                if wait is None:
                    backoff = min(backoff * 2, 600)      # otherwise longer each time
                    wait = backoff
                self.last_error = f"{type(e).__name__}: {str(e)[:120]}"
                print("[collector]", self.last_error, f"– retry in {wait:.0f} s")
                self._stop.wait(wait)
                continue
            self._stop.wait(max(0.0, self.interval - (time.time() - t0)))

    def _r2_mirror(self):
        """New files of R2_REGIONS from the class bucket, every 5 minutes (the same idempotent backfill)."""
        cur = utc_cursor(self.con)
        env = fl.load_env()
        hours = max(BACKFILL_HOURS, 2.0)
        while not self._stop.is_set():
            for region in R2_REGIONS:
                try:
                    since = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=hours)
                    _, new = fl.backfill(cur, region, since=since, env=env, log=lambda m: None)
                    if new:
                        print(f"[r2 mirror] {region}: {new} new snapshots", flush=True)
                        fl.build_silver(cur)
                except Exception as e:
                    print("[r2 mirror]", region, type(e).__name__, str(e)[:150], flush=True)
            hours = 0.5                                   # after the first run: only the last half hour
            self._stop.wait(300)

    def _enrichment(self):
        cur = utc_cursor(self.con)
        while not self._stop.is_set():
            try:
                jobs.run_due(cur, log=lambda m: print("[enrichment]", m.splitlines()[0]))
            except Exception as e:
                print("[enrichment]", type(e).__name__, e)
            self._stop.wait(20)

    def pipeline_status(self):
        running = any(t.is_alive() for t in self._threads)
        return {"state": "running" if running else "stopped",
                "update": "RUNNING" if running else (self.last_error or None)}

    def start_pipeline(self):
        if any(t.is_alive() for t in self._threads):
            return
        self._stop.clear()
        self._threads = [threading.Thread(target=self._collector, daemon=True, name="collector"),
                         threading.Thread(target=self._enrichment, daemon=True, name="enrichment")]
        if R2_REGIONS and fl.load_env().get("R2_ACCESS_KEY_ID"):
            self._threads.append(threading.Thread(target=self._r2_mirror, daemon=True, name="r2 mirror"))
        for t in self._threads:
            t.start()

    def stop_pipeline(self):
        self._stop.set()


MockBackend = LocalBackend          # the app asks isinstance(backend, MockBackend) for "read every table directly"


def get_backend():
    return LocalBackend()
