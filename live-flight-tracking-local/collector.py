"""Collector for flights.duckdb: backfill from the class bucket (optional), then poll OpenSky live – with the
finished functions of notebooks 01–06 (flightlab.py). Runs until Ctrl+C (or --minutes).

    uv run collector.py                                   # Switzerland, every 35 s, forever
    uv run collector.py --backfill-hours 48               # first the last 48 h from R2, then live
    uv run collector.py --backfill-hours 48 --no-live     # only the backfill
    uv run collector.py --region world --interval 360     # the world (4 credits per call: account needed)

The map app (app/app.py) has this collector built in – run either the app or this script, not both: DuckDB allows
only ONE process that writes to flights.duckdb."""
import argparse
from datetime import datetime, timedelta, timezone

import flightlab as fl


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--region", default="switzerland", choices=sorted(fl.REGIONS))
    ap.add_argument("--interval", type=float, default=35, help="seconds between calls (default 35)")
    ap.add_argument("--minutes", type=float, default=0, help="stop after N minutes (0 = until Ctrl+C)")
    ap.add_argument("--min-credits", type=int, default=20, help="stop when fewer credits are left")
    ap.add_argument("--backfill-hours", type=float, default=0, help="load the last N hours from the R2 bucket first")
    ap.add_argument("--no-live", action="store_true", help="only backfill, do not call the API")
    args = ap.parse_args()

    con = fl.connect()
    fl.load_reference(con)
    fl.build_silver(con)                         # creates silver.flights – the gold views read it
    fl.build_gold(con)
    if args.backfill_hours:
        since = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=args.backfill_hours)
        fl.backfill(con, args.region, since=since)
        print(f"silver: {fl.build_silver(con)} snapshots processed")
    if args.no_live:
        return
    env = fl.load_env()
    token = None
    if env.get("OPENSKY_CLIENT_ID") and env.get("OPENSKY_CLIENT_SECRET"):
        token = fl.get_token(env["OPENSKY_CLIENT_ID"], env["OPENSKY_CLIENT_SECRET"])
        print("OpenSky account: 4,000 credits per day")
    else:
        print("anonymous: 400 credits per day for your IP address (shared with everyone behind it)")

    def log(msg):
        print(msg)
        fl.build_silver(con)                     # every new snapshot goes straight on to silver

    try:
        n = fl.collect(con, args.region, args.interval, args.minutes, args.min_credits, token, log=log)
        print(f"done: {n} new snapshots")
    except KeyboardInterrupt:
        print("stopped")
    finally:
        fl.build_silver(con)
        con.close()


if __name__ == "__main__":
    main()
