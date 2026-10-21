"""The whole thing with one command – no notebook needed:

    uv run start.py              # history from the class bucket + live Switzerland + map in the browser
    uv run start.py --hours 6    # less history (faster start)
    uv run start.py --no-world   # Switzerland only (no world feed from the bucket)

What it does: reads .env, backfills the last --hours from R2 (if the R2 keys are there, no OpenSky credits),
then starts the map app (app/app.py) – it collects Switzerland live, mirrors the world feed every 5 minutes and runs
all analyses in the background – and opens http://127.0.0.1:<port> in your browser. Stop with Ctrl+C.
"""
import argparse
import os
import runpy
import socket
import sys
import threading
import time
import webbrowser
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import flightlab as fl  # noqa: E402


def free_port(start=8050):
    for port in range(start, start + 20):
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", port)) != 0:
                return port
    raise SystemExit("no free port between 8050 and 8069")


def main():
    ap = argparse.ArgumentParser(description="Backfill from R2 and start the map app.")
    ap.add_argument("--hours", type=float, default=24, help="hours of history from the class bucket (default 24)")
    ap.add_argument("--no-world", action="store_true", help="do not mirror the world feed from the bucket")
    ap.add_argument("--no-browser", action="store_true", help="do not open the browser")
    args = ap.parse_args()

    env = fl.load_env()
    has_r2 = all(env.get(k) for k in ("R2_ENDPOINT", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"))
    if not has_r2:
        print("No R2 keys in .env – no history and no world feed, only live Switzerland from OpenSky.\n"
              "Copy .env.template to .env and fill in the R2 values from the lecture slides.")
    os.environ.setdefault("BACKFILL_HOURS", str(args.hours if has_r2 else 0))   # done by the app's collector thread
    if has_r2 and not args.no_world:
        os.environ.setdefault("R2_REGIONS", "world")
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    port = int(os.environ.setdefault("PORT", str(free_port())))

    if not args.no_browser:
        def _open():
            time.sleep(8)                                    # the app needs a few seconds to start
            webbrowser.open(f"http://127.0.0.1:{port}")
        threading.Thread(target=_open, daemon=True).start()

    print(f"Starting the map on http://127.0.0.1:{port} – history: {os.environ['BACKFILL_HOURS']} h, "
          f"world feed: {'on' if os.environ.get('R2_REGIONS') else 'off'}. Ctrl+C stops it.")
    sys.path.insert(0, str(HERE / "app"))
    os.chdir(HERE / "app")
    runpy.run_path(str(HERE / "app" / "app.py"), run_name="__main__")


if __name__ == "__main__":
    main()
