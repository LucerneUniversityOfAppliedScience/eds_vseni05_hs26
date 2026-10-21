import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium", app_title="99 – Clean up")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 99 – Clean up

    At the end of the course: what the workshop left on your laptop, how big it is, and a button to delete what you
    tick. Nothing is deleted without the button.

    **Stop the app and `collector.py` first** (Ctrl+C in their terminal) – while they run, `flights.duckdb` is locked.
    Close the other notebooks too.

    Never touched: the notebooks, `flightlab.py`, `app/`, `enrichment/`, `data/` and `pyproject.toml`. To remove
    everything, delete the whole folder afterwards.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    import shutil
    from pathlib import Path

    ROOT = mo.notebook_dir().parent

    def size_of(paths):
        total = 0
        for p in paths:
            if p.is_file():
                total += p.stat().st_size
            elif p.is_dir():
                total += sum(f.stat().st_size for f in p.rglob("*") if f.is_file())
        return total

    def human(n):
        for unit in ("B", "KB", "MB", "GB"):
            if n < 1024 or unit == "GB":
                return f"{n:,.0f} {unit}" if unit == "B" else f"{n:,.1f} {unit}"
            n /= 1024

    def existing(*names):
        return [ROOT / n for n in names if (ROOT / n).exists()]

    GROUPS = {
        "flights": ("Flight database `flights.duckdb` (your collected and backfilled data)",
                    existing("flights.duckdb", "flights.duckdb.wal")),
        "practice": ("Practice databases `practice_02.duckdb` … `practice_06.duckdb` (notebooks 02–06)",
                     sorted(ROOT.glob("practice*.duckdb")) + sorted(ROOT.glob("practice*.duckdb.wal"))),
        "cache": ("Downloads and notebook copies in `cache/` (airports, weather, BFS, EUROCONTROL, snapshot copies)",
                  existing("cache")),
        "marimo": ("marimo session files `__marimo__/` and grid layouts `layouts/`",
                   [p for p in ROOT.rglob("__marimo__") if p.is_dir()] + existing("solutions/layouts", "exercises/layouts")),
        "env": ("Your keys in `.env` (R2 read key, OpenSky client) – delete when the course is over",
                existing(".env")),
    }
    VENV = ROOT / ".venv"            # not deletable from here: this notebook runs inside it
    return GROUPS, ROOT, VENV, human, shutil, size_of


@app.cell(hide_code=True)
def _(GROUPS, VENV, human, mo, size_of):
    picks = mo.ui.dictionary({
        key: mo.ui.checkbox(label=f"{label} – **{human(size_of(paths))}**" if paths else f"{label} – *not there*",
                            value=False, disabled=not paths)
        for key, (label, paths) in GROUPS.items()
    })
    delete = mo.ui.run_button(label="Delete the ticked items", kind="danger")
    _venv = (f"The Python environment `.venv/` ({human(size_of([VENV]))}) cannot delete itself while this notebook "
             "runs in it – close marimo, then `rm -rf .venv` in the terminal (`uv sync` rebuilds it).") if VENV.exists() else ""
    mo.vstack([mo.md("### What is on your laptop"), picks, delete, mo.md(_venv)])
    return delete, picks


@app.cell(hide_code=True)
def _(GROUPS, ROOT, delete, mo, picks, shutil):
    mo.stop(not delete.value, mo.md("*Tick what you want to delete, then press the button.*"))
    _done, _failed = [], []
    for _key, _on in picks.value.items():
        if not _on:
            continue
        for _p in GROUPS[_key][1]:
            try:
                if not _p.resolve().is_relative_to(ROOT.resolve()):     # never outside the workshop folder
                    raise ValueError("outside the workshop folder")
                shutil.rmtree(_p) if _p.is_dir() else _p.unlink()
                _done.append(str(_p.relative_to(ROOT)))
            except Exception as _e:
                _failed.append(f"{_p.relative_to(ROOT)}: {type(_e).__name__} – {_e}")
    mo.vstack([
        mo.callout(mo.md("Deleted: " + (", ".join(f"`{d}`" for d in _done) or "nothing")), kind="success"),
        mo.callout(mo.md("Not deleted (is the app still running?):\n\n" + "\n".join(f"- {f}" for f in _failed)),
                   kind="danger") if _failed else mo.md(""),
        mo.md("Rerun this notebook to see what is left. Without `.venv`, start again with `uv sync`."),
    ])
    return


if __name__ == "__main__":
    app.run()
