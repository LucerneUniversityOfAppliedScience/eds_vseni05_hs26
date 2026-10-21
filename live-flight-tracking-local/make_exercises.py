"""Maintainer tool: solutions/*.py -> exercises/*.py. In a solution notebook every gap looks like

    # >>> TODO 1.1: what to do
    # ?? response = None          <- the line(s) the learner starts with
    real solution code ...
    # <<< TODO 1.1

The exercise keeps "# TODO 1.1: what to do" and the "# ??" lines (uncommented), the solution code is dropped.
Run after every change of a solution:  uv run python make_exercises.py"""
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
START = re.compile(r"^(\s*)# >>> (TODO [\d.]+:.*)$")
END = re.compile(r"^\s*# <<< TODO")
GIVEN = re.compile(r"^(\s*)# \?\? ?(.*)$")

for src in sorted((HERE / "solutions").glob("*.py")):
    out, inside, gaps = [], False, 0
    for line in src.read_text().splitlines():
        m = START.match(line)
        if m:
            inside, gaps = True, gaps + 1
            out.append(f"{m.group(1)}# {m.group(2)}")
            continue
        if inside and END.match(line):
            inside = False
            continue
        if inside:
            g = GIVEN.match(line)
            if g:
                out.append(f"{g.group(1)}{g.group(2)}")
            continue
        out.append(line)
    if inside:
        raise SystemExit(f"{src.name}: unclosed TODO block")
    dst = HERE / "exercises" / src.name
    dst.write_text("\n".join(out) + "\n")
    print(f"{dst.relative_to(HERE)}: {gaps} gaps")
