# For instructors: preparation & flow

## Checklist before the workshop

- [ ] The repo is **public** (students clone without login, the setup job downloads the data release without a token).
- [ ] The data release exists: `data-v1` with asset `workshop_data.zip`
      (build it with `./data/build_data_zip.sh`, upload with `gh release create`, see [data/README.md](../data/README.md)).
- [ ] Ask students to create a [Free Edition account](https://www.databricks.com/learn/free-edition) and install the CLI
      **beforehand** ([01_setup_free_edition.md](01_setup_free_edition.md)) – saves 15–20 minutes.
- [ ] Run through everything once in a fresh Free Edition workspace:
  ```bash
  databricks bundle validate --profile <profile>
  databricks bundle deploy   --profile <profile>
  databricks bundle run setup_data --profile <profile>
  ```
  then run the notebooks of `src/01_getting_started`, the project (with the solutions) and open the dashboard.
- [ ] Solutions stay hidden until Thursday (see below).

## Solutions: hidden until Thursday

`solutions/` and `resources/solution_*.yml` are listed in `.gitignore`, so they are **not pushed**.
The bundle works without them. They contain:

| Path | Content |
|---|---|
| `solutions/02_exercises/` | solutions of e01–e08 |
| `solutions/03_project/step_*.py` | complete project notebooks (source of truth, with `>>> SOLUTION` markers) |
| `solutions/make_student_versions.py` | generates `src/03_project/step_*.py` from the solutions (replaces the marked blocks with TODOs) |
| `solutions/pipeline/` + `resources/solution_pipeline.yml` | reference Lakeflow Spark Declarative Pipeline (step 8) + refresh job |
| `solutions/dashboards/` + `resources/solution_dashboard.yml` | reference dashboard as code (step 6) |
| `solutions/04_bonus/` | NYC taxi solutions |

**On Thursday:** remove the two `solutions` lines from `.gitignore`, commit and push. Students `git pull` and
`databricks bundle deploy` – the reference pipeline and dashboard then appear in their workspace.

> ⚠️ The repo is public: never commit anything from `solutions/` before Thursday – the Git history is public too.

**Changing a project task?** Edit the notebook in `solutions/03_project/`, then regenerate the student version:
`python solutions/make_student_versions.py`.

## Didactic arc

1. **Setup as code** (DAB) – "one command and the whole environment is there"
2. **Getting started + basic exercises** – learn the tools on small, clear datasets
3. **Project by hand** (notebooks, steps 1–5) – understand what happens, with real data (~40 M rows)
4. **Share** (dashboard, Genie) – how results reach decision makers
5. **Automate** (pipeline, job – reference solution) – how it runs in practice

## Reference results of the project (solution run on the full data)

| Question | Result |
|---|---|
| Q1 trend 2015 → 2025 | bikes per station and day **954 → 1 624 (+70 %)**; vehicles per site and day **17 303 → 14 415 (−17 %)** |
| Q2 lockdown 16.3.–10.5.2020 vs 2019 (weekdays) | bikes 1 321 → 1 201 (−9 %); vehicles 16 300 → 12 386 (**−24 %**) |
| Q3 rain (weekdays 2023–2025) | bikes 1 855 dry vs 1 513 rainy (**−18 %**); vehicles 15 018 vs 14 983 (≈ 0 %) |
| Q4 correlation with temperature | bikes **0.72**, vehicles **0.05** |
| Q5 rush hour (2025) | peak at 17:00 on weekdays and 16:00 on weekends, for bikes and cars (morning peak at 7–8) |
| Q6 hotspots 2025 | bikes: Langstrasse (Unterführung Nord) ≈ 3 500/day, Stadttunnel Süd ≈ 3 060, Langstrasse (Unterführung Süd) ≈ 2 950; cars: A1 Schwamendingen ≈ 82 000/day, Rosengartenstrasse ≈ 50 000 |
| Q7 data quality | active bike stations per year vary from 11 (2011) to 23; totals per year mainly reflect the number of stations |

Discussion points: the rain effect hits cyclists, not drivers; the lockdown reduced car traffic much more than cycling;
cycling grows strongly while car traffic per site declines slightly – and why per-station averages are still
not perfect (new stations are not placed randomly → panel analysis as extension).

## Free Edition specifics

See the official [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations).

- **Fair-usage compute quota per account.** If it is exceeded, compute is shut down for the rest of the day
  (in extreme cases the rest of the month); data and settings are kept. Every student has their own account, so one
  normal run through the workshop is fine – but avoid re-running steps 2/3 (≈ 40 M rows) and the reference pipeline
  over and over. Some limits can be raised with **LinkedIn verification** (button *Verify identity* in the top bar).
- **Jobs:** officially max. 5 concurrent job tasks per account; in practice tasks of one run were queued one after the
  other ("There are already 1 active runs"). Interactive notebooks are not affected.
- **Lakeflow pipelines:** one active pipeline per type. A pipeline update can fail with
  `RESOURCE_EXHAUSTED: You have hit your limit for serverless compute` when the account is busy – simply retry later.
- **SQL warehouse:** exactly one, size 2X-Small (`Serverless Starter Warehouse`).
- **Outbound internet is restricted to an allow-list.** GitHub, PyPI and Open-Meteo work;
  `data.stadt-zuerich.ch` and the NYC TLC CDN do not. That is why the data comes from the GitHub release.
- **Catalogs** cannot be created via the API/bundle (no storage root), but `CREATE CATALOG` in SQL works
  (uses the default storage). The setup job therefore creates `mzmv` with SQL.
- `bundle deploy` times out on large files (> ≈ 50 MB) → keep data out of the bundle.

## Common pitfalls

| Situation | Help |
|-----------|-------|
| Legacy Python CLI (`databricks-cli` v0.x) installed | `pip uninstall databricks-cli`, install the new CLI |
| Laptop without admin rights | option B (Git folder in the browser) |
| `Schema ... already exists` | students already have schemas with the same name → delete them or `--var="catalog=..."` |
| Project notebooks do not find tables | previous step not (completely) run, or serverless not connected |
| Dashboard empty | gold tables (step 4) not created yet |
| Step 2/3 is slow | first run on ~40 M rows takes a few minutes on serverless – that's normal |

## Reuse / adapt

- **Shared workspace instead of Free Edition:** schema names would collide. Use an additional target with
  `mode: development` (prefix per user) or one catalog per person via `--var="catalog=<name>"`.
- **Newer data:** download the latest files from Open Data Zurich into `data/`, rebuild the ZIP, publish as `data-v2`
  and change the variable `data_url` in `databricks.yml`.
