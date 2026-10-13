# MZMV – Load the Mobility and Transport Microcensus into Databricks

The **Mobility and Transport Microcensus** (*Mikrozensus Mobilität und Verkehr, MZMV*) of the Swiss Federal Statistical
Office (FSO/BFS) and the Federal Office for Spatial Development (ARE) is the most important survey on travel behaviour
in Switzerland: households, persons, vehicles, trips (*Wege*) and stages (*Etappen*), every five years since 1994.

This guide shows how you upload the data into **your** Databricks workspace and turn it into bronze Delta tables.

> ⚠️ **Data protection:** the MZMV data is provided under a data use agreement. It is **not** part of this repository
> and must never be uploaded to GitHub, shared publicly or copied to other people's workspaces.
> You receive the ZIP file from your lecturer.

## Overview

```
 your laptop                     Unity Catalog (catalog "mzmv")
 MZMV.zip (2.2 GB) ──upload──►  raw.landing   (volume)  MZMV.zip
                                      │  job mzmv_load, task "unzip"
                                      ▼
                                raw.files     (volume)  MZMV/MZMV2021_mit_Geo/..., MZMV/Zeitreihen1994-2021/...
                                      │  job mzmv_load, task "load_bronze"
                                      ▼
                                bronze        (schema)  mzmv2021_wege, mzmv2021_haushalte, …, zeitreihen_wege94, …
                                silver, gold  (schemas) → your own work
```

The catalog `mzmv` with the schemas `raw`, `bronze`, `silver`, `gold` and the volumes `raw.landing` and `raw.files` is
created automatically by the setup job `workshop_setup` (see the main README).

## Step 1 – Upload the ZIP file (≈ 5–15 min, depending on your connection)

**Option A – browser (recommended):**

1. In your workspace open **Catalog** → `mzmv` → `raw` → **Volumes** → `landing`.
2. Click **Upload to this volume** and select `MZMV.zip`. (Limit in the browser: 5 GB per file – the ZIP has 2.2 GB.)
3. Wait until the upload is finished – do not close the browser tab.

**Option B – CLI:**

```bash
databricks fs cp MZMV.zip dbfs:/Volumes/mzmv/raw/landing/MZMV.zip --profile workshop
```

## Step 2 – Run the job `mzmv_load` (≈ 5–10 min)

**In the UI:** *Jobs & Pipelines* → `mzmv_load` → **Run now**.

**Or with the CLI:**

```bash
databricks bundle run mzmv_load --profile workshop
```

The job has two tasks:

| Task | Notebook | What it does |
|---|---|---|
| `unzip` | [01_unzip.py](01_unzip.py) | extracts the ZIP **recursively** (the delivery contains ZIP files inside the ZIP) into `raw.files`. By default only the SAS data, the documentation, the questionnaires and the SAS code are extracted (≈ 2.6 GB instead of ≈ 9 GB). |
| `load_bronze` | [02_load_bronze.py](02_load_bronze.py) | reads every SAS data file (`.sas7bdat`) and writes it as a Delta table into `mzmv.bronze` – with data types, **column descriptions as comments** and the **value labels** as table `<dataset>_formats`. |

**Parameters** (*Run now with different parameters*):

| Parameter | Default | Meaning |
|---|---|---|
| `datasets` | `mzmv2021,zeitreihen` | which datasets to load. Add `mzmv2015` for the 2015 survey (large: ≈ 1.9 GB SAS, incl. 737 MB route segments). |
| `include` | `2_DB_SAS,1_Beschreibung,6_SAS-Codes,7_Fragebogen` | which folders to extract. `*` = everything (incl. CSV, SPSS and route geometries). |
| `zip_name` | `*.zip` | which file(s) in `raw.landing` to extract |

## Step 3 – Explore the bronze tables

**39 tables** with the default settings:

| Dataset | Tables | Content |
|---|---|---|
| **MZMV 2021** (`mzmv2021_*`) | `haushalte` (55 018), `zielpersonen` (55 018), `haushaltspersonen`, `fahrzeuge`, `wege` (151 188), `wegeinland`, `etappen` (205 030), `ausgaenge`, `tagesreisen`, `reisenmueb`, `verifikationspunkte`, `dienstleistungen`, `modul3`, `formats` | the full 2021 survey with geodata |
| **Time series 1994–2021** (`zeitreihen_*`) | `haushalte94` … `haushalte21`, `personen94` … `personen21`, `wege94` … `wege21`, `etappen94` … `etappen21`, `formats` | harmonised core variables of the surveys 1994, 2000, 2005, 2010, 2015, 2021 |

Try it:

1. **Catalog Explorer** → `mzmv` → `bronze` → `mzmv2021_wege` → tab **Columns**: every column has its German description,
   e.g. `WP` = *Personengewicht*.
2. Decode a code with the formats table:
   ```sql
   SELECT START AS code, LABEL AS label
   FROM mzmv.bronze.mzmv2021_formats
   WHERE FMTNAME = 'SPRACHE'
   ```
3. A first weighted result – average number of trips per person and day in 2021 (weights `WP`):
   ```sql
   SELECT ROUND(SUM(WP * n_trips) / SUM(WP), 2) AS trips_per_person_and_day
   FROM (
     SELECT z.HHNR, z.WP, COUNT(w.WEGNR) AS n_trips
     FROM mzmv.bronze.mzmv2021_zielpersonen z
     LEFT JOIN mzmv.bronze.mzmv2021_wege w ON w.HHNR = z.HHNR
     GROUP BY z.HHNR, z.WP
   )
   ```
   ⚠️ Always use the **weights** (`WM` for households, `WP` for persons) – the sample is not representative without them.

## Documentation

The documentation is extracted to `/Volumes/mzmv/raw/files/MZMV/…/1_Beschreibung*` – most important:

- `2023-06-05-Datenbankstruktur_MZMV2021_mit_Geodaten.xlsx` – all tables, variables and codes of 2021
- `Variablen_MZMV_1994-2021.xlsx` and `Zeitreihen_1994-2021_Mikrozensus_Dokumentation.pdf` – the time series
- `7_Fragebogen` – the questionnaires; `6_SAS-Codes der Standardauswertungen` – SAS code of the official standard tables

## FAQ

**Why SAS and not CSV?** The delivery contains both with identical content. SAS keeps the data types (e.g. real dates
instead of `05.03.2021`) and the column descriptions – that's why we load SAS. The CSV files differ between the
years (separator `;` vs. `,`, encoding) and would need more cleaning.

**Can `.sas` files be stored in Delta?** No – `.sas` files are SAS **programs**, not data. They stay as files in the
volume. A nice exercise: translate one of the standard evaluations into SQL!

**The job fails with "No ZIP file found".** The upload into `raw.landing` is missing or not finished yet.

**Can I start over?** The job overwrites the bronze tables on every run and skips files that are already extracted.
The cleanup job `workshop_cleanup` drops the whole `mzmv` catalog – including your uploaded ZIP.
