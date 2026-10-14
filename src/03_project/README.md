# Project: Bike vs. car in Zurich – a step-by-step guide

**Question:** *How has sustainable mobility in Zurich developed – and what influences whether people cycle or drive?*

You combine four open data sources of the City of Zurich and Open-Meteo, build a small lakehouse
(bronze → silver → gold) and finish with an **AI/BI dashboard** that you can share.

```
 SOURCES (volume)               BRONZE            SILVER                    GOLD                      DASHBOARD
 bike & pedestrian counts ──►  bike_counts  ──►  bike_counts_hourly ──┐
 bike stations            ──►  bike_stations ──► bike_stations ───────┤──►  daily_traffic   ──┐
 motorized traffic counts ──►  car_counts   ──►  car_counts_hourly ───┤     yearly_trend    ──┼──►  KPIs, trends,
                                               └► car_sites ──────────┤     location_map    ──┤     rain effect,
 weather                  ──►  weather      ──►  weather_hourly ──────┘     hourly_profile  ──┘     rush hours, map
```

| Data | Size |
|---|---|
| Bike & pedestrian counts, 15-minute values, 2009–2026 | 15.8 million rows |
| Motorized traffic counts, hourly values, 2012–2026 | 23.4 million rows |
| Counting stations | 175 bike/pedestrian stations, ≈ 110 car counting sites |
| Weather, hourly, 2009–2026 | 156 000 rows |

## Before you start

- The bundle is deployed and the setup job `workshop_setup` ran successfully (see the main README).
- You did **01_getting_started/01_databricks_basics** (or know notebooks, `display()` and `%sql`).
- Work through the notebooks **in order** – each step builds on the tables of the previous one.
- Every task is marked **Task x.y**. Replace `raise NotImplementedError(...)` (Python) or
  `SELECT 'replace this line with your query'` (SQL) with your solution.
- Stuck? Read the hints in the task, ask the Databricks Assistant (✨) – but make sure you understand the code –
  or ask your neighbour. Below every task you find **💡 Hint 1** (the idea), **🧩 Hint 2** (a code skeleton) and
  the **🔑 Solution** – click to expand, but try it yourself first.

## The steps

| Step | Notebook | What you do | Time |
|---|---|---|---|
| 1 | [step_1_explore_sources.py](step_1_explore_sources.py) | Look at the four sources, understand columns, grains and data quality issues | 20' |
| 2 | [step_2_bronze.py](step_2_bronze.py) | Load all sources 1:1 into `mobility_bronze` (Parquet and CSV) | 20' |
| 3 | [step_3_silver.py](step_3_silver.py) | Clean and type the data, convert coordinates, bring bikes and cars to the **same grain (hour)** | 40' |
| 4 | [step_4_gold.py](step_4_gold.py) | Aggregate to days, **join bikes + cars + weather**, build the tables for the dashboard | 40' |
| 5 | [step_5_analysis.py](step_5_analysis.py) | Answer 8 questions: trend, COVID lockdown, rain, temperature, rush hours, hotspots, data quality, CO₂ | 45' |
| 6 | this README, [below](#step-6--build-the-dashboard) | Build and publish an **AI/BI dashboard** | 40' |
| 7 | this README, [below](#step-7--ask-genie) | Ask questions in natural language with **Genie** | 15' |
| 8 | optional, [below](#step-8-optional--automate-it) | Automate everything with a pipeline and a job | – |

---

## Step 6 – Build the dashboard

**Goal:** one page that answers the project question at a glance. Requirement: steps 2–4 completed
(the dashboard reads the `mobility_gold` tables).

### 6.1 Create the dashboard and the datasets

1. Left sidebar → **Dashboards** → **Create dashboard**. Rename it (click the title), e.g. *Bike vs. car in Zurich*.
2. Switch to the **Data** tab → **Create from SQL** and add the following four datasets
   (rename each dataset by clicking its name):

**`daily`** – one row per day
```sql
SELECT date, year, weekday, is_weekend,
       bikes_per_station, pedestrians_per_station, vehicles_per_site,
       bikes, vehicles, temp_mean_c, precipitation_mm,
       IF(is_rainy_day, 'rainy (>= 1 mm)', 'dry') AS weather
FROM workspace.mobility_gold.daily_traffic
WHERE year BETWEEN 2011 AND 2026
```

**`yearly`** – one row per year
```sql
SELECT make_date(year, 1, 1) AS year_start, *
FROM workspace.mobility_gold.yearly_trend
```

**`profile`** – rush hours 2025
```sql
SELECT * FROM workspace.mobility_gold.hourly_profile
```

**`locations`** – map 2025
```sql
SELECT * FROM workspace.mobility_gold.location_map
```

### 6.2 Add the widgets

Switch back to the canvas (page tab at the bottom). Use **Add a visualization** (chart icon in the toolbar),
place the widget, then configure it in the panel on the right.

| # | Widget | Dataset | Settings |
|---|---|---|---|
| 1 | **Text** | – | Title and one sentence about the data source |
| 2 | **Counter** "Bikes per station and day" | `daily` | Value: `AVG(bikes_per_station)` |
| 3 | **Counter** "Vehicles per site and day" | `daily` | Value: `AVG(vehicles_per_site)` |
| 4 | **Counter** "Rainy days" | `daily` | Value: `COUNT` of rows, filter `weather = rainy (>= 1 mm)` (or compute a 0/1 column in the dataset) |
| 5 | **Line** "Long-term trend" | `yearly` | X: `year_start` (yearly), Y: `avg_daily_bikes_per_station` **and** `avg_daily_vehicles_per_site` |
| 6 | **Bar** "Rain effect (weekdays)" | `daily` | X: `weather`, Y: `AVG(bikes_per_station)`; add a widget filter `is_weekend = false` |
| 7 | **Line** "Rush hours 2025" | `profile` | X: `hour`, Y: `AVG(avg_per_location)`, Color: `day_type`; filter `mode = Bike` (duplicate the widget for cars) |
| 8 | **Map (point map)** "Counting locations 2025" | `locations` | Latitude: `lat`, Longitude: `lon`, Color: `mode`, Size: `avg_daily_count` |
| 9 | **Filter – date range** | `daily` | Field: `date` → filters every widget that uses `daily` |

💡 **Tips**
- The ✨ **Assistant** in the dashboard editor can create widgets from a description, e.g.
  *"Bar chart of average bikes_per_station by weekday"*.
- Bikes per station (≈ 1 000–2 000) and vehicles per site (≈ 10 000–15 000) have very different scales.
  Try a **second y-axis** in the line chart (Y axis → *Enable dual axis*), or show them in two charts.
- Keep it simple: one clear message per widget, meaningful titles, units in the titles.

### 6.3 Publish and share

1. **Publish** (top right). Choose *Embed credentials* so that viewers see the data with your permissions.
2. **Share** → add your neighbour, or copy the link.
3. Look at the published version: what would a city planner learn from your dashboard in 30 seconds?

---

## Step 7 – Ask Genie

1. Left sidebar → **Genie** → **New**.
2. Add the tables `workspace.mobility_gold.daily_traffic`, `yearly_trend`, `hourly_profile`, `location_map`.
3. Under **Instructions** add for example:
   ```text
   Data: automatic bike, pedestrian and motorized traffic counts of the City of Zurich, joined with daily weather.
   bikes_per_station = average number of bikes per active counting station and day.
   vehicles_per_site = average number of motor vehicles per active counting site and day.
   A rainy day has at least 1 mm of precipitation (is_rainy_day = true).
   Always compare averages per station/site, not totals, because the number of stations changes over the years.
   ```
4. Ask questions, e.g.:
   - *How did bikes per station develop between 2015 and 2025 compared to vehicles per site?*
   - *What was the busiest bike counting location in 2025?*
   - *On which weekday is cycling most popular?*
   - *Show the rush hours for bikes on weekdays as a line chart.*
5. 🔎 Click **Show code** for an answer: is the generated SQL correct? Compare it with your results from step 5.
6. *Optional:* link the Genie space to your dashboard (dashboard draft → ⋮ → *Settings* → *Genie*).

🤔 **Discussion:** where is Genie helpful, where is it risky? What is needed so that experts – e.g. in a
transport planning office – can trust the answers?

---

## Step 8 (optional) – Automate it

In steps 2–4 you ran notebooks by hand. In practice this runs **automatically**, e.g. every night:

- A **Lakeflow Declarative Pipeline** describes the bronze → silver → gold tables *declaratively*
  (`CREATE OR REFRESH STREAMING TABLE ...`, `CREATE OR REFRESH MATERIALIZED VIEW ...`) incl. data quality rules
  (`CONSTRAINT ... EXPECT ...`). Databricks figures out the order and loads only new files.
- A **job** refreshes the pipeline on a schedule.
- Both – and the dashboard – are defined as code in the **bundle** (`resources/*.yml`) and deployed with
  `databricks bundle deploy`.

Try it yourself: **Jobs & Pipelines → Create → ETL pipeline**, and rewrite one of your silver tables in SQL.
A complete reference is part of this repo and deployed with the bundle:

- **Pipeline:** [reference/pipeline](reference/pipeline/transformations) → *Jobs & Pipelines* → `mobility_pipeline` → **Start**
  (writes to the schema `mobility_pipeline`; the first run needs a few minutes).
- **Dashboard as code:** [reference/dashboard/mobility.lvdash.json](reference/dashboard/mobility.lvdash.json) →
  *Dashboards* → `Bike vs car in Zurich` (shows data once your gold tables from step 4 exist).
