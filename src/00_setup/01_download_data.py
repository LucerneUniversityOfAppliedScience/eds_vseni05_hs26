# Databricks notebook source
# MAGIC %md
# MAGIC # 01 – Setup: download the workshop data into the volume
# MAGIC
# MAGIC This notebook is normally run **automatically** by the job
# MAGIC `workshop_setup` (see `resources/setup_data.job.yml`).
# MAGIC
# MAGIC The data is too large for the Git repo, so it is published as a ZIP file in a
# MAGIC **GitHub release**. This notebook downloads the ZIP and extracts it into the Unity Catalog volume
# MAGIC `/Volumes/<catalog>/mobility_raw/landing/`.
# MAGIC
# MAGIC **Fallback (no internet access):** download `workshop_data.zip` yourself and upload it to
# MAGIC `/Volumes/<catalog>/mobility_raw/landing/_download/workshop_data.zip`
# MAGIC (Catalog Explorer → volume → *Upload to this volume*). The notebook then skips the download.
# MAGIC
# MAGIC | Folder in the volume | Content | Source |
# MAGIC |---|---|---|
# MAGIC | `mobility/bike_pedestrian_counts/` | Automatic bike & pedestrian counts, City of Zurich, 2009–2026, 15-minute values | [Open Data Zurich](https://data.stadt-zuerich.ch/dataset/ted_taz_verkehrszaehlungen_werte_fussgaenger_velo), CC0 |
# MAGIC | `mobility/bike_pedestrian_stations/` | Bike & pedestrian counting stations (name, direction, coordinates) | [Open Data Zurich](https://data.stadt-zuerich.ch/dataset/geo_standorte_der_automatischen_fuss__und_velozaehlungen), CC0 |
# MAGIC | `mobility/motorized_traffic_counts/` | Motorized traffic counts, City of Zurich, 2012–2026, hourly values | [Open Data Zurich](https://data.stadt-zuerich.ch/dataset/sid_dav_verkehrszaehlung_miv_od2031), CC0 |
# MAGIC | `mobility/weather/` | Hourly weather in Zurich, 2009–2026 | [Open-Meteo](https://open-meteo.com/), CC BY 4.0 |
# MAGIC | `sample_data/` | Sample files (CSV, JSON, XML, XLSX, Parquet, …) for the exercises | various |
# MAGIC | `adventureworks/` | AdventureWorks sample database as CSV | Microsoft sample data |

# COMMAND ----------

import os
import shutil
import tempfile
import urllib.request
import zipfile

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text(
    "data_url",
    "https://github.com/LucerneUniversityOfAppliedScience/eds_vseni05_hs26/releases/download/data-v1/workshop_data.zip",
    "Data URL (ZIP)",
)
dbutils.widgets.text("target_dir", "", "Target dir (empty = auto)")

catalog = dbutils.widgets.get("catalog").strip() or "workspace"
data_url = dbutils.widgets.get("data_url").strip()
target_dir = dbutils.widgets.get("target_dir").strip() or f"/Volumes/{catalog}/mobility_raw/landing"
zip_in_volume = f"{target_dir}/_download/workshop_data.zip"

if not os.path.isdir(target_dir):
    raise FileNotFoundError(
        f"Volume not found: {target_dir} – did you deploy the bundle (Deployments panel → Deploy)?"
    )
print(f"Target: {target_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download (skipped if the ZIP is already in the volume)

# COMMAND ----------

os.makedirs(os.path.dirname(zip_in_volume), exist_ok=True)

if os.path.exists(zip_in_volume):
    print(f"ZIP already in volume, skipping download: {zip_in_volume}")
else:
    print(f"Downloading {data_url} ...")
    local_zip = os.path.join(tempfile.mkdtemp(), "workshop_data.zip")
    with urllib.request.urlopen(data_url, timeout=600) as response, open(local_zip, "wb") as out:
        shutil.copyfileobj(response, out, length=16 * 1024 * 1024)
    print(f"Downloaded {os.path.getsize(local_zip) / 1e6:.0f} MB")
    shutil.copyfile(local_zip, zip_in_volume)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract into the volume

# COMMAND ----------

local_zip = os.path.join(tempfile.mkdtemp(), "workshop_data.zip")
shutil.copyfile(zip_in_volume, local_zip)   # zipfile needs random access -> work on a local copy

extracted, skipped = 0, 0
with zipfile.ZipFile(local_zip) as zf:
    for member in zf.infolist():
        if member.is_dir() or os.path.basename(member.filename).startswith("."):
            continue
        dst = os.path.join(target_dir, member.filename)
        if os.path.exists(dst) and os.path.getsize(dst) == member.file_size:
            skipped += 1
            continue
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        with zf.open(member) as src, open(dst, "wb") as out:
            shutil.copyfileobj(src, out, length=16 * 1024 * 1024)
        print(f"extract  {member.filename} ({member.file_size / 1e6:.1f} MB)")
        extracted += 1

print(f"\nDone: {extracted} extracted, {skipped} already there.")
if extracted + skipped == 0:
    raise RuntimeError("The ZIP file contained no files.")

# COMMAND ----------

display(dbutils.fs.ls(target_dir))
