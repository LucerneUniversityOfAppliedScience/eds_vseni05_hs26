# Databricks notebook source
# MAGIC %md
# MAGIC # MZMV 01 – Unzip the uploaded ZIP file
# MAGIC
# MAGIC Run by the job `02_mzmv_load` (task `unzip`), but you can also run it by hand.
# MAGIC
# MAGIC ```
# MAGIC /Volumes/mzmv/raw/landing/MZMV.zip   ──unzip──►   /Volumes/mzmv/raw/files/MZMV/...
# MAGIC ```
# MAGIC
# MAGIC - The original delivery contains **ZIP files inside the ZIP** (e.g. `Zeitreihen_SAS.zip`). They are extracted
# MAGIC   recursively into a folder with the same name.
# MAGIC - macOS metadata (`__MACOSX`, `.DS_Store`) and the SAS transport copy of 2015 (`Lecture_en_SAS_via_fichier_CPT`) are skipped.
# MAGIC - Parameter `include`: only paths that contain one of these comma-separated words are extracted.
# MAGIC   Default: SAS data, documentation, questionnaires and SAS code. Use `*` to extract everything (≈ 9 GB).
# MAGIC - Files that already exist with the same size are skipped, so the notebook can be re-run safely.

# COMMAND ----------

import fnmatch
import io
import os
import shutil
import zipfile

dbutils.widgets.text("catalog", "mzmv", "Catalog")
dbutils.widgets.text("zip_name", "*.zip", "ZIP file(s) in the landing volume (pattern)")
dbutils.widgets.text("include", "2_DB_SAS,1_Beschreibung,6_SAS-Codes,7_Fragebogen", "Include (comma-separated, * = all)")

catalog = dbutils.widgets.get("catalog").strip()
landing = f"/Volumes/{catalog}/raw/landing"
target = f"/Volumes/{catalog}/raw/files"
include = [w.strip() for w in dbutils.widgets.get("include").split(",") if w.strip()]

zips = sorted(f for f in os.listdir(landing) if fnmatch.fnmatch(f, dbutils.widgets.get("zip_name").strip()))
if not zips:
    raise FileNotFoundError(
        f"No ZIP file found in {landing}. Upload the MZMV ZIP file first "
        "(Catalog → mzmv → raw → landing → 'Upload to this volume')."
    )
print("ZIP files:", zips)
print("include  :", include)

# COMMAND ----------

stats = {"extracted": 0, "skipped": 0, "nested": 0}


def wanted(path: str) -> bool:
    name = os.path.basename(path)
    if "__MACOSX" in path or "Lecture_en_SAS" in path or name.startswith(".") or name == "":
        return False   # macOS metadata and the SAS transport (CPT) copy of 2015 are not needed
    return "*" in include or any(word in path for word in include)


def copy_member(zf: zipfile.ZipFile, member: zipfile.ZipInfo, dst: str):
    if os.path.exists(dst) and os.path.getsize(dst) == member.file_size:
        stats["skipped"] += 1
        return
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    with zf.open(member) as src, open(dst, "wb") as out:
        shutil.copyfileobj(src, out, length=16 * 1024 * 1024)
    stats["extracted"] += 1
    print(f"extract  {dst.replace(target + '/', '')}  ({member.file_size / 1e6:.1f} MB)")


def extract(zf: zipfile.ZipFile, prefix: str):
    for member in zf.infolist():
        path = prefix + member.filename
        if member.is_dir() or not wanted(path):
            continue
        if member.filename.lower().endswith(".zip"):
            # nested ZIP: extract its content into a folder with the same name (without .zip)
            stats["nested"] += 1
            print(f"nested   {path}")
            with zipfile.ZipFile(io.BytesIO(zf.read(member))) as inner:
                extract(inner, path[:-4] + "/")
        else:
            copy_member(zf, member, os.path.join(target, path))


for name in zips:
    # Volumes support random-access reads, so the ZIP can be opened directly from the volume
    with zipfile.ZipFile(os.path.join(landing, name)) as zf:
        extract(zf, "")

print(f"\nDone: {stats}")

# COMMAND ----------

# Overview: number of files and size per top-level folder
summary = {}
for root, _, files in os.walk(target):
    key = os.path.relpath(root, target).split(os.sep)[:2]
    key = "/".join(key)
    n, size = summary.get(key, (0, 0))
    summary[key] = (n + len(files), size + sum(os.path.getsize(os.path.join(root, f)) for f in files))

display(spark.createDataFrame(
    [(k, n, round(s / 1e6, 1)) for k, (n, s) in sorted(summary.items()) if n],
    "folder string, files long, size_mb double",
))
