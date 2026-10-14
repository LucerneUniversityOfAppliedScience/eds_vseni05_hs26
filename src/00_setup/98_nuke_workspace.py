# Databricks notebook source
# MAGIC %md
# MAGIC # 98 – Nuke workspace: start from a clean workspace
# MAGIC
# MAGIC Use this notebook if your workspace already contains things from earlier courses or experiments and you want to
# MAGIC **start completely fresh**. It finds and deletes:
# MAGIC
# MAGIC | # | What | How |
# MAGIC |---|---|---|
# MAGIC | 1 | AI/BI **dashboards** | moved to the trash |
# MAGIC | 2 | **Jobs** | deleted |
# MAGIC | 3 | **Pipelines** (incl. their streaming tables / materialized views) | deleted |
# MAGIC | 4 | **Genie spaces** | moved to the trash |
# MAGIC | 5 | all **catalogs** you created (e.g. `mzmv`) – not `workspace`, `samples`, `system` | `DROP CATALOG … CASCADE` |
# MAGIC | 6 | all **schemas** in the catalog `workspace` except the built-in `default` and `information_schema` – incl. tables, views, volumes | `DROP SCHEMA … CASCADE` |
# MAGIC | 7 | everything inside `workspace.default`: tables, views, volumes, functions, registered models | `DROP …` |
# MAGIC | 8 | the deployed bundle folder `/Workspace/Users/<you>/mobility_workshop` | deleted |
# MAGIC
# MAGIC In **Free Edition** the catalog `workspace` is the default catalog and cannot be deleted. The notebook therefore
# MAGIC keeps the catalog and its built-in schemas `default` and `information_schema`, but empties everything else in it.
# MAGIC
# MAGIC **Not touched:** your Git folders and notebooks, the catalogs `workspace` (only emptied), `samples` and `system`.
# MAGIC
# MAGIC ### How to use
# MAGIC 1. Run all cells with the default `mode = dry_run` → the notebook only **lists** what it would delete.
# MAGIC 2. Check the list. Then set the widgets `mode` = `delete` and `confirm` = `NUKE` and run all cells again.
# MAGIC 3. Afterwards: in your Git folder → **Deployments** → **Deploy**, then run `01_workshop_setup`.
# MAGIC
# MAGIC ⚠️ **Deleted data cannot be restored.** Run this as a notebook, not as a job – a job would delete itself.

# COMMAND ----------

dbutils.widgets.dropdown("mode", "dry_run", ["dry_run", "delete"], "Mode")
dbutils.widgets.text("confirm", "", "Type NUKE to delete")
dbutils.widgets.dropdown("include_jobs_pipelines_dashboards", "yes", ["yes", "no"], "Also jobs, pipelines, dashboards, Genie")
dbutils.widgets.text("keep_catalogs", "workspace,samples,system,hive_metastore", "Catalogs to keep")

MODE = dbutils.widgets.get("mode")
DELETE = MODE == "delete" and dbutils.widgets.get("confirm").strip() == "NUKE"
INCLUDE_WORKFLOWS = dbutils.widgets.get("include_jobs_pipelines_dashboards") == "yes"
KEEP_CATALOGS = {c.strip() for c in dbutils.widgets.get("keep_catalogs").split(",") if c.strip()}

if MODE == "delete" and not DELETE:
    raise RuntimeError("mode = delete, but confirm is not 'NUKE' – nothing was deleted.")
print("🔥 DELETE MODE – objects will be deleted!" if DELETE else "🔍 DRY RUN – nothing will be deleted, only listed.")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
me = w.current_user.me().user_name
summary = []


def act(kind: str, name: str, action):
    """Run `action` in delete mode, only print it in dry-run mode. Errors are reported, not raised."""
    if not DELETE:
        print(f"would delete  {kind:<10} {name}")
        summary.append((kind, name, "would delete"))
        return
    try:
        action()
        print(f"deleted       {kind:<10} {name}")
        summary.append((kind, name, "deleted"))
    except Exception as e:
        print(f"FAILED        {kind:<10} {name}: {str(e).splitlines()[0][:150]}")
        summary.append((kind, name, "FAILED"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1–4 – Dashboards, jobs, pipelines, Genie spaces

# COMMAND ----------

if INCLUDE_WORKFLOWS:
    for d in w.lakeview.list():
        act("dashboard", d.display_name, lambda d=d: w.lakeview.trash(d.dashboard_id))

    for j in w.jobs.list():
        act("job", j.settings.name if j.settings else str(j.job_id), lambda j=j: w.jobs.delete(j.job_id))

    # Deleting a pipeline also removes its streaming tables and materialized views
    for p in w.pipelines.list_pipelines():
        act("pipeline", p.name, lambda p=p: w.pipelines.delete(p.pipeline_id))

    try:
        spaces = w.genie.list_spaces().spaces or []
    except Exception as e:
        spaces = []
        print(f"(Genie spaces not listed: {str(e).splitlines()[0][:100]})")
    for s in spaces:
        act("genie", s.title, lambda s=s: w.genie.trash_space(s.space_id))
else:
    print("Skipped (include_jobs_pipelines_dashboards = no)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 – Own catalogs

# COMMAND ----------

for row in spark.sql("SHOW CATALOGS").collect():
    catalog = row[0]
    if catalog in KEEP_CATALOGS or catalog.startswith("__"):
        continue
    act("catalog", catalog, lambda c=catalog: spark.sql(f"DROP CATALOG IF EXISTS `{c}` CASCADE"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 – Schemas in the catalog `workspace`

# COMMAND ----------

for row in spark.sql("SHOW SCHEMAS IN `workspace`").collect():
    schema = row[0]
    if schema in ("default", "information_schema"):
        continue
    act("schema", f"workspace.{schema}", lambda s=schema: spark.sql(f"DROP SCHEMA IF EXISTS `workspace`.`{s}` CASCADE"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 – Everything inside `workspace.default` (the schema itself stays)

# COMMAND ----------

for row in spark.sql("SHOW TABLES IN `workspace`.`default`").collect():
    if row.isTemporary:
        continue
    name = f"`workspace`.`default`.`{row.tableName}`"
    kind = spark.sql(f"DESCRIBE TABLE EXTENDED {name}").filter("col_name = 'Type'").collect()
    is_view = bool(kind) and "VIEW" in kind[0].data_type.upper()
    act("view" if is_view else "table", name,
        lambda n=name, v=is_view: spark.sql(f"DROP {'VIEW' if v else 'TABLE'} IF EXISTS {n}"))

for row in spark.sql("SHOW VOLUMES IN `workspace`.`default`").collect():
    name = f"`workspace`.`default`.`{row.volume_name}`"
    act("volume", name, lambda n=name: spark.sql(f"DROP VOLUME IF EXISTS {n}"))

for row in spark.sql("SHOW USER FUNCTIONS IN `workspace`.`default`").collect():
    parts = (["workspace", "default"] + row[0].split("."))[-3:]   # SHOW returns "fn" or "catalog.schema.fn"
    name = ".".join(f"`{p}`" for p in parts)
    act("function", name, lambda n=name: spark.sql(f"DROP FUNCTION IF EXISTS {n}"))

try:
    models = list(w.registered_models.list(catalog_name="workspace", schema_name="default"))
except Exception as e:
    models = []
    print(f"(registered models not listed: {str(e).splitlines()[0][:100]})")
for m in models:
    act("model", m.full_name, lambda m=m: w.registered_models.delete(m.full_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 – Deployed bundle folder

# COMMAND ----------

bundle_folder = f"/Workspace/Users/{me}/mobility_workshop"
this_notebook = "/Workspace" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

if this_notebook.startswith(bundle_folder + "/"):
    print(f"Skipped {bundle_folder}: this notebook runs from there. Run it from your Git folder to delete the folder.")
else:
    try:
        w.workspace.get_status(bundle_folder)
        act("folder", bundle_folder, lambda: w.workspace.delete(bundle_folder, recursive=True))
    except Exception:
        print(f"(no bundle folder {bundle_folder})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

if summary:
    display(spark.createDataFrame(summary, "type string, name string, status string"))
else:
    print("Nothing found – the workspace is already clean.")

if not DELETE:
    print("\nThis was a dry run. To delete: set mode = delete and confirm = NUKE, then run all cells again.")
else:
    print("\nDone. Next: Git folder → Deployments → Deploy, then run the job 01_workshop_setup.")
