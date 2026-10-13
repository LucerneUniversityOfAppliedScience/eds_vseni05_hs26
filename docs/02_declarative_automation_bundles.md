# 2 – Set up the environment with Declarative Automation Bundles (DABs)

⏱️ ≈ 15 minutes

## What is this about?

Instead of clicking schemas, volumes and jobs together **by hand** in the UI, we describe them **as code** in YAML
files. A single command then creates everything:

```bash
databricks bundle deploy
```

This is called **infrastructure as code**. Benefits:

- **Reproducible** – every student gets exactly the same environment
- **Versioned** – changes are traceable in Git
- **Portable** – the same files work in development, test and production workspaces
- **Fewer mistakes** – no "did I tick that box?"

> Declarative Automation Bundles were formerly called *Databricks Asset Bundles* – you will find both names online.

## Anatomy of this bundle

```
databricks.yml                ← entry point: name, variables, target
resources/
├── unity_catalog.yml         ← 6 schemas + 1 volume
├── setup_data.job.yml        ← job: create the mzmv catalog, download the data, load AdventureWorks
└── cleanup.job.yml           ← job: delete all workshop data (manual)
src/ …                        ← code the resources refer to (notebooks)
```

### `databricks.yml` – the essentials

```yaml
bundle:
  name: mobility_workshop

include:
  - resources/*.yml               # include all resource files

variables:
  catalog:
    default: workspace            # in Free Edition the catalog is called "workspace"
  data_url:
    default: https://github.com/.../releases/download/data-v1/workshop_data.zip

targets:
  prod:                           # target environment (there could also be dev, test, …)
    mode: production
    default: true
    workspace:
      root_path: ~/${bundle.name} # files go to /Workspace/Users/<you>/mobility_workshop
    presets:
      trigger_pause_status: PAUSED   # schedules stay disabled
```

The **workspace host is not in the file** – it comes from your CLI profile (`--profile workshop`).
That's why everybody can use the same file.

### One resource – example: volume

```yaml
resources:
  volumes:
    landing_volume:
      name: landing
      catalog_name: ${var.catalog}                            # variable
      schema_name: ${resources.schemas.raw_schema.name}       # reference to another resource
      volume_type: MANAGED
```

`${...}` are **substitutions**: variables (`${var.catalog}`), references to other resources
(`${resources.schemas.raw_schema.name}` – this also tells Databricks to create the schema first)
or deployment information (`${workspace.file_path}`, `${workspace.current_user.userName}`).

### Why is the data not in the repo?

The data is ≈ 260 MB. That is too much for Git and for the bundle upload (`bundle deploy` would time out).
It is therefore published as a ZIP file in a **GitHub release**; the setup job downloads it directly into your volume.
GitHub is on the allow-list of the Free Edition firewall – many other websites are not.

---

## Option A – With the CLI (recommended)

Prerequisite: [setup](01_setup_free_edition.md) done, terminal open in the repo folder.

### 1. Validate

```bash
databricks bundle validate --profile workshop
```

Checks the YAML files and shows where the bundle will be deployed. Expected: `Validation OK!`

### 2. Deploy

```bash
databricks bundle deploy --profile workshop
```

Uploads the notebooks to `/Workspace/Users/<you>/mobility_workshop/` and creates all resources.

### 3. Load the data (run the setup job)

```bash
databricks bundle run setup_data --profile workshop
```

The job creates the catalog `mzmv`, downloads the data ZIP (≈ 100 MB) from GitHub, extracts it into
`workspace.mobility_raw.landing` and loads the AdventureWorks tables. Duration: 3–5 minutes.

### 4. Overview

```bash
databricks bundle summary --profile workshop
```

Shows all created resources with links to the workspace.

### 5. Continue in the workspace

In the browser open **Workspace → Home → mobility_workshop → files → src** and start with
`01_getting_started/01_databricks_basics`.

> ⚠️ Changes you make to the bundle files in the workspace are overwritten by your local version on the next
> `bundle deploy`. For the workshop that's fine – in real projects you work locally or in a Git folder (option B)
> and deploy from there.

---

## Option B – Browser only (Git folder)

If you cannot install the CLI:

1. In the workspace click **Workspace** → **Home** → **Create → Git folder**.
2. Paste the repository URL `https://github.com/LucerneUniversityOfAppliedScience/eds_vseni05_hs26.git` → **Create Git folder**.
3. Open the file `databricks.yml` in the new folder. Databricks recognises the bundle automatically.
4. Open the **Deployments** icon (🚀) in the left panel of the editor, choose target `prod` → **Deploy**.
5. After the deployment click **Run** (▶︎) next to the job `[Workshop] Setup: load data` in the same panel.
6. Open the notebooks directly in the Git folder under `src/`.

---

## Exercises on bundles (for the fast ones)

1. **Find the resources:** open *Jobs & Pipelines* and *Catalog* in the UI. How can you tell that the objects are
   managed by a bundle? (Hint: banner, tags, path)
2. **Change something and redeploy:** add a tag `owner: <your name>` to the cleanup job in
   `resources/cleanup.job.yml` and run `databricks bundle deploy` again. Check the change in the UI.
3. **Add a schema:** add a schema `sandbox_<your name>` to `resources/unity_catalog.yml`, deploy, and find it in the catalog.
4. **A second target:** add a target `dev` with `mode: development` to `databricks.yml` and deploy with
   `databricks bundle deploy --target dev --profile workshop`. What happens to the resource names?
   (Hint: in development mode resources get the prefix `[dev <user>]` or `dev_<user>_` – so several people can work in
   the same workspace without getting in each other's way.) Clean up with
   `databricks bundle destroy --target dev --profile workshop`.
5. **Reset and remove everything** (only at the very end!):
   ```bash
   databricks bundle run cleanup --params confirm=yes --profile workshop   # deletes all data
   databricks bundle destroy --profile workshop                            # removes the bundle resources
   ```

---

## Troubleshooting

| Error | Cause / solution |
|---------------|------------------|
| `Schema 'mobility_bronze' already exists` | the schema was created by hand earlier → delete it in the catalog, or deploy with `--var="catalog=<other catalog>"` |
| `cannot configure default credentials` | `--profile workshop` missing or login expired → `databricks auth login ...` |
| Setup job: `Volume not found` | run `bundle deploy` first, then `bundle run` |
| Setup job: download fails | upload `workshop_data.zip` (from the GitHub release) manually to `/Volumes/workspace/mobility_raw/landing/_download/` and run the job again |
| Job waits with "There are already 1 active runs" | Free Edition runs only one job at a time – wait until the other one has finished |
| Notebook does not find tables | previous step not completed? Catalog widget at the top of the notebook set to `workspace`? |
