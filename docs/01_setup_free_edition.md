# 1 – Setup: Databricks Free Edition & CLI

⏱️ ≈ 15 minutes · Ideally done **before** the workshop.

## Step 1: Databricks Free Edition account

1. Open <https://www.databricks.com/learn/free-edition> and click **Sign up**.
2. Register with your university e-mail (or a Google/Microsoft login).
3. After a few minutes your personal workspace is ready.
4. Copy the **URL** of your workspace from the address bar, e.g. `https://dbc-a1b2c3d4-e5f6.cloud.databricks.com`
   (only up to `.com`, without the path).

> **Free Edition:** free of charge, serverless compute only, small resource limits – perfectly fine for this workshop.

## Step 2: Get the repo

```bash
git clone https://github.com/LucerneUniversityOfAppliedScience/eds_vseni05_hs26.git
cd eds_vseni05_hs26
```

No Git installed? On GitHub use **Code → Download ZIP** and unpack it.

## Step 3: Install the Databricks CLI

**macOS (Homebrew)**

```bash
brew tap databricks/tap
brew install databricks
```

**Windows (winget)** – in PowerShell:

```bash
winget install Databricks.DatabricksCLI
```

**macOS / Linux (without Homebrew)**

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
```

Check the version (≥ 0.281 recommended):

```bash
databricks --version
```

## Step 4: Log in (OAuth in the browser)

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile workshop
```

> ⚠️ Replace `<your-workspace>` **including the angle brackets** with your URL from step 1, e.g.
> `databricks auth login --host https://dbc-a1b2c3d4-e5f6.cloud.databricks.com --profile workshop`.
> Otherwise the shell reports `no such file or directory`.

The browser opens, you confirm the login – done. The profile `workshop` is stored locally in `~/.databrickscfg`.

Test:

```bash
databricks current-user me --profile workshop
```

You should see your user name. 🎉

👉 Continue with [02_declarative_automation_bundles.md](02_declarative_automation_bundles.md)

## Problems?

| Problem | Solution |
|---------|--------|
| `databricks: command not found` | open a new terminal; on Windows sign out and in again |
| Old CLI version (`v0.18` or similar) | that is the legacy Python CLI → `pip uninstall databricks-cli`, then install as above |
| Browser does not open | open the URL shown in the terminal manually |
| Laptop without admin rights | use option B (browser only) in the bundle guide |
