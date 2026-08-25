# Analytics Project

End-to-end data pipeline for an e-commerce analytics layer: **orders**, **customers**, **products**, and **order line items**. Built with dbt on Databricks so you can run it locally or deploy via CI/CD.

## What you get

**Medallion architecture** (no prefix/postfix on schema names):

- **Bronze** (`bronze` schema): Raw data from seed CSVs (customers, orders, order_items, products) loaded into Databricks.
- **Silver** (`silver` schema): Staging views for orders, order_items, products (1:1 with raw) + intermediate views. Customers have no staging—only `scd_customers` (snapshot from raw) in gold.
- **Gold** (`gold` schema): Marts — `fct_orders`, `fct_order_items`, `dim_products`; **`scd_customers`** (Type 2 SCD — the only customer table, attributes only, no order-derived fields).

**Concepts included (beginner-friendly):**
- **Incremental models**: `fct_orders` and `fct_order_items` use `merge` so only new/changed data is processed each run.
- **Slowly changing dimension (Type 2)**: `scd_customers` snapshot tracks customer history; use `dbt_valid_from` / `dbt_valid_to` for point-in-time queries.
- **Hooks**: `on-run-start` / `on-run-end` in `dbt_project.yml` for run logging.
- **Tests**: Generic (unique, not_null, relationships, accepted_values) plus singular tests (revenue non-negative, no future dates, positive quantities, valid email, positive product price).

Catalog: **workspace**. Workspace host is set per-developer/per-environment (see [Connect to Databricks](#2-connect-to-databricks) below) — not hardcoded here.

## Prerequisites

- **Python 3.12** (recommended; 3.10–3.11 also work; 3.14 is not yet supported by dbt’s dependencies)
- A Databricks workspace with a **SQL Warehouse** (or all-purpose cluster)
- Databricks **host**, **HTTP path**, and **token** (or OAuth) for connection

## Quick start

### 1. Clone and install

```bash
git clone <your-repo-url>
cd databricks
python3.12 -m pip install -r requirements.txt
```

### 2. Connect to Databricks

Copy `profiles.yml.example` to `profiles.yml` in the project root (it's gitignored, so it stays local to you) and fill in your connection details via environment variables — never hardcode credentials in it:

```bash
cp profiles.yml.example profiles.yml
```

```yaml
analytics:
  target: dev
  outputs:
    dev:
      type: databricks
      host: "{{ env_var('DATABRICKS_HOST') }}"
      http_path: "{{ env_var('DATABRICKS_HTTP_PATH') }}"
      schema: "{{ env_var('DBT_SCHEMA', 'silver') }}"
      catalog: "{{ env_var('DATABRICKS_CATALOG', 'workspace') }}"
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
      threads: 4
      connect_timeout: 60
      connect_retries: 3
```

- `host`: your workspace URL, no `https://` prefix, no trailing slash (e.g. `dbc-xxxxxxxx-xxxx.cloud.databricks.com`)
- `http_path`: from Databricks → SQL Warehouse → Connection details → HTTP path
- `token`: from User Settings → Developer → Access tokens

Then run with:

```bash
export DATABRICKS_HOST="dbc-xxxxxxxx-xxxx.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/xxxxx"
export DATABRICKS_TOKEN="dapi..."
DBT_PROFILES_DIR=. dbt seed && dbt run && dbt test
```

Catalog is `workspace`; schema names are `bronze`, `silver`, `gold` (no prefix/postfix).

### 3. Install packages, load seeds, run models and snapshots

```bash
dbt deps
dbt seed
dbt snapshot   # scd_customers from raw (must run before run, as models depend on it)
dbt run
dbt test
```

### 4. Docs (optional)

```bash
dbt docs generate
dbt docs serve
```

## Project layout (medallion)

```
seeds/              # raw_*.csv → bronze schema
models/
  staging/          # silver: stg_* (views), documented + tested in _staging_models.yml
  intermediate/     # silver: int_* (views)
  marts/core/       # gold: fct_orders, fct_order_items (incremental), dim_products
snapshots/          # gold: scd_customers (Type 2 SCD — only customer table, attributes only)
tests/              # singular tests (assert_*.sql) + schema tests in yml
macros/             # generate_schema_name (schema as-is, no prefix/postfix)
```

**Note:** `models/staging/` also has `orders_clean.sql`, `products_clean.sql`, and `returns_clean.sql`, sourced from a second set of seeds (`orders.csv`, `products.csv`, `returns.csv`) alongside the `raw_*` ones. These aren't covered by `_staging_models.yml`'s docs/tests and aren't referenced elsewhere in this README — likely in-progress or experimental, not yet part of the documented pipeline.

## CI/CD (GitHub Actions)

### Deploy bundle (DAB) — dev and prod

Two workflows deploy the **Databricks Asset Bundle** by branch:

| Branch | Workflow                    | Target | GitHub environment |
|--------|-----------------------------|--------|---------------------|
| **dev**  | `.github/workflows/deploy-bundle-dev.yml`  | `dev`  | `dev`  |
| **main** | `.github/workflows/deploy-bundle-prod.yml` | `prod` | `prod` |

- **Push to `dev`** → validate and deploy to **dev** (`databricks bundle deploy -t dev`). Job and files go to the dev bundle path in the workspace. The job is *not* auto-run — trigger it manually (Databricks Jobs UI, or `databricks bundle run dbt_databricks_job -t dev`) when you want to test it.
- **Push to `main`** → validate, deploy to **prod** (`databricks bundle deploy -t prod`), then **automatically runs** `dbt_databricks_job` (`databricks bundle run dbt_databricks_job -t prod`).
  - The run is scoped to what actually changed: the workflow diffs the push against the previous commit and only rebuilds the affected model(s)/seed(s) (plus anything downstream) or the specific singular test that changed, instead of the whole project every time. If nothing under `models/`, `seeds/`, or `tests/` changed — or on a branch's first push — it falls back to a full build. See the `Determine changed dbt models` step in `deploy-bundle-prod.yml` for the exact logic.

**Required GitHub secrets** (per environment or repo): `DATABRICKS_HOST`, `DATABRICKS_TOKEN`. Configure in **Settings → Secrets and variables → Actions** (repo-level) or per **Environment** (dev / prod) if you use different workspaces or tokens.

**Local deploy:**

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
databricks bundle deploy -t dev    # or -t prod
```

Override `warehouse_id` or `catalog` in `databricks.yml` or via `--var` if needed.

### dbt CI workflow (optional)

If you add a workflow (e.g. `.github/workflows/dbt.yml`) that runs dbt on PRs, use these secrets as needed:

| Secret                 | Description              |
|------------------------|--------------------------|
| `DATABRICKS_HTTP_PATH` | SQL Warehouse HTTP path  |
| `DBT_SCHEMA`           | Schema for CI runs       |
| `DATABRICKS_CATALOG`   | Unity Catalog name       |

## Interview talking points

When asked *"Walk me through a data project you've built"* or *"What's in your dbt project?"*, you can say:

- **Architecture:** "I built a medallion pipeline on Databricks: bronze for raw data, silver for staging and intermediate models, gold for fact and dimension tables. Schema names are clean—no prefix or postfix."
- **Facts & dimensions:** "Gold has two fact tables—`fct_orders` (order grain) and `fct_order_items` (line grain)—and one dimension, `dim_products`. Customers are in `scd_customers` only (Type 2 SCD, attributes only, no order aggregates)."
- **Scale & history:** "The fact tables are incremental with merge so we only process new data. Customer changes are tracked in `scd_customers` for point-in-time reporting."
- **Quality & impact:** "I added generic tests (unique, not_null, relationships, accepted_values) and singular tests for business rules."
- **Deployment:** "The pipeline runs in CI on every push via GitHub Actions against Databricks, and can be scheduled with Databricks Jobs for production."

See **[docs/star_schema.png](docs/star_schema.png)** for the dimensional model diagram, and **[docs/analysis_notebook.ipynb](docs/analysis_notebook.ipynb)** for example business-question queries against the gold layer.

**Production tip:** When raw data comes from a lake or warehouse instead of seeds, define **sources** in YAML and set **source freshness** so dbt can alert when data stops landing.

## Databricks workflow (DAB + native dbt task)

The pipeline is defined as a **Databricks Asset Bundle** so job and resources deploy from the repo.

- **`databricks.yml`** — bundle root: bundle name, variables (`warehouse_id`, `catalog`), and `dev`/`prod` targets.
- **`resources/DBT_automation_job _from_code.yml`** — job `dbt_databricks_job` with one **dbt task**: `project_directory` = deployed bundle root, `warehouse_id`/`catalog` from bundle variables, commands `dbt deps`, `dbt seed`, `dbt snapshot`, `dbt build --select "{{job.parameters.dbt_select}}"`, environment `dbt-default` (dbt-databricks ≥1.0, &lt;2.0), and email notifications on success/failure.
  - `dbt_select` is a **job parameter** (default `fqn:*`, meaning "build everything"). The `deploy-bundle-prod.yml` workflow computes and passes a narrower value automatically on push to `main` — see the CI/CD section above.

**Push to `dev`** deploys only; **push to `main`** deploys and auto-runs the job (see CI/CD section above). To trigger it yourself — e.g. to test a specific model without waiting on a push — use the Databricks Jobs UI ("Run now with different parameters") or the CLI:

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
databricks bundle run dbt_databricks_job -t dev    # or -t prod

# Or target a specific model/seed/test instead of a full build:
databricks bundle run dbt_databricks_job -t dev --params dbt_select="stg_orders+"
```

### Data quality: null percentage check

After the dbt task runs, a second task (`null_percentage_check`) runs `scripts/check_null_percentage.py`, which computes the null/None percentage per column for a config-driven list of tables:

- **`seeds/dq_tables_config.csv`** — one row per table to check (`catalog`, `schema_name`, `table_name`, `enabled`). Lists the model-layer tables (`stg_orders`, `stg_order_items`, `stg_products`, `int_orders_enriched`, `int_order_items_with_product`, `fct_orders`, `fct_order_items`, `dim_products`, `scd_customers`). Loaded via `dbt seed` into `<catalog>.config.dq_tables_config`. Add or disable tables by editing this CSV.
- **`<catalog>.config.dq_null_check_results`** — results table (created automatically on first run). One row per `(run, table, column)` with `total_rows`, `null_count`, `null_pct`, so history accumulates across runs.
- The script also prints a summary table to the job's logs.

It runs two ways:

- **As a job task** (already wired in): uses the native Spark session on the job's compute — no credentials needed.
- **Standalone** (locally or in CI): uses `databricks-sql-connector` with the same `DATABRICKS_HOST` / `DATABRICKS_HTTP_PATH` / `DATABRICKS_TOKEN` env vars as the rest of this project:
  ```bash
  export DATABRICKS_HOST="dbc-xxxxxxxx-xxxx.cloud.databricks.com"
  export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/xxxxx"
  export DATABRICKS_TOKEN="dapi..."
  python scripts/check_null_percentage.py
  ```