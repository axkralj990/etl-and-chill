# ETL Architecture (Learning Guide)

This doc explains the pipeline as **E/T/L**, where each step happens in this repo, and how the dashboard is decoupled.

## 1) End-to-End Data Flow

```text
Notion API          Oura API            Legacy CSVs
   |                   |                    |
   +--------- Extract (E) ------------------+
                      |
                      v
                raw_records (DuckDB)
                      |
          Normalize + merge + map (T)
          - canonical_notion_daily
          - canonical_oura_daily
                      |
                      v
              build_daily_features (T)
                      |
                      v
                daily_features (serving)
                      |
                      v
                Streamlit dashboard
```

## 2) Where E, T, and L happen

### Extract (E)
- Connectors call source APIs/files and fetch raw payloads.
- Key modules:
  - `src/life/connectors/notion.py`
  - `src/life/connectors/oura.py`
  - legacy parsers in `src/life/pipeline/shared.py`

### Transform (T)
- Raw payloads are normalized into canonical rows.
- Business mapping is done in normalizers (labels -> scores, workout parsing, etc.).
- Key modules:
  - `src/life/normalizers/notion_daily.py`
  - `src/life/normalizers/oura_daily.py`
- Additional transformation creates derived metrics and rolling windows in `build_daily_features()`.

### Load (L)
- Data is written to DuckDB tables via storage layer.
- Key module:
  - `src/life/storage/duckdb.py`
- Loads occur in stages:
  - raw -> `raw_records`
  - canonical -> `canonical_*`
  - serving -> `daily_features`

## 3) Pipeline orchestration

```text
life CLI command
  -> pipeline mode runner
    -> extract functions
    -> normalize
    -> upsert canonical tables
    -> build_daily_features
    -> update sync_state + pipeline_runs
```

Entry points:
- `uv run life backfill-legacy`
- `uv run life bridge-to-today`
- `uv run life sync-incremental`

Orchestration modules:
- `src/life/cli.py`
- `src/life/pipeline/backfill_legacy.py`
- `src/life/pipeline/bridge_to_today.py`
- `src/life/pipeline/sync_incremental.py`
- shared steps in `src/life/pipeline/shared.py`

## 4) Decoupling: Dashboard vs Ingestion

The dashboard does **not** call Notion/Oura directly.

```text
Ingestion pipeline writes DuckDB  --->  Dashboard reads DuckDB only
```

- Dashboard read layer: `src/life/dashboard/data.py`
- UI layer: `src/life/dashboard/app.py`

Why this is good:
- UI stays fast and reproducible.
- API outages do not break chart rendering.
- You can rerun ingestion independently from analytics.

## 5) Storage layers and intent

```text
raw_records             = audit trail / source fidelity
canonical_*             = clean source-specific models
daily_features          = analytics-ready, joined/derived serving table
sync_state + pipeline_runs = orchestration metadata
```

Where transformed data lives:
- Normalized transforms live in `canonical_notion_daily` and `canonical_oura_daily`.
- Feature transforms live in `daily_features`.

So this project currently uses a **single serving table** model for BI/dashboard reads.

## 6) Why this design fits this app

For a personal analytics app, this design is usually the sweet spot:
- simple operational model
- low cost (DuckDB file)
- strong local portability
- enough structure to scale incrementally

Tradeoff:
- not a distributed warehouse architecture
- limited multi-user concurrency vs cloud OLAP systems

## 7) Nomenclature quick map

- **ELT vs ETL**: This project is mostly ETL-style (transform while normalizing + feature build before serving reads).
- **Canonical model**: Stable, cleaned source-specific schema.
- **Serving layer**: Table optimized for downstream reads (`daily_features`).
- **Idempotent load**: Upserts make reruns safe.
- **Orchestration metadata**: `sync_state`, `pipeline_runs`.

## 8) Why ETL here (and not ELT)

### Why ETL fits this project
- We run on a local/small footprint stack (DuckDB + Streamlit), not a large warehouse.
- Source payloads need parsing and normalization before they become useful (Notion labels, workout parsing, score mappings).
- The dashboard expects consistent typed fields; precomputing them keeps UI logic simple.
- For a learning/personal project, ETL gives clear boundaries and easier debugging.

### What ELT would look like
In ELT, you would:
1. Load mostly raw source data first (minimal shaping), then
2. Transform inside the warehouse/lakehouse with SQL/dbt/Spark models.

### Where ELT is usually better
- Cloud data warehouses (BigQuery, Snowflake, Redshift, Databricks SQL).
- Large datasets where warehouse compute elasticity is valuable.
- Many downstream consumers needing different marts/semantic layers.
- Teams using dbt for lineage, tests, and model versioning.

### Rule of thumb
- **ETL**: better for this app's current scale, simplicity, and local execution model.
- **ELT**: better once data volume, team size, model count, and governance needs grow.
