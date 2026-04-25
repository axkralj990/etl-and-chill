## etl-and-chill

A personal health and life analytics ETL pipeline that ingests Notion and Oura data into DuckDB, builds derived features, and powers an interactive dashboard for daily, weekly, monthly, and yearly insights.

### Features

- Notion daily log ingestion and normalization
- Oura v2 daily endpoint ingestion (non-intraday)
- Oura v2 sleep + daily metrics (steps, sleep durations, HR/HRV, temperature deviation)
- Legacy one-time backfill support
- DuckDB storage with raw/canonical/features layers
- Derivative fields (rolling means, score deltas, stress flags)
- Structured JSON logging for easy querying
- Modular extension points for OpenAI tagging and cmdstanpy inference
- Oura OAuth helper commands + automatic token refresh
- Streamlit dashboard with Home, Explore, Trends, Sleep, and Summary tabs

### Setup

1. Use Python 3.12
2. Install dependencies

```bash
uv sync
```

3. Create environment file

```bash
cp .env.example .env
```

4. Fill secrets in `.env`:

- `NOTION_TOKEN`
- `NOTION_DATABASE_ID`
- `OURA_ACCESS_TOKEN`

OAuth setup guide:

- `docs/oura_oauth.md`

Pipeline runbook:

- `docs/pipeline_runbook.md`

5. Tune runtime pipeline config in `config/pipeline.toml`:

- Oura endpoint list
- incremental lookback/fallback windows
- bridge fallback window

### Commands

- One-time legacy backfill

```bash
uv run life backfill-legacy
```

- One-time bridge from end of legacy to today

```bash
uv run life bridge-to-today
```

- Incremental sync (periodic)

```bash
uv run life sync-incremental
```

- Oura OAuth authorize URL

```bash
uv run life oura-oauth-url --state "life-init"
```

- Oura OAuth exchange code and save tokens

```bash
uv run life oura-oauth-exchange --redirect-url "<redirect-url>" --save
```

- Oura OAuth status (safe, no secrets printed)

```bash
uv run life oura-oauth-status
```

- Dashboard

```bash
uv run streamlit run src/life/dashboard/app.py
```

Dashboard tabs:

- Home (today + 7-day overview)
- Explore (bubble chart)
- Trends (day/week/month/year)
- Period Summary (weekly/monthly/yearly aggregation)
- Correlations & Lags
- Sleep + Recovery
- Data Quality

### Storage

DuckDB file is configured with `DUCKDB_PATH` (default `data/life.duckdb`).

Core tables:

- `raw_records`
- `canonical_notion_daily`
- `canonical_oura_daily`
- `daily_features`
- `sync_state`
- `pipeline_runs`
