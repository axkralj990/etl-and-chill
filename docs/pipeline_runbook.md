# Pipeline Runbook

## Initial setup

1. Install dependencies:

```bash
uv sync
```

2. Prepare `.env`:

```bash
cp .env.example .env
```

3. Complete Notion and Oura settings.

## Recommended first run sequence

1. OAuth bootstrap (see `docs/oura_oauth.md`)
2. Legacy backfill:

```bash
uv run life backfill-legacy
```

3. Bridge from legacy end to today:

```bash
uv run life bridge-to-today
```

4. Ongoing incremental sync:

```bash
uv run life sync-incremental
```

## Runtime configuration

Tune `config/pipeline.toml`:

- Oura endpoint list
- Incremental lookback window
- Bridge fallback window

## Scheduling

Run `sync-incremental` via scheduler (cron/launchd).

Example (every 4 hours):

```cron
0 */4 * * * cd /Users/aleksijkraljic/Desktop/repos/personal/life && /usr/bin/env uv run life sync-incremental >> logs/sync.log 2>&1
```

## Validation

Before real fetches or after changes:

```bash
uv run ruff check .
uv run pytest -q
```
