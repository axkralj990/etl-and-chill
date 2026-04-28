# Pipeline Extensions (What to Learn Next)

This doc lists practical next upgrades, with alternatives, pros/cons, and what is likely best for this app.

## 1) Observability upgrade: OpenTelemetry + Grafana

### Current state
- Logs are structured (`structlog`) and pipeline run metadata is stored in DuckDB (`pipeline_runs`).
- Good baseline, but limited distributed tracing and metric dashboards.

### Proposed extension

```text
App / Pipeline
   |  (OTel SDK)
   v
OTel Collector (optional)
   |--> traces -> Tempo / Jaeger
   |--> metrics -> Prometheus
   |--> logs -> Loki
                     |
                  Grafana
```

### Benefits
- End-to-end trace of sync jobs (which endpoint was slow, where failed).
- Time-series dashboards for run duration, rows ingested, failure count.
- Better on-call style debugging even for small systems.

### Costs
- More moving parts and setup overhead.
- Can be overkill if one user and low change rate.

### Recommendation for this app
- Start lightweight:
  - keep structlog
  - add a few numeric counters in `pipeline_runs`
  - optionally add Prometheus exporter later
- Move to full OTel + Grafana when you want production-grade telemetry practice.

## 2) Scheduler/orchestrator choices: Cron vs Prefect vs Airflow

### Cron (current)
Pros:
- Minimal setup
- Perfect for simple fixed schedules (09:00, 22:00)
- Very low operational burden

Cons:
- Weak dependency management and retries across many tasks
- Limited observability/UI unless you build it

### Prefect
Pros:
- Modern Python-first orchestration
- Good local-to-cloud path
- Easier than Airflow for many small/medium projects

Cons:
- Additional service/platform concepts to learn
- More complex than cron for simple pipelines

### Airflow
Pros:
- Industry-standard DAG orchestrator
- Strong ecosystem and scheduling flexibility

Cons:
- Heavy operational footprint
- Higher learning and maintenance overhead

### Recommendation for this app
- Stay with **cron now** (best simplicity/fit).
- Evaluate **Prefect first** if pipeline branches/retries/SLAs grow.
- Use **Airflow** mainly if you want explicit Airflow experience or multi-pipeline org scale.

## 3) Data marts vs current online transformation

### Current pattern
- Transformations are materialized into `daily_features` (serving table).
- Dashboard reads this table directly.

This is already a small **mart-like serving layer**.

### When to add separate marts
- You introduce many subject areas (sleep mart, workout mart, stress mart).
- Different consumers require different grain/aggregation.
- Query performance or semantic complexity becomes hard to manage in one table.

### Potential mart split

```text
canonical_* -> feature build ->
  mart_daily_health
  mart_workouts
  mart_goals_progress
  mart_bayes_inputs
```

### Pros/cons
Pros:
- Better domain boundaries
- Easier testing and ownership
- Faster, simpler downstream queries

Cons:
- More ETL code and schema management
- Higher migration overhead for small projects

### Recommendation for this app
- Keep single serving table for now.
- Add marts only when dashboard logic becomes difficult to evolve safely.

## 4) Additional extension ideas

### a) Data quality framework
- Add explicit expectation checks (e.g., Great Expectations or custom checks table).
- Track pass/fail trends per run.

### b) Backfill/replay strategy
- Add "rebuild from raw_records" command to regenerate canonical + features deterministically.

### c) Contract tests for sources
- Add schema contract tests for Notion/Oura payload drift.

### d) Semantic layer
- Add a thin semantic metrics module (definitions for readiness trend, goal attainment, etc.).

## 5) Learning progression path (practical)

1. Master current ETL boundaries (E/T/L + serving layer).
2. Add observability metrics/traces incrementally.
3. Add one new source with canonical model + tests.
4. Introduce one extra mart and compare complexity/performance.
5. Migrate scheduler from cron -> Prefect (optional learning milestone).

This path teaches patterns without jumping too quickly into heavyweight tooling.
