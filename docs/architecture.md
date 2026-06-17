# Beacon Lakehouse — Architecture

## Overview

Beacon Lakehouse is a production-grade, locally-runnable unified analytical
lakehouse. It implements the medallion architecture (Bronze → Silver → Gold)
for a local-business marketing analytics domain, and exposes a low-latency
context API designed for AI agent consumption.

---

## Medallion Architecture

```
Postgres (operational)     REST API (SEO)     CSV (Ad Spend)
        │                       │                   │
        └───────────────────────┼───────────────────┘
                                │  dlt (ingestion)
                                ▼
                    ┌─────────────────────┐
                    │  BRONZE LAYER        │
                    │  Delta Lake on MinIO │
                    │  (append-only, raw)  │
                    └──────────┬──────────┘
                               │  dbt-duckdb (staging)
                               ▼
                    ┌─────────────────────┐
                    │  SILVER LAYER        │
                    │  DuckDB (typed,      │
                    │  deduplicated,       │
                    │  surrogate keys)     │
                    └──────────┬──────────┘
                               │  dbt-duckdb (marts)
                               ▼
                    ┌─────────────────────┐
                    │  GOLD LAYER          │
                    │  DuckDB (business    │
                    │  logic, contracts,   │
                    │  aggregations)       │
                    └──────────┬──────────┘
                               │  FastAPI (serving)
                               ▼
                    ┌─────────────────────┐
                    │  SERVING LAYER       │
                    │  GET /context/{id}   │
                    │  p99 < 50ms          │
                    └─────────────────────┘
```

---

## Data Sources

| Source | Type | Tool | Represents |
|---|---|---|---|
| Postgres | Operational DB | psycopg2 + dlt | Clients, leads, appointments |
| SEO REST API | JSON fixture | dlt filesystem source | Weekly keyword rankings |
| Ad spend CSV | Flat file | dlt CSV source | Daily channel spend |

---

## Stack Decisions

See `docs/adr/` for full rationale on each choice.

| Layer | Tool | Production Equivalent |
|---|---|---|
| Ingestion | dlt | Fivetran / Airbyte + dlt |
| Table format | Delta Lake (delta-rs) | Databricks Delta / Apache Iceberg |
| Object storage | MinIO | AWS S3 / GCS |
| Operational DB | Postgres (Docker) | Any production RDBMS |
| Analytical engine | DuckDB | Snowflake / BigQuery |
| Transforms | dbt-duckdb | dbt-snowflake / dbt-bigquery |
| Orchestration | Dagster | Dagster Cloud / Apache Airflow |
| Data quality | Great Expectations | Soda Core / Monte Carlo |
| Serving | FastAPI | FastAPI on Cloud Run / ECS |

---

## Data Flows

### Bronze (Ingestion)

1. `make seed` runs three generators:
   - `seeds/generate_postgres.py` → loads 500 clients + leads + appointments into Postgres
   - `seeds/generate_seo_api.py` → writes `data/dlt/seo_fixtures/seo_rankings.json`
   - `seeds/generate_ad_spend.py` → writes `data/dlt/ad_spend/ad_spend.csv`

2. `make ingest` runs `ingestion/pipelines.py`, which calls three dlt pipelines:
   - Postgres source → Delta tables: `raw_clients`, `raw_leads`, `raw_appointments`
   - SEO JSON source → Delta table: `raw_seo_rankings`
   - CSV source → Delta table: `raw_ad_spend`
   - All Delta tables land in MinIO bucket `beacon-bronze/`

### Silver (Staging)

`dbt run --select staging` materializes five views in DuckDB:

| Model | Key transformations |
|---|---|
| `stg_clients` | Surrogate key, cast date, deduplicate by `_loaded_at` |
| `stg_leads` | ROW_NUMBER dedup, cast decimal value, lower-case enums |
| `stg_appointments` | Surrogate key, cast timestamp |
| `stg_seo_rankings` | Composite surrogate key, cast date |
| `stg_ad_spend` | Composite surrogate key, cast numerics, dedup |

### Gold (Marts)

`dbt run --select marts` materializes four tables in DuckDB with enforced contracts:

| Model | Grain | Key additions |
|---|---|---|
| `dim_clients` | 1 per client | `days_active`, `lead_conversion_rate` |
| `fct_leads` | 1 per lead | `is_converted`, `has_appointment`, `age_days` |
| `fct_ad_performance` | client×channel×day | `ctr`, `cpc`, `cpl` |
| `fct_appointments` | 1 per appointment | `days_to_appointment`, `is_completed` |

---

## Orchestration (Dagster)

Assets are organized in three groups:

```
bronze group → silver group → gold group
(dlt)           (dbt staging)   (dbt marts)
```

SLA enforcement via `FreshnessPolicy`:
- Silver assets: must refresh within **25 hours**
- Gold assets: must refresh within **26 hours**

`AssetChecks` validate row counts and schema after each Gold materialization.

---

## Data Quality (Great Expectations)

Expectation suites run against Silver (DuckDB) tables:
- `stg_clients_suite`: 500 ± 2% rows, valid industry/plan values
- `stg_leads_suite`: 17,500–32,500 rows, valid source/status enums

The GX checkpoint is wired into CI — a failure blocks the pipeline badge.

---

## Serving Layer

`GET /context/{client_id}` returns:

```json
{
  "client_id": 1,
  "client_name": "Apex Legal Group",
  "industry": "legal",
  "plan_tier": "pro",
  "days_active": 165,
  "total_leads": 52,
  "converted_leads": 12,
  "lead_conversion_rate": 0.2308,
  "total_appointments": 10,
  "top_keywords": [
    {"keyword": "legal near me", "position": 8}
  ],
  "monthly_spend_usd": 4234.5,
  "avg_cpl": 18.75,
  "last_updated": "2024-01-15T10:30:00"
}
```

Target latency: p50 < 10ms, p99 < 50ms (DuckDB in-process query).

---

## CI/CD Pipeline

GitHub Actions (`.github/workflows/ci.yml`) runs in order:

1. `ruff check` + `black --check`
2. `dbt compile` (SQL syntax gate)
3. `dbt run` + `dbt test` (schema contract gate)
4. Great Expectations checkpoint (data quality gate)
5. `pytest` (unit + integration)
6. `docker build` (Dockerfile validity)

---

## Local Development

```
make up        # Start Postgres + MinIO + Dagster
make seed      # Generate synthetic data
make ingest    # Run dlt Bronze pipelines
make transform # Run dbt Silver + Gold
make quality   # Run GX checkpoint
make serve     # Start FastAPI on :8000
make test      # Run pytest
```
