[![CI](https://github.com/MRomeo99/Unified-Analytical-Lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/MRomeo99/Unified-Analytical-Lakehouse/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11-blue)
![dbt](https://img.shields.io/badge/dbt-1.8-orange)
![Dagster](https://img.shields.io/badge/dagster-1.7-purple)
![License](https://img.shields.io/badge/license-MIT-green)

# Beacon Lakehouse

Production-grade unified analytical lakehouse for local-business marketing data — demonstrating the full AI data engineering stack from multi-source ingestion to a low-latency AI agent context API.

---

## Architecture

```
Postgres (clients/leads/appointments)
  SEO REST API (keyword rankings)          dlt ingestion
  Ad Spend CSV (daily channel spend)   ────────────────▶  Delta Lake on MinIO
                                                              (Bronze)
                                                                │
                                                           dbt-duckdb
                                                                │
                                                              DuckDB
                                                           stg_* models
                                                              (Silver)
                                                                │
                                                           dbt-duckdb
                                                                │
                                                              DuckDB
                                                          dim_* / fct_*
                                                              (Gold)
                                                                │
                                                            FastAPI
                                                                │
                                                   GET /context/{client_id}
                                                       (AI Agent API)
```

Orchestrated by **Dagster** (Software-Defined Assets with FreshnessPolicies).
Quality-gated by **Great Expectations** checkpoints.

For the reasoning behind every major structural choice — medallion vs. alternatives, batch vs. streaming, star schema design, the serving layer as an AI agent interface, and more — see [**docs/design-decisions.md**](docs/design-decisions.md). Individual tool-level decisions (dlt vs. Fivetran, DuckDB vs. Snowflake, etc.) each have an ADR in [`docs/adr/`](docs/adr/).

---

## What this demonstrates

- **Lakehouse architecture** — Delta Lake tables (ACID, time travel) on S3-compatible object storage
- **Medallion pipeline** — Bronze (raw) → Silver (typed/deduplicated) → Gold (business-ready)
- **Data contracts** — dbt model contracts enforced on all Gold marts; CI fails on violation
- **SLAs for freshness** — Dagster FreshnessPolicies with 25-hour Silver / 26-hour Gold thresholds
- **dbt at scale** — surrogate keys, CTEs, generic tests, source freshness, full column documentation
- **Data quality gating** — Great Expectations checkpoint blocks CI on expectation failure
- **AI-agent data access** — `GET /context/{client_id}` returns structured JSON for context injection
- **Multi-source ingestion** — operational DB + REST API + flat file, all via dlt
- **Reproducible synthetic data** — `faker` + `random.seed(42)` for deterministic test data
- **Production swap path** — every local tool maps to a named production equivalent

---

## Quick start

```bash
make up        # Start Postgres, MinIO, and Dagster (Docker)
make seed      # Generate 500 clients, 25k+ leads, 2.7M ad spend rows
make pipeline  # Ingest → Transform → Quality gate (full end-to-end)
```

Then open:
- **Dagster UI**: http://localhost:3000
- **MinIO console**: http://localhost:9001
- **API docs**: http://localhost:8000/docs (after `make serve`)

---

## Data sources

| Source | Type | Tool | Represents |
|---|---|---|---|
| Postgres | Operational DB | dlt + psycopg2 | 500 clients, ~25k leads, appointments |
| SEO JSON | REST API fixture | dlt filesystem | 260,000 weekly keyword rankings |
| Ad spend CSV | Flat file | dlt CSV | 547,500 daily channel spend rows |

---

## Medallion layers

| Layer | Tool | Tables | Rows (after seed) |
|---|---|---|---|
| Bronze | Delta Lake on MinIO | `raw_clients`, `raw_leads`, `raw_appointments`, `raw_seo_rankings`, `raw_ad_spend` | ~835k |
| Silver | DuckDB (dbt views) | `stg_clients`, `stg_leads`, `stg_appointments`, `stg_seo_rankings`, `stg_ad_spend` | ~835k |
| Gold | DuckDB (dbt tables) | `dim_clients`, `fct_leads`, `fct_ad_performance`, `fct_appointments` | ~573k |

---

## Data contracts

dbt model contracts (`transform/beacon/contracts/schema.yml`) enforce column names and
data types on all four Gold marts. If a model is modified to drop a required column,
`dbt run` exits non-zero and CI fails.

**To see the gate in action:**
```bash
pytest tests/test_contract_breaking.py -m integration -v
```
This test temporarily removes `client_name` from `dim_clients`, runs dbt, asserts the
failure, then restores the model. The restore is guaranteed by a `try/finally` block.

---

## SLAs & freshness

Dagster `FreshnessPolicy` is set on every Silver and Gold asset:

| Layer | Maximum lag | Alert |
|---|---|---|
| Silver | 25 hours | Asset turns yellow in Dagster UI |
| Gold | 26 hours | Asset turns yellow in Dagster UI |
| SEO (weekly source) | 14 days | Extended SLA matches source cadence |

The Dagster asset graph visually shows green (fresh) / yellow (stale) / red (failed)
for each layer. A daily schedule runs at 02:00 UTC to maintain freshness.

---

## Serving layer

```
GET /context/{client_id}
```

**Example request:**
```bash
curl http://localhost:8000/context/1
```

**Example response:**
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
    {"keyword": "legal near me", "position": 8},
    {"keyword": "best legal in Chicago", "position": 12}
  ],
  "monthly_spend_usd": 4234.50,
  "avg_cpl": 18.75,
  "last_updated": "2024-01-15T10:30:00"
}
```

**Measured latency** (DuckDB in-process, Gold tables in memory):

| Percentile | Latency |
|---|---|
| p50 | ~8ms |
| p99 | ~42ms |

> Measured with `locust` at 50 concurrent users on an M1 MacBook Pro.
> Run `locust -f locustfile.py --headless -u 50 -r 10 --run-time 60s` to reproduce.

---

## Production swap path

| Local | Production | What changes |
|---|---|---|
| MinIO | AWS S3 / GCS | `AWS_ENDPOINT_URL` env var only |
| Postgres (Docker) | RDS / Cloud SQL | `POSTGRES_HOST` env var |
| delta-rs | Databricks Delta / Spark | Replace dlt destination config |
| DuckDB | Snowflake / BigQuery | `profiles.yml` target only; SQL unchanged |
| dbt-duckdb | dbt-snowflake / dbt-bigquery | `pip install dbt-snowflake` + profiles.yml |
| Dagster (local) | Dagster Cloud / Astronomer | Docker → managed deployment |
| Great Expectations | Soda Core / Monte Carlo | Swap GX checkpoints for Soda scans |
| FastAPI (local) | Cloud Run / ECS / Lambda | `docker build` → cloud deploy |

---

## Project structure

```
beacon-lakehouse/
├── seeds/                        # Synthetic data generators
├── ingestion/                    # dlt pipelines (Bronze)
│   └── sources/
├── transform/beacon/             # dbt project (Silver + Gold)
│   ├── models/staging/           # stg_*.sql
│   ├── models/marts/             # dim_*.sql, fct_*.sql
│   └── contracts/                # Model contracts (schema.yml)
├── orchestration/                # Dagster assets, checks, schedules
├── quality/                      # Great Expectations
├── serving/                      # FastAPI
├── tests/                        # pytest (unit + integration)
├── docs/adr/                     # Architecture Decision Records
├── .github/workflows/ci.yml      # GitHub Actions CI
├── docker-compose.yml
├── Makefile
└── pyproject.toml
```

---

## License

MIT — see [LICENSE](LICENSE).
