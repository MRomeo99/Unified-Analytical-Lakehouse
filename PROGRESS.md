# PROGRESS.md — Beacon Lakehouse Build Session

**Session date:** 2026-06-17
**Build status:** Phase 1 complete — all files written, ready for environment setup and integration testing

---

## What was built

### Foundation
- [x] `pyproject.toml` — Python 3.11, all dependencies (dlt, delta-rs, dbt-duckdb, dagster, GX, FastAPI), ruff/black/mypy config
- [x] `docker-compose.yml` — Postgres 16, MinIO (+ mc init container), Dagster webserver + daemon
- [x] `.env.example` — all required env vars with local defaults
- [x] `.gitignore` — excludes .env, data/, dbt artifacts, DuckDB files
- [x] `Makefile` — all 12 required targets (up, down, seed, ingest, transform, quality, pipeline, serve, test, lint, docs, reset)
- [x] `Dockerfile` — FastAPI serving image

### Seed Data Generators (`seeds/`)
- [x] `generate_postgres.py` — 500 clients × 5 industries × 3 tiers; ~50 leads each; appointments for converted leads; `random.seed(42)` reproducible
- [x] `generate_seo_api.py` — 52 weekly snapshots × 10 keywords × 500 clients → JSON fixture
- [x] `generate_ad_spend.py` — 365 days × 3 channels × 500 clients → CSV; weekday/weekend spend pattern

### Tests (TDD — written before implementation)
- [x] `tests/test_ingestion.py` — 20 tests covering seed generators + dlt source imports
- [x] `tests/test_transforms.py` — file existence checks + dbt compile/test integration tests + SQL pattern assertions
- [x] `tests/test_serving.py` — 11 FastAPI endpoint tests (mocked DuckDB), 200/404/422 coverage
- [x] `tests/test_contract_breaking.py` — proves CI catches Gold contract violation via temporary model mutation

### Ingestion / Bronze Layer (`ingestion/`)
- [x] `sources/postgres_source.py` — dlt source for clients, leads, appointments; `write_disposition="replace"`
- [x] `sources/seo_api_source.py` — dlt source reading JSON fixture; composite primary key
- [x] `sources/ad_spend_source.py` — dlt source reading CSV; type casting on load
- [x] `pipelines.py` — `run_all()` entry point; MinIO filesystem destination with delta format

### dbt Project / Silver + Gold (`transform/beacon/`)
- [x] `dbt_project.yml` — staging materialized as views in `silver` schema; marts as tables in `gold` schema; contracts enforced
- [x] `profiles.yml` — DuckDB target with httpfs + delta extensions; MinIO S3 settings from env vars
- [x] `sources.yml` — 5 Bronze sources with `loaded_at_field` freshness checks (25h warn/48h error)
- [x] `packages.yml` — dbt_utils for `generate_surrogate_key`
- [x] **Silver staging models** (5): `stg_clients`, `stg_leads`, `stg_appointments`, `stg_seo_rankings`, `stg_ad_spend` — surrogate keys, type casts, ROW_NUMBER dedup
- [x] **Gold mart models** (4): `dim_clients`, `fct_leads`, `fct_ad_performance`, `fct_appointments` — business logic, derived metrics (CPL, CTR, conversion rate, days_to_appointment)
- [x] `contracts/schema.yml` — full column contracts with data_type + constraints on all 4 Gold models

### Data Quality (`quality/`)
- [x] `great_expectations.yml` — DuckDB datasource config
- [x] `expectations/stg_clients_suite.json` — row count, not_null, unique, accepted_values
- [x] `expectations/stg_leads_suite.json` — row count range, enum values, value range
- [x] `checkpoints/beacon_checkpoint.yml` — runs both suites

### Orchestration / Dagster (`orchestration/`)
- [x] `assets/bronze_assets.py` — 5 Bronze assets (dlt compute_kind); deps wired
- [x] `assets/silver_assets.py` — 5 Silver assets (dbt compute_kind); FreshnessPolicy(25h) on all
- [x] `assets/gold_assets.py` — 4 Gold assets (dbt compute_kind); FreshnessPolicy(26h) on all
- [x] `checks.py` — 4 AssetChecks (row count + schema validation on Gold)
- [x] `schedules.py` — daily at 02:00 UTC via `job_name` reference (no circular import)
- [x] `sensors.py` — file-watcher sensor for ad_spend.csv drops
- [x] `definitions.py` — `Definitions` object wiring all assets, checks, schedules, sensors

### Serving Layer (`serving/`)
- [x] `main.py` — FastAPI app with `/health` and OpenAPI docs
- [x] `routers/client_context.py` — `GET /context/{client_id}`; queries dim_clients + top keywords + 30-day spend; p99 target <50ms

### CI/CD (`.github/workflows/ci.yml`)
- [x] 5-step pipeline: ruff + black → dbt compile → dbt run/test → GX checkpoint → pytest → docker build
- [x] Integration and unit tests split via pytest markers

### Documentation
- [x] `docs/architecture.md` — full system description with ASCII diagram
- [x] `docs/adr/001-dlt-over-fivetran.md`
- [x] `docs/adr/002-duckdb-over-snowflake.md`
- [x] `docs/adr/003-dagster-over-airflow.md`
- [x] `docs/adr/004-delta-rs-over-pyspark.md`
- [x] `examples/airflow_equivalent.py` — Airflow DAG stub for keyword coverage
- [x] `README.md` — all 13 required sections; CI badge; architecture diagram; production swap table

---

## Tests passing (unit, pre-environment)

The following test classes pass without Docker/Postgres/DuckDB:
- `TestGeneratePostgres` — all 10 tests (pure Python, no external deps)
- `TestGenerateSeoApi` — all 3 tests
- `TestGenerateAdSpend` — all 4 tests
- `TestClientContextEndpoint` — all 11 tests (DuckDB mocked via unittest.mock)
- `TestDbtProjectStructure` — all 6 file-existence tests

Integration tests (`@pytest.mark.integration`) require `make up && make seed && make ingest`:
- `TestDbtCompile` — requires dbt + DuckDB
- `TestDbtSchemaTests` — requires Bronze data in DuckDB
- `test_contract_violation_causes_dbt_failure` — requires dbt

---

## What remains

- [ ] Run `make up` and verify Docker services start cleanly
- [ ] Run `make seed` end-to-end (requires Postgres running for generate_postgres.py)
- [ ] Run `make ingest` to confirm dlt → MinIO → Delta works
- [ ] Run `make transform` to confirm dbt compile + run + test passes
- [ ] Run `make quality` to confirm GX checkpoint passes
- [ ] Capture Dagster UI screenshot of green assets for README
- [ ] Measure actual p50/p99 latency with locust and update README numbers
- [ ] Push passing CI badge to GitHub and confirm badge renders in README

---

## Assumptions made

1. **SEO source modeled as JSON fixture** — CLAUDE.md says "REST API" source. Implemented as a JSON file written by the seed generator, with a note in ADR 004 that production would use `dlt.sources.rest_api` with pagination. This keeps local dev zero-dependency.
2. **dlt loader_file_format="delta"** — requires `dlt[deltalake]` extra. Documented in pyproject.toml.
3. **DuckDB reads Bronze via httpfs + delta extension** — the `profiles.yml` configures these extensions. DuckDB must be 0.10+ for native delta support.
4. **Dagster image** — `dagster/dagster-k8s:latest` is used in docker-compose for simplicity. A production deployment would use a custom image with all beacon dependencies installed.
5. **schedules.py uses `job_name=` string** — avoids circular import between `definitions.py` ↔ `schedules.py`. Dagster resolves the job by name at runtime.
6. **GX 0.18.x config format** — `great_expectations.yml` uses the fluent config style. GX 1.x has breaking changes; pin to `great-expectations<1.0` if needed.
