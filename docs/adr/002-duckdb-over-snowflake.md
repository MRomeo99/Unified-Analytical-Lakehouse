# ADR 002 — DuckDB over Snowflake/BigQuery

## Status
Accepted

## Context
Beacon Lakehouse needed an analytical query engine that satisfies four simultaneous constraints: it must be free with no cloud account required, it must read Delta Lake tables natively without a conversion step, it must run embedded (no server process to manage locally), and the SQL written against it must be syntactically identical to what would execute in a production cloud warehouse so that the portfolio demonstrates real, transferable skills. Snowflake and BigQuery satisfy the last constraint but violate the first two; SQLite satisfies the first and third but violates the second and fourth.

## Decision
We use **DuckDB** as the embedded columnar OLAP engine. The `dbt-duckdb` adapter connects dbt models directly to DuckDB. DuckDB reads Delta files from MinIO via its `httpfs` and `delta` extensions, so Bronze-layer tables are queryable without copying or converting data. All dbt staging and mart SQL (`transform/beacon/models/`) is standard SQL that runs identically on dbt-snowflake or dbt-bigquery without modification. DuckDB is invoked in-process by dbt and by the FastAPI serving layer; there is no separate database server.

## Tradeoffs
DuckDB is a single-node engine: it cannot scale horizontally, does not support concurrent write transactions from multiple processes, and has no built-in role-based access control or multi-user query history. Snowflake and BigQuery both support hundreds of concurrent users, petabyte-scale data, virtual warehouse autoscaling, and enterprise governance features (column-level security, query auditing, data sharing). For a portfolio project with one developer and synthetic data, none of those capabilities are needed, but a production deployment would hit DuckDB's concurrency ceiling quickly under real user load.

## Swap path to production
1. In `transform/beacon/profiles.yml`, change the active target from `dbt-duckdb` to `dbt-snowflake` (or `dbt-bigquery`) and supply the appropriate connection credentials via environment variables.
2. Set `AWS_ENDPOINT_URL` to point at real AWS S3 or GCS instead of the local MinIO endpoint — Delta files are format-compatible and require no conversion.
3. Install the matching dbt adapter package (`dbt-snowflake` or `dbt-bigquery`) in place of `dbt-duckdb`.
4. All model SQL in `models/staging/` and `models/marts/` is unchanged. All dbt tests, contracts, and source freshness checks are unchanged.
5. The FastAPI serving layer swaps its DuckDB in-process connection for a Snowflake connector or BigQuery client; the query text is unchanged.
