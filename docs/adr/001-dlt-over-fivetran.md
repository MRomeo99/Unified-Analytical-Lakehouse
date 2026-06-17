# ADR 001 — dlt over Fivetran/Airbyte

## Status
Accepted

## Context
Beacon Lakehouse must run end-to-end on a developer's laptop with zero cloud accounts, zero paid services, and zero manual setup. The EL layer needed to handle multiple source types (Postgres operational DB, a REST API, and flat-file CSV exports), manage schema evolution as synthetic data changes, and support incremental loading so re-runs don't duplicate rows. Any managed EL platform (Fivetran, Airbyte Cloud) requires an account and internet connectivity, violating the self-contained local-dev constraint.

## Decision
We use **dlt (data load tool)** as the extraction and loading framework. Each source is implemented as a dlt source in `ingestion/sources/` (`postgres_source.py`, `seo_api_source.py`, `ad_spend_source.py`). The dlt filesystem destination with `delta` format writes ACID-compliant Delta Lake tables directly to MinIO (local S3). Incremental loading uses dlt's built-in cursor-based state tracking. Schema evolution is handled automatically by dlt's schema inference on each run.

## Tradeoffs
Fivetran and Airbyte Cloud each offer 300+ pre-built, battle-tested connectors with enterprise SLAs, automatic schema migration alerts, and zero connector maintenance. Choosing dlt means writing and maintaining connector code for every source. dlt's schema inference is automatic but less hardened at scale than managed platforms that have processed petabytes across thousands of customers. Debugging extraction failures requires Python skills rather than a managed UI with built-in alerting.

## Swap path to production
Replace the Postgres source (`postgres_source.py`) and ad spend CSV source (`ad_spend_source.py`) with managed Fivetran or Airbyte connectors that sync directly into cloud object storage or a cloud data warehouse. Retain dlt exclusively for custom REST API sources (e.g., `seo_api_source.py`) where managed connectors do not exist. The Delta Lake table format and MinIO destination code are replaced by the managed platform's native output format; the `ingestion/pipelines.py` entry point is retired in favour of the managed platform's scheduling UI.
