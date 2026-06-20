# Design Decisions — Beacon Lakehouse

**Perspective:** Senior AI Data Engineer  
**Scope:** Architectural choices, patterns, alternatives considered, and honest trade-off analysis across every major layer of the system.

> The individual tool-level choices (dlt vs Fivetran, DuckDB vs Snowflake, Dagster vs Airflow, delta-rs vs PySpark) each have a dedicated Architecture Decision Record in [`docs/adr/`](adr/). This document covers the *design patterns* and *structural decisions* that sit above individual tool selection — the "why did we build it this way" rather than "why did we pick this tool."

---

## Table of Contents

1. [Why medallion architecture — and what we gave up](#1-why-medallion-architecture--and-what-we-gave-up)
2. [Batch over streaming — a deliberate choice for this domain](#2-batch-over-streaming--a-deliberate-choice-for-this-domain)
3. [Data contracts at the Gold layer only](#3-data-contracts-at-the-gold-layer-only)
4. [The serving layer as an AI agent interface](#4-the-serving-layer-as-an-ai-agent-interface)
5. [dbt project structure: staging vs marts, and what each layer owns](#5-dbt-project-structure-staging-vs-marts-and-what-each-layer-owns)
6. [Surrogate keys via md5 rather than database sequences](#6-surrogate-keys-via-md5-rather-than-database-sequences)
7. [Star schema over wide denormalized tables](#7-star-schema-over-wide-denormalized-tables)
8. [CI gate ordering: why data quality runs after dbt, not before](#8-ci-gate-ordering-why-data-quality-runs-after-dbt-not-before)
9. [Great Expectations execution engine: PandasExecutionEngine over SqlAlchemy](#9-great-expectations-execution-engine-pandasexecutionengine-over-sqlalchemy)
10. [Synthetic data design: Faker + seed(42)](#10-synthetic-data-design-faker--seed42)
11. [Why Python end-to-end — no JVM, no Scala, no separate config language](#11-why-python-end-to-end--no-jvm-no-scala-no-separate-config-language)
12. [Partitioning strategy: by date at Bronze, none at Silver/Gold](#12-partitioning-strategy-by-date-at-bronze-none-at-silvergold)
13. [Source freshness as the SLA mechanism](#13-source-freshness-as-the-sla-mechanism)

---

## 1. Why medallion architecture — and what we gave up

### The decision

Three-layer medallion: Bronze (raw/append-only), Silver (typed, deduplicated, validated), Gold (business-ready marts). Each layer has a distinct responsibility and a clear promotion gate before data moves forward.

### Why not alternatives

**Lambda architecture (batch + streaming paths):**  
Lambda was considered because the job description language around "unified analytical layer" sometimes implies real-time capability. I rejected it. Lambda requires maintaining two codebases — a streaming path and a batch path — that produce semantically equivalent outputs, then merging them. The complexity is significant and the benefit only materialises when sub-hour latency matters. For this domain (daily ad spend, weekly SEO rankings, lead intake that converts over days), hour-old data is indistinguishable from second-old data for any business decision a marketer would make.

**Kappa architecture (streaming only, replay for batch):**  
Kappa collapses Lambda's two paths into one stream, solving the dual-codebase problem. The cost is that every query against historical data requires a replay, and the serving layer must handle out-of-order event semantics. For a portfolio that needs to be cloned and run in under 5 minutes, building a Kafka/Flink stack to demonstrate Kappa would be noise without signal. The hiring signal from a well-executed batch medallion with visible SLAs outweighs a half-built streaming pipeline.

**Wide flat tables (no medallion):**  
Many teams skip layering entirely and write denormalized "OBT" (one big table) models directly from raw sources. This works until: (a) a source schema changes and you have to unpick business logic from transformation logic across dozens of downstream tables, or (b) a new consumer needs a different grain. Medallion's forced separation of concerns — Silver never has business logic, Gold never has raw columns — is the constraint that makes models maintainable.

### What we gave up

Medallion adds latency between ingestion and query availability. A row that enters Bronze at 01:00 may not be queryable in Gold until the Silver and Gold dbt runs complete. For this domain that is acceptable. A streaming consumer would not accept it.

Medallion also creates storage redundancy: every row exists in Bronze (Delta), Silver (DuckDB view over Bronze), and Gold (DuckDB table materialised from Silver). For synthetic data at this scale the cost is trivial; at petabyte scale, storage costs require careful thought about which layers materialise vs. remain virtual.

---

## 2. Batch over streaming — a deliberate choice for this domain

### The decision

Every data source refreshes on a batch cadence. dlt pipelines run daily (scheduled by Dagster). There is no Kafka topic, no Flink job, no micro-batch window.

### Why batch fits this domain

| Source | Natural cadence | Fastest meaningful refresh |
|---|---|---|
| Postgres (clients/leads) | Event-driven, but decisions are daily | Daily |
| SEO rankings | Snapshot weekly by Google | Weekly |
| Ad spend | Settlement finalized end-of-day by platforms | Daily |
| Appointments | Calendar-driven, confirmed same-day | Daily |

A marketer reviewing campaign performance at 9am does not need data from 8:59am. They need yesterday's complete data. Streaming infrastructure built to deliver sub-second latency for daily-action decisions is engineering theatre.

### Where streaming would be the right answer

If the domain required real-time lead routing (a new lead triggers an immediate CRM task), fraud detection on ad spend, or minute-level campaign pacing decisions, the batch assumption would break. That is the correct time to introduce Kafka and Flink — when the business problem *requires* it, not because the stack looks more impressive.

### Alternatives considered

**Micro-batch (Spark Structured Streaming / Flink with 1-minute windows):**  
Delivers "near real-time" latency without full streaming semantics. Added complexity without reducing latency below what batch already provides for this domain.

**Change Data Capture (Debezium → Kafka):**  
Appropriate for operational databases where row-level changes need to stream out immediately. Overkill for a reporting pipeline where yesterday's snapshot is the correct grain.

---

## 3. Data contracts at the Gold layer only

### The decision

dbt model contracts (enforced column names and data types) are applied **only to Gold marts** — not to Silver staging models. Breaking a contract causes `dbt run` to exit non-zero, which fails CI.

### Why only Gold

Contracts are a promise to downstream consumers. Silver is an internal transformation layer — its schema is an implementation detail of the pipeline, not a public API. If the SEO source adds a new column, I want Silver to pick it up automatically (via `SELECT *` logic in staging models) without treating the addition as a breaking change.

Gold is where the downstream contract matters. The FastAPI `/context/{client_id}` endpoint, BI dashboards, and AI agent prompts all consume Gold. If `fct_leads` unexpectedly drops `lead_value`, every consumer breaks silently. The contract makes that break loud and immediate.

### Alternatives considered

**Contracts on every layer:**  
Applying contracts to Bronze and Silver would catch schema drift from sources earlier. The cost: every source schema change (a new Postgres column, an extra CSV field) becomes a deliberate contract migration instead of a transparent pass-through. For source-owned schemas you cannot control, this is more friction than benefit.

**No contracts; rely on tests:**  
Generic dbt tests (`not_null`, `unique`, `accepted_values`) catch data quality issues but do not enforce schema shape. A Gold model that drops a column still passes `dbt test` — the column-existence check must be explicit. Contracts provide that check as a first-class feature without writing custom singular tests.

**JSON Schema / Pydantic at the API layer:**  
The FastAPI endpoint validates response shape via Pydantic models. This catches the problem at the consumer boundary, not at the transformation boundary. By the time the API rejects a response, the bad model has already run successfully and potentially written incorrect data to downstream users who don't go through the API. Fail earlier.

---

## 4. The serving layer as an AI agent interface

### The decision

The FastAPI endpoint `GET /context/{client_id}` returns a single structured JSON object designed for direct injection into an LLM prompt or agent context window. It is intentionally narrow: one client, one snapshot, one request.

### The design rationale

An AI agent reasoning about a client — generating a campaign recommendation, drafting a performance report, identifying an anomaly — needs a single rich context object, not multiple JOIN-capable tables. The endpoint pre-computes the JOIN across `dim_clients`, `fct_leads`, `fct_ad_performance`, and `stg_seo_rankings` at serving time rather than asking the agent to formulate SQL.

This is the "structured RAG for operational data" pattern: instead of embedding raw documents and retrieving by cosine similarity, you embed structured business entities and retrieve them by ID. The token-dense JSON snapshot is what goes into the LLM system prompt alongside the user's question.

```
[Agent receives user question]
         │
         ▼
GET /context/{client_id}    ←── FastAPI queries DuckDB Gold tables
         │
         ▼
[Structured context injected into prompt]
         │
         ▼
[LLM generates response grounded in actual client data]
```

### Alternatives considered

**GraphQL endpoint:**  
GraphQL would allow agents (or BI tools) to query arbitrary subsets of the data model. The flexibility introduces a problem: an agent that issues a poorly-formed GraphQL query gets back a confusing partial result. A fixed-schema endpoint that always returns the full client snapshot is more predictable for an LLM consumer.

**gRPC / Protocol Buffers:**  
Higher throughput and strict schema at the wire level. Unnecessary for a context API called at agent-decision time (once per interaction, not in a tight loop). gRPC clients require stub generation which adds friction to multi-language agent frameworks. HTTP + JSON is universally supported across LangChain, LlamaIndex, AutoGen, and every other agent SDK.

**Direct DuckDB query from the agent:**  
Some agent frameworks support a SQL tool that queries the database directly. The risk: an agent writing ad-hoc SQL against the data warehouse with no guard rails can produce expensive full-table scans, incorrect JOIN cardinalities, or accidentally expose sensitive columns. A purpose-built endpoint is a security boundary and a performance boundary (the query is optimised once and cached).

**Exposing a REST API over each Gold table separately:**  
`/dim_clients/{id}`, `/fct_leads?client_id={id}`, etc. Requires the agent to make multiple requests and merge results. Adds latency (multiple round-trips), requires the agent to understand the data model structure, and shifts JOIN logic from the data layer (where it belongs) to the agent layer (where it does not).

### What we gave up

A single-entity endpoint is not general-purpose. A BI tool that needs to compare 50 clients, or a query that aggregates across the entire portfolio, cannot use this API efficiently. The correct pattern for those use cases is direct warehouse access (DuckDB/Snowflake) or a separate analytics endpoint. The `/context` API is intentionally optimised for one access pattern: a single-entity context lookup.

---

## 5. dbt project structure: staging vs marts, and what each layer owns

### The decision

Strict ownership rules between Silver (staging) and Gold (marts):

| Layer | SQL it contains | SQL it must not contain |
|---|---|---|
| Staging (`stg_*.sql`) | Type casts, deduplication, null handling, surrogate keys, `_loaded_at` audit column | Business logic, JOINs to other entities, derived metrics |
| Marts (`dim_*.sql`, `fct_*.sql`) | JOINs, aggregations, business metrics, derived flags | Raw column names, type conversions, source-specific logic |

### Why this ownership boundary

The staging model is a contractual wrapper around the source. If the source changes (a column renamed, a status code added), the staging model changes in one place and every downstream mart automatically gets the updated logic. Without this separation, business logic migrates into staging models over time ("I'll just filter out test leads right here") and becomes invisible to anyone reading the mart.

The mart is the business model. It answers questions like "what is the lead conversion rate for this client over the period?" If that calculation changes (e.g., only count leads older than 7 days as valid conversions), the change is made in one mart, not scattered across three staging models that each have slightly different filter logic.

### The CTE pattern

Every mart uses CTEs (no subqueries). Every CTE is named after what it represents, not after a step in a sequence:

```sql
-- Good: named after the business concept
with booked_appointments as (
    select ...
),
converted_leads as (
    select ...
)

-- Avoid: named after the transformation step
with step_1 as (
    select ...
),
step_2 as (
    select ...
)
```

Named CTEs are self-documenting. A reader unfamiliar with the model can understand the logical flow without reading every column. Numbered steps communicate nothing about what the data represents.

---

## 6. Surrogate keys via md5 rather than database sequences

### The decision

All primary keys in staging and mart models are md5-based surrogate keys generated by `dbt_utils.generate_surrogate_key()`:

```sql
{{ dbt_utils.generate_surrogate_key(['client_id']) }} as client_key
```

### Why md5 over alternatives

**Database sequences (AUTOINCREMENT / SERIAL):**  
Sequences are assigned at write time by the database. This makes them non-reproducible: if you drop and recreate a table, the same row gets a different key. For a pipeline that is rebuilt from scratch on every CI run (or every `make reset`), sequence-based keys break foreign key relationships silently.

**Natural keys (client_id directly as PK):**  
Source natural keys (`id` from Postgres) work until you have two sources with overlapping ID spaces — a Postgres `leads.id = 1` and an API `seo_events.id = 1` are different records with the same identifier. md5 keys hash the combination of source identifier + source system identifier, making collisions impossible across sources.

**UUIDs at ingestion time:**  
Assigning UUIDs in the dlt pipeline avoids the md5-in-dbt coupling. The downside: the key is opaque, cannot be reproduced deterministically from the source record, and requires storing an extra column in Bronze. md5 keys are reproducible given the same input columns — running `dbt run` twice over identical Bronze data produces identical surrogate keys, which is what idempotent pipelines require.

### The tradeoff

md5 keys are longer strings than integers and slightly slower to JOIN on at very large scale. For the data volumes in this project (hundreds of thousands of rows, not billions), this cost is unmeasurable. At petabyte scale on Snowflake, hash-based keys on wide fact tables would warrant benchmarking against integer surrogate keys.

---

## 7. Star schema over wide denormalized tables

### The decision

The Gold layer uses a classic star schema: one dimension table (`dim_clients`) and three fact tables (`fct_leads`, `fct_ad_performance`, `fct_appointments`). Gold models are narrow (only the columns relevant to the grain), not wide (every possible attribute denormalized).

### Why star schema for this domain

Star schema was the right choice because the data has two types of information with fundamentally different update patterns:

- **Client attributes** (name, industry, plan tier, onboard date) change rarely. Storing them in `dim_clients` means an update to a client's plan tier propagates to every fact that JOINs to it — without rebuilding the fact table.
- **Events** (leads, ad spend, appointments) arrive daily, are immutable after settlement, and are measured at different grains. One fact table per grain prevents the cross-grain cardinality problems that wide tables create.

### Alternatives considered

**One Big Table (OBT):**  
Pre-JOINing dim and fact into a single wide table eliminates JOIN cost at query time. This is appropriate for serving layers (e.g., the FastAPI context API pre-JOINs at request time). It is not appropriate as a persistent model because: (a) any change to a dimension column requires rebuilding the entire OBT, (b) multiple grains in one table create fan-out when aggregated, and (c) the table becomes unmaintainably wide over time.

**Activity schema (one table with `activity_type` column):**  
All events (leads, appointments, ad spend) stored in a single `activities` table with an event-type column. Reduces the number of tables to manage. The query complexity increases significantly: every query requires a `WHERE activity_type = 'lead'` filter, and JOINing across activity types requires self-joins. The aggregation logic that `fct_leads.is_converted` expresses clearly becomes a lateral join on a filtered activities table.

**Snowflake schema (normalised dimensions):**  
Sub-dimensions (e.g., `dim_industry`, `dim_plan_tier` as separate tables) reduce dimension table size. For this domain, the benefit is minimal: the dimension has 5 industries and 3 plan tiers — they belong as columns, not as join targets.

---

## 8. CI gate ordering: why data quality runs after dbt, not before

### The decision

The CI pipeline runs in this order:

```
lint → dbt compile → dbt run + dbt test → Great Expectations → pytest integration → docker build
```

Great Expectations runs *after* `dbt test`, not before it.

### The reasoning

`dbt test` catches schema-level issues (nulls, uniqueness, referential integrity, accepted values) at the model level. These are cheap to run (they compile to SQL COUNT queries) and fail fast on structure problems before invoking the heavier GX engine.

GX adds a second validation layer that tests are harder to express as dbt generic tests: range checks across the full distribution, row-count bounds that account for seeding variance, and column membership with parameterised value sets. GX is the semantic gate; dbt is the structural gate.

If GX ran before `dbt test`, a GX failure would obscure downstream structural failures. If `dbt run` itself failed (a broken model), GX would have no data to validate against.

### What the gate sequence means in practice

A broken Silver model (`stg_leads` that drops `lead_id`) fails at `dbt test` with a clear `not_null` error. The GX job never runs. The developer sees one focused error rather than a cascade.

A structurally correct model that generates out-of-range lead values (`lead_value: -500`) passes `dbt test` (the test checks for not-null, not range) and fails at the GX `expect_column_values_to_be_between` expectation. The error is semantically meaningful: the model ran, data was generated, but the values are wrong.

---

## 9. Great Expectations execution engine: PandasExecutionEngine over SqlAlchemy

### The decision

The GX data quality gate uses `PandasExecutionEngine`. Silver tables are loaded into Pandas DataFrames via the native duckdb Python API, then validated by GX's Pandas engine. `SqlAlchemyExecutionEngine` (the more obvious SQL-native approach) is not used.

### Why not SqlAlchemy

This decision was discovered empirically, not by design upfront. The initial implementation used `SqlAlchemyExecutionEngine` with `duckdb-engine` (the DuckDB SQLAlchemy dialect). It failed with:

```
_duckdb.InvalidInputException: Invalid Input Error: No open result set
```

Root cause: `duckdb-engine 0.17.0` (current latest) is incompatible with `duckdb 1.5.x`. Upgrading to SQLAlchemy 2.x changed the error to `list index out of range`, a different symptom of the same compatibility gap. The duckdb-engine package has not been updated to handle breaking changes in DuckDB 1.x's cursor API.

### Why PandasExecutionEngine works

The native `duckdb` Python API has first-class support for returning query results as Pandas DataFrames:

```python
conn = duckdb.connect("./data/beacon.duckdb", read_only=True)
df = conn.execute("SELECT * FROM main_silver.stg_clients").df()
```

This bypasses SQLAlchemy entirely. GX's `PandasExecutionEngine` then validates the DataFrame in-process, with no network round-trips and no DBAPI cursor management. The result is faster metric calculation (100% of metrics complete vs. 3/39 with SQLAlchemy) and zero version compatibility surface.

### The broader principle

The SQLAlchemy abstraction layer is valuable for databases where DuckDB's native API doesn't exist. For DuckDB specifically, the native Python API is superior to any SQLAlchemy dialect: it is maintained by the DuckDB team, tracks the DuckDB release cadence, and supports DuckDB-specific features (e.g., returning Arrow tables, `COPY TO`, and file-format extensions) that a generic DBAPI wrapper cannot expose. Prefer native APIs when they exist; use SQLAlchemy when they don't.

### Implications for production

If this project moved to Snowflake, the GX execution engine would switch to `SqlAlchemyExecutionEngine` with the `snowflake-sqlalchemy` dialect (which has a well-maintained adapter and a long track record of SQLAlchemy compatibility). The expectation suites in `quality/expectations/` are engine-agnostic JSON — they require no changes. Only `quality/run_checks.py` changes: swap the DuckDB connection for a Snowflake connector.

---

## 10. Synthetic data design: Faker + seed(42)

### The decision

All data is generated by deterministic Python seed scripts (`seeds/generate_postgres.py`, etc.) using `Faker` and `random.seed(42)`. `make seed` always produces identical data regardless of when it is run.

### Why reproducibility matters

The CI pipeline runs `dbt test` against data that was generated from scratch in that job. If the seed was non-deterministic, a `unique` test could pass in one run and fail in another depending on which random IDs were generated. Reproducibility transforms data tests from flaky checks into reliable gates.

Reproducibility also matters for debugging: if a developer sees a test failure on the CI run from 2pm, they can reproduce it locally with `make seed` — the data will be identical.

### Design choices in the data generator

**Volume:** 500 clients was chosen to be large enough for distribution tests (GX row-count range checks, accepted-value cardinality checks) to be meaningful, but small enough that `make pipeline` completes in under two minutes on a laptop.

**Temporal range:** All data falls in calendar year 2023. The dbt models use `vars.beacon_start_date` and `vars.beacon_end_date` to define the window. This prevents tests from depending on `CURRENT_DATE`, which would produce different join cardinalities as time passes and would make the `days_active` metric in `dim_clients` non-deterministic.

**Distribution design:** Lead counts per client follow a normal distribution (mean ~50, std ~10). This means the GX row-count expectation for `stg_leads` can use a tight range (`17,500–32,500` for 500 clients × 35–65 leads each) rather than an impossibly wide one.

### Alternatives considered

**Production data samples (anonymised):**  
Using real data (even anonymised) creates a dependency on an external data source that may not be available to contributors, may have PII risks, and changes over time. Synthetic data is preferable for a publicly-hosted portfolio project.

**Static CSV fixtures:**  
Committing CSV files with pre-generated data avoids the seed script entirely. The downside: a 2.7M-row ad spend CSV committed to git would bloat the repository significantly. Script-generated data keeps the repo lean and the data fresh on each run.

---

## 11. Why Python end-to-end — no JVM, no Scala, no separate config language

### The decision

The entire stack is Python: dlt (Python), delta-rs Python bindings, DuckDB Python API, dbt (Python CLI + SQL templates), Dagster (Python), Great Expectations (Python), FastAPI (Python), pytest (Python). There is no JVM, no Scala, no Go, and no Java.

### Why this matters for AI data engineering specifically

The intersection of data engineering and AI requires moving freely between the data pipeline layer and the model layer. The Python data science ecosystem (PyTorch, Hugging Face, LangChain, scikit-learn) runs in Python. A stack that requires context-switching to Scala for the pipeline layer and Python for the model layer creates friction that slows down the "data → features → model" iteration loop.

In practice, an AI data engineer at a company doing RAG or fine-tuning will need to:
- Pull a sample from the data warehouse into a DataFrame for inspection
- Feed it through an embedding model
- Write it back to a vector store
- Test the retrieval pipeline

Every step of that loop is Python. A Spark/Scala pipeline that sits upstream creates a wall between the data and the experimentation.

### What we gave up

PySpark is the production standard for distributed Delta Lake writes and is the dominant technology in data engineering job postings. By using delta-rs instead of PySpark, the project does not demonstrate Spark's distributed execution model, broadcast joins, or Catalyst optimizer. This is addressed by ADR 004 and the `examples/airflow_equivalent.py` stub pattern — the architecture documents the swap path explicitly rather than hiding the limitation.

---

## 12. Partitioning strategy: by date at Bronze, none at Silver/Gold

### The decision

Bronze Delta tables are partitioned by `date` (daily granularity for ad spend) or `snapshot_week` (weekly for SEO). Silver and Gold models in DuckDB are not partitioned.

### Why partition Bronze but not Silver/Gold

Bronze is written incrementally by dlt. Each dlt run appends one partition (today's data). Delta Lake's partition pruning means a query for a single day reads one partition file rather than the full table — this matters at scale when Bronze accumulates years of daily ad spend rows.

Silver and Gold are rebuilt by dbt on every pipeline run. DuckDB reads the full Silver view (which reads full Bronze) on each mart rebuild. Adding explicit DuckDB table partitioning to `fct_ad_performance` (the highest-cardinality Gold table) would reduce individual query latency on large datasets, but at this data volume DuckDB's columnar execution is fast enough without it. Adding partitioning would complicate the dbt model config and the FastAPI query without a measurable benefit at the current scale.

### The production flip

At Snowflake/BigQuery scale, cluster keys or partition columns on `fct_ad_performance` (by `spend_date`) and `fct_leads` (by `created_at`) would reduce scan cost significantly. The dbt model SQL requires no changes; only the `dbt_project.yml` config block gains a `partition_by` key:

```yaml
fct_ad_performance:
  +partition_by:
    field: spend_date
    data_type: date
    granularity: month
```

---

## 13. Source freshness as the SLA mechanism

### The decision

Dagster `FreshnessPolicy` on Silver and Gold assets defines the SLA as a maximum age (25 hours for Silver, 26 hours for Gold). Stale assets turn yellow in the Dagster UI. The dbt `sources.yml` defines `freshness:` thresholds on source tables (warn after 24 hours, error after 48 hours for daily sources; warn after 8 days, error after 10 days for weekly SEO).

### Why freshness rather than latency SLAs

Latency SLAs ("Gold must be updated within 2 hours of Bronze landing") require measuring pipeline execution time end-to-end. Freshness SLAs ("Gold must have been refreshed in the last 26 hours") are simpler: they are a property of the data itself, not of the pipeline execution. Any consumer — a BI dashboard, an AI agent, a downstream data team — can check the `_loaded_at` column against `CURRENT_TIMESTAMP` to know if they are looking at stale data.

Freshness SLAs also decouple the upstream trigger from the downstream commitment. If Bronze lands at 01:00 some days and 03:00 others (due to upstream batch delays), a 25-hour Silver freshness SLA still holds as long as Silver runs within a few hours of Bronze landing. A latency SLA would require baking the upstream variance into the commitment.

### What good SLA design looks like

The SEO source is weekly — an SLA of 25 hours would immediately flag it as stale on all non-Monday days. This is why the SEO asset has a 14-day freshness policy rather than inheriting the default daily threshold. Matching the SLA to the source cadence is the correct behaviour; applying a uniform SLA to sources with different refresh rates is the mistake most monitoring setups make.

---

## Summary: Principles behind the decisions

| Principle | How it shows up |
|---|---|
| **Fail loudly, fail early** | Contracts on Gold, GX gate in CI, freshness on every asset |
| **The swap path must be real** | Every local tool has a documented production equivalent and a clear migration step |
| **Python throughout** | Reduces context switching between pipeline and ML/AI layers |
| **Reproducibility over convenience** | Deterministic seeds, md5 surrogate keys, idempotent dbt runs |
| **Contracts belong at consumption boundaries** | Gold only, not Silver; API Pydantic models, not raw dicts |
| **Native APIs over abstraction layers** | duckdb Python API over SQLAlchemy/duckdb-engine; direct dlt sources over generic connectors |
| **Match SLA to cadence** | Weekly sources get weekly SLA windows, not daily ones |
| **Serve the access pattern, not the general case** | `/context/{client_id}` is narrow by design; it is not a generic query API |
