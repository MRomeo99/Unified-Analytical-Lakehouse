# ADR 004 — delta-rs over PySpark

## Status
Accepted

## Context
Needed the Delta Lake table format (ACID transactions, time travel, partition pruning) for the Bronze layer without requiring a JVM, a Spark cluster, or a Databricks subscription. Local development must run entirely on a laptop via Docker Compose with no cloud accounts and no paid services. PySpark adds significant operational complexity and memory overhead that is impractical for a locally-runnable portfolio project.

## Decision
delta-rs Python bindings via the `deltalake` package. delta-rs supports reading, writing, and compacting Delta tables natively in Python without a JVM dependency. dlt uses the `deltalake` destination to write Bronze tables directly to MinIO (S3-compatible local object storage). DuckDB reads those same Delta files via its built-in `delta` extension, enabling the full Bronze-to-Gold pipeline to run without Spark at any layer.

## Tradeoffs
PySpark is the production standard for distributed Delta writes and is the native engine on Databricks. delta-rs is single-node and does not support all Delta writer protocol features — notably, deletion vectors and liquid clustering support are limited compared to the full Spark Delta implementation. For datasets that exceed a single machine's memory or require concurrent distributed writes, delta-rs is not a viable production choice.

## Swap path to production
The Delta files written by delta-rs are fully compatible with Databricks and Apache Spark — no format conversion is required. In production, replace dlt's filesystem/deltalake destination with a Databricks-native Delta write or a Spark-based dlt destination. The Delta files stored in S3 or GCS are read identically by both delta-rs and Spark. The dbt models in the Silver and Gold layers require no changes; only the Bronze ingestion destination configuration changes.
