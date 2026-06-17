"""
Dagster software-defined assets for the Bronze (raw ingestion) layer.

Each asset maps 1:1 to a dlt pipeline run.
"""

from dagster import AssetExecutionContext, asset


@asset(
    group_name="bronze",
    description="Raw client records ingested from Postgres via dlt → Delta Lake on MinIO.",
    compute_kind="dlt",
)
def raw_clients(context: AssetExecutionContext) -> None:
    """Ingest clients from Postgres operational database."""
    from ingestion.sources.postgres_source import postgres_source
    from ingestion.pipelines import run_postgres_pipeline

    context.log.info("Starting Postgres ingestion pipeline...")
    pipeline = run_postgres_pipeline()
    context.log.info(f"Pipeline run: {pipeline.last_run_info}")


@asset(
    group_name="bronze",
    description="Raw lead records ingested from Postgres via dlt → Delta Lake on MinIO.",
    compute_kind="dlt",
    deps=[raw_clients],
)
def raw_leads(context: AssetExecutionContext) -> None:
    """Raw leads are loaded as part of the Postgres pipeline (same run as raw_clients)."""
    context.log.info("raw_leads loaded as part of raw_clients Postgres pipeline.")


@asset(
    group_name="bronze",
    description="Raw appointment records from Postgres.",
    compute_kind="dlt",
    deps=[raw_leads],
)
def raw_appointments(context: AssetExecutionContext) -> None:
    """Raw appointments loaded with Postgres pipeline."""
    context.log.info("raw_appointments loaded as part of raw_clients Postgres pipeline.")


@asset(
    group_name="bronze",
    description="Weekly SEO keyword rankings from JSON fixture (simulates REST API).",
    compute_kind="dlt",
)
def raw_seo_rankings(context: AssetExecutionContext) -> None:
    """Ingest SEO rankings from JSON fixture."""
    from ingestion.pipelines import run_seo_pipeline

    context.log.info("Starting SEO pipeline...")
    run_seo_pipeline()


@asset(
    group_name="bronze",
    description="Daily ad spend data from CSV flat files.",
    compute_kind="dlt",
)
def raw_ad_spend(context: AssetExecutionContext) -> None:
    """Ingest ad spend from CSV export."""
    from ingestion.pipelines import run_ad_spend_pipeline

    context.log.info("Starting ad spend pipeline...")
    run_ad_spend_pipeline()
