"""
Entry point for all dlt ingestion pipelines (Bronze layer).

Reads from Postgres, SEO JSON fixture, and CSV ad spend.
Writes Delta Lake tables to MinIO via the filesystem destination.
"""

import os
from pathlib import Path

import dlt
from dotenv import load_dotenv

from ingestion.sources.ad_spend_source import ad_spend_source
from ingestion.sources.postgres_source import postgres_source
from ingestion.sources.seo_api_source import seo_api_source


def _make_filesystem_destination() -> dlt.destinations.filesystem:
    """Configure the dlt filesystem destination pointing at MinIO."""
    bucket_url = os.environ["BRONZE_BUCKET_URL"]  # e.g. s3://beacon-bronze
    return dlt.destinations.filesystem(
        bucket_url=bucket_url,
        credentials={
            "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "endpoint_url": os.environ["AWS_ENDPOINT_URL"],
            "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        },
    )


def run_postgres_pipeline() -> dlt.Pipeline:
    """Ingest Postgres operational tables → Bronze Delta tables."""
    pipeline = dlt.pipeline(
        pipeline_name="beacon_postgres",
        destination=_make_filesystem_destination(),
        dataset_name="bronze",
    )
    source = postgres_source(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        database=os.environ["POSTGRES_DB"],
        username=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    pipeline.run(source, loader_file_format="delta")
    return pipeline


def run_seo_pipeline() -> dlt.Pipeline:
    """Ingest SEO ranking JSON fixture → Bronze Delta table."""
    data_dir = os.environ.get("DLT_DATA_DIR", "./data/dlt")
    fixture = str(Path(data_dir) / "seo_fixtures" / "seo_rankings.json")
    pipeline = dlt.pipeline(
        pipeline_name="beacon_seo",
        destination=_make_filesystem_destination(),
        dataset_name="bronze",
    )
    source = seo_api_source(fixture_path=fixture)
    pipeline.run(source, loader_file_format="delta")
    return pipeline


def run_ad_spend_pipeline() -> dlt.Pipeline:
    """Ingest CSV ad spend data → Bronze Delta table."""
    data_dir = os.environ.get("DLT_DATA_DIR", "./data/dlt")
    csv_path = str(Path(data_dir) / "ad_spend" / "ad_spend.csv")
    pipeline = dlt.pipeline(
        pipeline_name="beacon_ad_spend",
        destination=_make_filesystem_destination(),
        dataset_name="bronze",
    )
    source = ad_spend_source(csv_path=csv_path)
    pipeline.run(source, loader_file_format="delta")
    return pipeline


def run_all() -> None:
    """Run all three Bronze-layer pipelines in sequence."""
    load_dotenv()
    print("Running Postgres pipeline...")
    run_postgres_pipeline()
    print("Running SEO rankings pipeline...")
    run_seo_pipeline()
    print("Running ad spend pipeline...")
    run_ad_spend_pipeline()
    print("All Bronze pipelines complete.")


if __name__ == "__main__":
    run_all()
