"""
Dagster sensors — file-based freshness sensor for ad spend CSV drops.
"""

from dagster import RunRequest, SensorEvaluationContext, sensor


@sensor(
    job_name="beacon_full_pipeline",
    description="Triggers a pipeline run when a new ad_spend.csv is detected.",
    minimum_interval_seconds=300,
)
def ad_spend_csv_sensor(context: SensorEvaluationContext):
    """Watch for new ad spend CSV and trigger a pipeline run."""
    import os
    from pathlib import Path

    csv_path = Path(os.environ.get("DLT_DATA_DIR", "./data/dlt")) / "ad_spend" / "ad_spend.csv"
    if not csv_path.exists():
        return

    mtime = str(csv_path.stat().st_mtime)
    last_mtime = context.cursor or ""
    if mtime != last_mtime:
        context.update_cursor(mtime)
        yield RunRequest(run_key=mtime, run_config={})
