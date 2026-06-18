"""
Dagster schedule definitions — daily pipeline run at 02:00 UTC.
"""

from dagster import DefaultScheduleStatus, ScheduleDefinition  # noqa: I001


daily_pipeline_schedule = ScheduleDefinition(
    name="daily_pipeline",
    cron_schedule="0 2 * * *",
    job_name="beacon_full_pipeline",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Run full Bronze → Silver → Gold pipeline daily at 02:00 UTC.",
)
