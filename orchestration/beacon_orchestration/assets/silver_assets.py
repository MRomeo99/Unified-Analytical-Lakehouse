"""
Dagster software-defined assets for the Silver (staging) layer.

Each asset runs a subset of dbt staging models.
FreshnessPolicies enforce SLA: Silver must refresh within 25 hours.
"""

from dagster import AssetExecutionContext, FreshnessPolicy, asset

from orchestration.beacon_orchestration.assets.bronze_assets import (
    raw_ad_spend,
    raw_appointments,
    raw_clients,
    raw_leads,
    raw_seo_rankings,
)

_SILVER_FRESHNESS = FreshnessPolicy(maximum_lag_minutes=25 * 60)


def _run_dbt_models(*model_names: str, context: AssetExecutionContext) -> None:
    """Run specific dbt models via subprocess."""
    import subprocess
    from pathlib import Path

    project_dir = Path(__file__).parent.parent.parent.parent / "transform" / "beacon"
    select = " ".join(model_names)
    result = subprocess.run(
        ["dbt", "run", "--select", select, "--project-dir", str(project_dir)],
        capture_output=True,
        text=True,
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"dbt run failed:\n{result.stderr}")


@asset(
    group_name="silver",
    description="Typed and deduplicated client records (Silver).",
    freshness_policy=_SILVER_FRESHNESS,
    compute_kind="dbt",
    deps=[raw_clients],
)
def stg_clients(context: AssetExecutionContext) -> None:
    """Run stg_clients dbt model."""
    _run_dbt_models("stg_clients", context=context)


@asset(
    group_name="silver",
    description="Typed and deduplicated lead records (Silver).",
    freshness_policy=_SILVER_FRESHNESS,
    compute_kind="dbt",
    deps=[raw_leads],
)
def stg_leads(context: AssetExecutionContext) -> None:
    """Run stg_leads dbt model."""
    _run_dbt_models("stg_leads", context=context)


@asset(
    group_name="silver",
    description="Typed appointment records (Silver).",
    freshness_policy=_SILVER_FRESHNESS,
    compute_kind="dbt",
    deps=[raw_appointments],
)
def stg_appointments(context: AssetExecutionContext) -> None:
    """Run stg_appointments dbt model."""
    _run_dbt_models("stg_appointments", context=context)


@asset(
    group_name="silver",
    description="Typed SEO ranking snapshots (Silver).",
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=14 * 24 * 60),  # 14-day SLA for weekly data
    compute_kind="dbt",
    deps=[raw_seo_rankings],
)
def stg_seo_rankings(context: AssetExecutionContext) -> None:
    """Run stg_seo_rankings dbt model."""
    _run_dbt_models("stg_seo_rankings", context=context)


@asset(
    group_name="silver",
    description="Typed daily ad spend records (Silver).",
    freshness_policy=_SILVER_FRESHNESS,
    compute_kind="dbt",
    deps=[raw_ad_spend],
)
def stg_ad_spend(context: AssetExecutionContext) -> None:
    """Run stg_ad_spend dbt model."""
    _run_dbt_models("stg_ad_spend", context=context)
