"""
Dagster software-defined assets for the Gold (mart) layer.

FreshnessPolicies on Gold ensure marts are rebuilt within 26 hours of any Silver update.
"""

from dagster import AssetExecutionContext, FreshnessPolicy, asset

from orchestration.beacon_orchestration.assets.silver_assets import (
    stg_ad_spend,
    stg_appointments,
    stg_clients,
    stg_leads,
)

_GOLD_FRESHNESS = FreshnessPolicy(maximum_lag_minutes=26 * 60)


def _run_dbt_models(*model_names: str, context: AssetExecutionContext) -> None:
    """Run dbt models for Gold layer."""
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
    group_name="gold",
    description="Client dimension with lifetime metrics and conversion rates (Gold).",
    freshness_policy=_GOLD_FRESHNESS,
    compute_kind="dbt",
    deps=[stg_clients, stg_leads, stg_appointments],
)
def dim_clients(context: AssetExecutionContext) -> None:
    """Build dim_clients Gold mart."""
    _run_dbt_models("dim_clients", context=context)


@asset(
    group_name="gold",
    description="Lead fact table with conversion flags and client context (Gold).",
    freshness_policy=_GOLD_FRESHNESS,
    compute_kind="dbt",
    deps=[stg_leads, stg_clients, stg_appointments],
)
def fct_leads(context: AssetExecutionContext) -> None:
    """Build fct_leads Gold mart."""
    _run_dbt_models("fct_leads", context=context)


@asset(
    group_name="gold",
    description="Daily ad performance fact table with CPL and CTR metrics (Gold).",
    freshness_policy=_GOLD_FRESHNESS,
    compute_kind="dbt",
    deps=[stg_ad_spend, stg_clients, stg_leads],
)
def fct_ad_performance(context: AssetExecutionContext) -> None:
    """Build fct_ad_performance Gold mart."""
    _run_dbt_models("fct_ad_performance", context=context)


@asset(
    group_name="gold",
    description="Appointment fact table with lead-to-appointment funnel metrics (Gold).",
    freshness_policy=_GOLD_FRESHNESS,
    compute_kind="dbt",
    deps=[stg_appointments, stg_leads, stg_clients],
)
def fct_appointments(context: AssetExecutionContext) -> None:
    """Build fct_appointments Gold mart."""
    _run_dbt_models("fct_appointments", context=context)
