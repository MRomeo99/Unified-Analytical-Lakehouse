"""
Dagster AssetChecks for row-count and schema validation on Gold marts.
"""

import os

import duckdb

from dagster import AssetCheckResult, AssetCheckSpec, asset_check

from orchestration.beacon_orchestration.assets.gold_assets import (
    dim_clients,
    fct_ad_performance,
    fct_appointments,
    fct_leads,
)


def _duck_conn() -> duckdb.DuckDBPyConnection:
    """Open a read-only DuckDB connection."""
    return duckdb.connect(os.environ.get("DUCKDB_PATH", "./data/beacon.duckdb"), read_only=True)


@asset_check(asset=dim_clients, description="dim_clients must have 500 rows (one per seeded client).")
def dim_clients_row_count() -> AssetCheckResult:
    """Verify dim_clients row count is within expected range."""
    with _duck_conn() as conn:
        row = conn.execute("SELECT count(*) FROM gold.dim_clients").fetchone()
        count = row[0] if row else 0
    passed = 490 <= count <= 510
    return AssetCheckResult(
        passed=passed,
        metadata={"row_count": count},
    )


@asset_check(asset=fct_leads, description="fct_leads must have at least 17500 rows.")
def fct_leads_row_count() -> AssetCheckResult:
    """Verify fct_leads has enough rows (500 clients × ≥35 leads each)."""
    with _duck_conn() as conn:
        row = conn.execute("SELECT count(*) FROM gold.fct_leads").fetchone()
        count = row[0] if row else 0
    passed = count >= 17_500
    return AssetCheckResult(
        passed=passed,
        metadata={"row_count": count},
    )


@asset_check(asset=fct_ad_performance, description="fct_ad_performance must have ~547500 rows.")
def fct_ad_performance_row_count() -> AssetCheckResult:
    """Verify fct_ad_performance row count (500 × 3 channels × 365 days)."""
    with _duck_conn() as conn:
        row = conn.execute("SELECT count(*) FROM gold.fct_ad_performance").fetchone()
        count = row[0] if row else 0
    expected = 500 * 3 * 365
    # Allow 5% variance
    passed = abs(count - expected) / expected < 0.05
    return AssetCheckResult(
        passed=passed,
        metadata={"row_count": count, "expected": expected},
    )


@asset_check(asset=dim_clients, description="dim_clients schema must match contract.")
def dim_clients_schema() -> AssetCheckResult:
    """Verify required columns exist in dim_clients."""
    required = {
        "client_key", "client_id", "client_name", "industry", "plan_tier",
        "onboard_date", "days_active", "total_leads", "lead_conversion_rate",
    }
    with _duck_conn() as conn:
        cols = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'gold' AND table_name = 'dim_clients'"
            ).fetchall()
        }
    missing = required - cols
    passed = len(missing) == 0
    return AssetCheckResult(
        passed=passed,
        metadata={"missing_columns": list(missing)},
    )
