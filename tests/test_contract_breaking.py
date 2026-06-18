"""
Demonstrates that CI catches a Gold model contract violation.

How this works:
1. A deliberately broken dim_clients model (missing required column) is written
   to a temp location.
2. dbt run with that model should fail because the contract in contracts/schema.yml
   enforces the column list.
3. The test asserts that the failure is raised, proving the CI gate works.

This test is marked as integration because it requires a running DuckDB + dbt.
"""

import subprocess
import textwrap
from pathlib import Path

import pytest

DBT_PROJECT_DIR = Path(__file__).parent.parent / "transform" / "beacon"


@pytest.mark.integration
def test_contract_violation_causes_dbt_failure(tmp_path):
    """
    Replace dim_clients with a broken version missing client_name.
    dbt run must fail with a contract violation error.
    """
    broken_sql = textwrap.dedent(
        """
        -- INTENTIONALLY BROKEN: client_name column removed to trigger contract violation
        with clients as (
            select * from {{ ref('stg_clients') }}
        )
        select
            client_key,
            client_id,
            -- client_name intentionally omitted
            industry,
            plan_tier,
            onboard_date,
            city,
            state,
            0                       as days_active,
            0                       as total_leads,
            0                       as converted_leads,
            0                       as total_appointments,
            null                    as lead_conversion_rate,
            current_timestamp       as _loaded_at
        from clients
        """
    )

    mart_path = DBT_PROJECT_DIR / "models" / "marts" / "dim_clients.sql"
    original = mart_path.read_text()

    try:
        mart_path.write_text(broken_sql)
        result = subprocess.run(
            [
                "dbt",
                "run",
                "--select",
                "dim_clients",
                "--project-dir",
                str(DBT_PROJECT_DIR),
                "--profiles-dir",
                str(DBT_PROJECT_DIR),
            ],
            capture_output=True,
            text=True,
        )
        # The broken model must NOT succeed — contract enforces schema
        assert result.returncode != 0, (
            "Expected dbt to fail due to contract violation (missing client_name), "
            f"but it exited 0.\n{result.stdout}"
        )
        # Confirm the failure is contract-related
        combined = result.stdout + result.stderr
        assert (
            "contract" in combined.lower() or "column" in combined.lower()
        ), f"Expected contract/column error in dbt output:\n{combined}"
    finally:
        mart_path.write_text(original)


@pytest.mark.integration
def test_correct_model_passes_contract():
    """
    The original dim_clients must pass the contract after restoring it.
    (Run after test_contract_violation_causes_dbt_failure to confirm restore.)
    """
    result = subprocess.run(
        [
            "dbt",
            "run",
            "--select",
            "dim_clients",
            "--project-dir",
            str(DBT_PROJECT_DIR),
            "--profiles-dir",
            str(DBT_PROJECT_DIR),
        ],
        capture_output=True,
        text=True,
    )
    assert (
        result.returncode == 0
    ), f"Expected clean dim_clients to pass contract but got:\n{result.stderr}"
