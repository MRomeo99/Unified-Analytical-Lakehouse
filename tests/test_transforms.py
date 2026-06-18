"""
Tests for dbt transform layer (Silver staging + Gold marts).

TDD: these tests define the contract before SQL models exist.
Running `dbt compile` and `dbt test` is the "green" state.
"""

import subprocess
from pathlib import Path

import pytest

DBT_PROJECT_DIR = Path(__file__).parent.parent / "transform" / "beacon"


def _run_dbt(*args: str) -> subprocess.CompletedProcess:
    """Run a dbt command in the project directory."""
    return subprocess.run(
        [
            "dbt",
            *args,
            "--project-dir",
            str(DBT_PROJECT_DIR),
            "--profiles-dir",
            str(DBT_PROJECT_DIR),
        ],
        capture_output=True,
        text=True,
    )


class TestDbtProjectStructure:
    """Verify required dbt files exist before running anything."""

    def test_dbt_project_yml_exists(self):
        """dbt_project.yml must be present."""
        assert (DBT_PROJECT_DIR / "dbt_project.yml").exists()

    def test_profiles_yml_exists(self):
        """profiles.yml must configure the DuckDB target."""
        assert (DBT_PROJECT_DIR / "profiles.yml").exists()

    def test_sources_yml_exists(self):
        """sources.yml must define all Bronze sources."""
        assert (DBT_PROJECT_DIR / "sources.yml").exists()

    def test_staging_models_exist(self):
        """All five staging (Silver) models must be present."""
        staging = DBT_PROJECT_DIR / "models" / "staging"
        expected = [
            "stg_clients.sql",
            "stg_leads.sql",
            "stg_seo_rankings.sql",
            "stg_ad_spend.sql",
            "stg_appointments.sql",
        ]
        for f in expected:
            assert (staging / f).exists(), f"Missing {f}"

    def test_mart_models_exist(self):
        """All four Gold mart models must be present."""
        marts = DBT_PROJECT_DIR / "models" / "marts"
        expected = [
            "dim_clients.sql",
            "fct_leads.sql",
            "fct_ad_performance.sql",
            "fct_appointments.sql",
        ]
        for f in expected:
            assert (marts / f).exists(), f"Missing {f}"

    def test_contracts_schema_exists(self):
        """Gold contracts must have at least one schema.yml."""
        contracts = DBT_PROJECT_DIR / "contracts"
        ymls = list(contracts.glob("*.yml"))
        assert len(ymls) > 0, "No contract schema.yml found"


class TestDbtCompile:
    """dbt compile must succeed (catches SQL syntax errors)."""

    @pytest.mark.integration
    def test_dbt_compile_succeeds(self):
        """dbt compile should exit 0."""
        result = _run_dbt("compile")
        assert result.returncode == 0, f"dbt compile failed:\n{result.stderr}"


class TestDbtSchemaTests:
    """dbt test must pass all schema-level tests."""

    @pytest.mark.integration
    def test_dbt_tests_pass(self):
        """dbt test should exit 0 (all not_null, unique, etc.)."""
        result = _run_dbt("test")
        assert result.returncode == 0, f"dbt test failed:\n{result.stdout}\n{result.stderr}"


class TestStagingModelContracts:
    """Verify staging model SQL follows required patterns."""

    def _read_model(self, name: str) -> str:
        """Read a staging model file."""
        return (DBT_PROJECT_DIR / "models" / "staging" / name).read_text()

    def test_stg_clients_has_surrogate_key(self):
        """Staging clients model must generate a surrogate key."""
        sql = self._read_model("stg_clients.sql")
        assert "generate_surrogate_key" in sql or "surrogate_key" in sql.lower()

    def test_stg_clients_casts_types(self):
        """Staging model must explicitly cast onboard_date."""
        sql = self._read_model("stg_clients.sql")
        assert "cast" in sql.lower() or "::" in sql

    def test_stg_leads_deduplicates_via_row_number(self):
        """Staging leads must deduplicate using ROW_NUMBER window."""
        sql = self._read_model("stg_leads.sql")
        assert "row_number" in sql.lower()


class TestGoldModelContracts:
    """Verify Gold mart SQL follows required patterns."""

    def _read_model(self, name: str) -> str:
        """Read a mart model file."""
        return (DBT_PROJECT_DIR / "models" / "marts" / name).read_text()

    def test_dim_clients_uses_ctes(self):
        """dim_clients must use CTEs not subqueries."""
        sql = self._read_model("dim_clients.sql")
        assert "with " in sql.lower()

    def test_fct_leads_has_conversion_flag(self):
        """fct_leads must include a boolean conversion flag."""
        sql = self._read_model("fct_leads.sql")
        assert "is_converted" in sql.lower() or "converted" in sql.lower()

    def test_fct_ad_performance_calculates_cpl(self):
        """fct_ad_performance must compute cost-per-lead."""
        sql = self._read_model("fct_ad_performance.sql")
        assert "cpl" in sql.lower() or "cost_per" in sql.lower()
