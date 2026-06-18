"""
Data quality gate for Beacon Lakehouse.

Loads Silver views from DuckDB into Pandas DataFrames, then validates them
against Great Expectations suites stored in quality/expectations/.

Uses GX's PandasExecutionEngine rather than SqlAlchemyExecutionEngine to
avoid duckdb-engine / SQLAlchemy version compatibility issues.
"""

import os
import sys
from pathlib import Path

import duckdb
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest

QUALITY_DIR = Path(__file__).parent
SILVER_SCHEMA = "main_silver"

CHECKS = [
    ("stg_clients", "stg_clients_suite"),
    ("stg_leads", "stg_leads_suite"),
]


def _add_pandas_datasource(ctx: gx.DataContext) -> None:
    existing = {ds["name"] for ds in ctx.list_datasources()}
    if "beacon_pandas" in existing:
        return
    ctx.add_datasource(
        name="beacon_pandas",
        module_name="great_expectations.datasource",
        class_name="Datasource",
        execution_engine={"class_name": "PandasExecutionEngine"},
        data_connectors={
            "runtime": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["table_name"],
            }
        },
    )


def main() -> None:
    """Run all data quality checks; exit 1 if any fail."""
    db_path = os.environ.get("DUCKDB_PATH", "./data/beacon.duckdb")
    conn = duckdb.connect(db_path, read_only=True)

    ctx = gx.get_context(context_root_dir=str(QUALITY_DIR))
    _add_pandas_datasource(ctx)

    all_failures: list[str] = []

    for table, suite_name in CHECKS:
        print(f"Checking {SILVER_SCHEMA}.{table}...")
        df = conn.execute(f"SELECT * FROM {SILVER_SCHEMA}.{table}").df()
        print(f"  {len(df):,} rows loaded")

        batch_request = RuntimeBatchRequest(
            datasource_name="beacon_pandas",
            data_connector_name="runtime",
            data_asset_name=table,
            runtime_parameters={"batch_data": df},
            batch_identifiers={"table_name": table},
        )
        validator = ctx.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite_name,
        )
        result = validator.validate()

        for r in result.results:
            if not r.success:
                exp = r.expectation_config.expectation_type
                kw = r.expectation_config.kwargs
                res = r.result
                print(f"  FAILED: {exp} | kwargs={kw} | result={res}")
                all_failures.append(f"{table}: {exp}")

    conn.close()

    if all_failures:
        print(f"\nDATA QUALITY GATE FAILED ({len(all_failures)} failures)")
        sys.exit(1)

    print("\nData quality gate passed.")


if __name__ == "__main__":
    main()
