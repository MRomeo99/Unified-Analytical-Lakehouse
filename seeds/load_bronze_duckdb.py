"""
CI utility: populate DuckDB bronze schema directly from seed generators.

Bypasses dlt / MinIO / Postgres so the dbt layer can be tested in CI
without any external infrastructure. For local development use
'make ingest' to run the full dlt pipeline instead.
"""

import os
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd

from seeds.generate_ad_spend import generate_ad_spend
from seeds.generate_postgres import generate_appointments, generate_clients, generate_leads
from seeds.generate_seo_api import generate_seo_rankings

CLIENT_IDS = list(range(1, 501))


def _load(
    conn: duckdb.DuckDBPyConnection, table: str, records: list, extra: dict | None = None
) -> None:
    """Create or replace a bronze table from a list of dicts."""
    df = pd.DataFrame(records)
    if extra:
        for col, val in extra.items():
            df[col] = val
    conn.execute(f"DROP TABLE IF EXISTS bronze.{table}")
    conn.execute(f"CREATE TABLE bronze.{table} AS SELECT * FROM df")
    print(f"  {table}: {len(df):,} rows")


def main() -> None:
    duckdb_path = os.environ.get("DUCKDB_PATH", "./data/beacon.duckdb")
    Path(duckdb_path).parent.mkdir(parents=True, exist_ok=True)

    now = datetime.now(UTC).isoformat()

    print(f"Populating DuckDB bronze tables in {duckdb_path} ...")
    conn = duckdb.connect(duckdb_path)
    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")

    clients = generate_clients()
    leads = generate_leads(clients)
    appointments = generate_appointments(leads)

    _load(conn, "raw_clients", clients, {"_loaded_at": now})
    _load(conn, "raw_leads", leads, {"_loaded_at": now})
    _load(conn, "raw_appointments", appointments, {"_loaded_at": now})

    seo_rows = generate_seo_rankings(CLIENT_IDS)
    _load(conn, "raw_seo_rankings", seo_rows, {"_dlt_load_time": now})

    ad_rows = generate_ad_spend(CLIENT_IDS)
    _load(conn, "raw_ad_spend", ad_rows, {"_dlt_load_time": now})

    conn.close()
    print("Bronze tables ready.")


if __name__ == "__main__":
    main()
