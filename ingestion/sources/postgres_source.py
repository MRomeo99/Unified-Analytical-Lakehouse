"""
dlt source for extracting clients, leads, and appointments from Postgres.

Writes raw Delta Lake tables to MinIO (Bronze layer).
"""

from collections.abc import Iterator

import dlt
from dlt.sources import DltResource


@dlt.source(name="postgres_operational")
def postgres_source(
    host: str = dlt.config.value,
    port: int = dlt.config.value,
    database: str = dlt.config.value,
    username: str = dlt.config.value,
    password: dlt.secrets.value = dlt.config.value,
) -> tuple[DltResource, ...]:
    """Extract all operational tables from Postgres."""
    conn_str = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    return (
        _clients_resource(conn_str),
        _leads_resource(conn_str),
        _appointments_resource(conn_str),
    )


@dlt.resource(name="raw_clients", write_disposition="replace", primary_key="id")
def _clients_resource(conn_str: str) -> Iterator[dict]:
    """Extract client records."""
    import psycopg2
    import psycopg2.extras

    with psycopg2.connect(conn_str) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM clients ORDER BY id;")
            for row in cur:
                yield dict(row)


@dlt.resource(name="raw_leads", write_disposition="replace", primary_key="id")
def _leads_resource(conn_str: str) -> Iterator[dict]:
    """Extract lead records."""
    import psycopg2
    import psycopg2.extras

    with psycopg2.connect(conn_str) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM leads ORDER BY id;")
            for row in cur:
                yield dict(row)


@dlt.resource(name="raw_appointments", write_disposition="replace", primary_key="id")
def _appointments_resource(conn_str: str) -> Iterator[dict]:
    """Extract appointment records."""
    import psycopg2
    import psycopg2.extras

    with psycopg2.connect(conn_str) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM appointments ORDER BY id;")
            for row in cur:
                yield dict(row)
