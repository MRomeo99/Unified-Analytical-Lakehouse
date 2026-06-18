"""
GET /context/{client_id} — AI-agent-ready client context endpoint.

Returns a JSON snapshot of a client's current state suitable for injection
into an AI agent's context window. Target p99 latency: <50ms via DuckDB Gold.
"""

import os
from typing import Any

import duckdb
from fastapi import APIRouter, HTTPException

router = APIRouter()

_DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "./data/beacon.duckdb")


def fetch_client_context(client_id: int) -> dict[str, Any] | None:
    """Query DuckDB Gold layer for a client's full context snapshot."""
    with duckdb.connect(_DUCKDB_PATH, read_only=True) as conn:
        # Base client metrics from dim_clients
        client_row = conn.execute(
            """
            SELECT
                client_id,
                client_name,
                industry,
                plan_tier,
                days_active,
                total_leads,
                converted_leads,
                lead_conversion_rate,
                total_appointments
            FROM gold.dim_clients
            WHERE client_id = ?
            """,
            [client_id],
        ).fetchone()

        if client_row is None:
            return None

        cols = [
            "client_id",
            "client_name",
            "industry",
            "plan_tier",
            "days_active",
            "total_leads",
            "converted_leads",
            "lead_conversion_rate",
            "total_appointments",
        ]
        result = dict(zip(cols, client_row))

        # Top 5 keywords by position (lower = better)
        keywords = conn.execute(
            """
            SELECT keyword, position
            FROM gold.stg_seo_rankings
            WHERE client_id = ?
            ORDER BY position ASC
            LIMIT 5
            """,
            [client_id],
        ).fetchall()
        result["top_keywords"] = [{"keyword": k, "position": p} for k, p in keywords]

        # Last 30 days ad spend and average CPL
        spend_row = conn.execute(
            """
            SELECT
                coalesce(sum(spend), 0)      as monthly_spend_usd,
                coalesce(avg(cpl), null)     as avg_cpl
            FROM gold.fct_ad_performance
            WHERE client_id = ?
              AND spend_date >= current_date - interval '30 days'
            """,
            [client_id],
        ).fetchone()
        if spend_row:
            result["monthly_spend_usd"] = float(spend_row[0])
            result["avg_cpl"] = float(spend_row[1]) if spend_row[1] is not None else None

        result["last_updated"] = conn.execute("SELECT current_timestamp").fetchone()[0].isoformat()

    return result


@router.get(
    "/context/{client_id}",
    summary="Get client context snapshot",
    description=(
        "Returns a JSON snapshot of a client's current marketing performance state. "
        "Designed to be injected directly into an AI agent's context window."
    ),
)
def get_client_context(client_id: int) -> dict[str, Any]:
    """Return context snapshot for the given client_id."""
    context = fetch_client_context(client_id)
    if context is None:
        raise HTTPException(status_code=404, detail=f"Client {client_id} not found.")
    return context
