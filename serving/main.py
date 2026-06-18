"""
FastAPI application — Beacon Lakehouse serving layer.

Exposes AI-agent-ready context endpoints backed by DuckDB Gold tables.
OpenAPI docs auto-generated at /docs.
"""

from dotenv import load_dotenv
from fastapi import FastAPI

load_dotenv()

from serving.routers.client_context import router as context_router  # noqa: E402

app = FastAPI(
    title="Beacon Lakehouse API",
    description=(
        "Low-latency context API for the Beacon unified analytical lakehouse. "
        "Returns structured marketing analytics snapshots for AI agent consumption."
    ),
    version="1.0.0",
)

app.include_router(context_router, tags=["Client Context"])


@app.get("/health", tags=["Ops"])
def health() -> dict:
    """Liveness check."""
    return {"status": "ok"}
