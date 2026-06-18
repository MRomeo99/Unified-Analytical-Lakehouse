"""
dlt source for SEO ranking data.

In production this would call a REST API. Locally it reads from the JSON
fixture produced by seeds/generate_seo_api.py (simulating the API response).
Decision note: a real REST API connector would use dlt.sources.rest_api with
pagination; the fixture approach keeps local development zero-dependency.
"""

import json
from collections.abc import Iterator
from pathlib import Path

import dlt
from dlt.sources import DltResource


@dlt.source(name="seo_api")
def seo_api_source(
    fixture_path: str = dlt.config.value,
) -> DltResource:
    """Load SEO rankings from fixture (simulates REST API)."""
    return _seo_rankings_resource(fixture_path)


@dlt.resource(
    name="raw_seo_rankings",
    write_disposition="replace",
    primary_key=["client_id", "keyword", "snapshot_date"],
)
def _seo_rankings_resource(fixture_path: str) -> Iterator[dict]:
    """Yield all SEO ranking rows from the JSON fixture."""
    path = Path(fixture_path)
    if not path.exists():
        raise FileNotFoundError(
            f"SEO fixture not found at {fixture_path}. Run 'make seed' first."
        )
    with open(path) as f:
        rankings = json.load(f)
    yield from rankings
