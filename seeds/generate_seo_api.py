"""
Generates synthetic weekly SEO keyword ranking snapshots.

Simulates a REST API data source by writing JSON fixtures.
Realistic week-over-week position drift is applied.
"""

import json
import os
import random
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

KEYWORDS_PER_CLIENT = 10
NUM_WEEKS = 52
WEEK_ZERO = date(2023, 1, 2)  # first Monday of 2023

KEYWORD_TEMPLATES = [
    "{industry} near me",
    "best {industry} in {city}",
    "{industry} services",
    "affordable {industry}",
    "{industry} reviews",
    "top {industry} {city}",
    "local {industry}",
    "{industry} specials",
    "{industry} appointment",
    "emergency {industry}",
]

INDUSTRIES = ["legal", "dental", "home services", "med spa", "auto"]


def _keywords_for_client(client_id: int) -> list[str]:
    """Return 10 deterministic keyword strings for a client."""
    rng = random.Random(client_id * 7 + 42)
    industry = INDUSTRIES[rng.randint(0, len(INDUSTRIES) - 1)]
    city = fake.city()
    return [t.format(industry=industry, city=city) for t in KEYWORD_TEMPLATES]


def generate_seo_rankings(client_ids: list[int]) -> list[dict[str, Any]]:
    """Return 52 weekly position snapshots per keyword per client."""
    rng = random.Random(42)

    rows: list[dict[str, Any]] = []
    for client_id in client_ids:
        keywords = _keywords_for_client(client_id)
        for keyword in keywords:
            position = rng.randint(10, 80)  # starting position
            for week in range(NUM_WEEKS):
                snapshot_date = WEEK_ZERO + timedelta(weeks=week)
                # Drift: ±5 positions each week, clamped 1–100
                drift = rng.randint(-5, 5)
                position = max(1, min(100, position + drift))
                rows.append(
                    {
                        "client_id": client_id,
                        "keyword": keyword,
                        "position": position,
                        "snapshot_date": snapshot_date.isoformat(),
                    }
                )
    return rows


def main(client_ids: list[int] | None = None) -> None:
    """Generate SEO ranking data and save as JSON fixtures."""
    from dotenv import load_dotenv

    load_dotenv()

    if client_ids is None:
        client_ids = list(range(1, 501))

    print(f"Generating SEO rankings for {len(client_ids)} clients...")
    rankings = generate_seo_rankings(client_ids)
    print(f"  {len(rankings)} ranking rows")

    out_dir = Path(os.getenv("DLT_DATA_DIR", "./data/dlt")) / "seo_fixtures"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "seo_rankings.json"
    with open(out_file, "w") as f:
        json.dump(rankings, f)
    print(f"  Written to {out_file}")


if __name__ == "__main__":
    main()
