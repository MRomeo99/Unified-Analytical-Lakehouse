"""
Generates synthetic daily ad spend CSV exports (one per channel per client).

Weekday spend is higher than weekend spend to reflect real campaign patterns.
"""

import csv
import os
import random
from datetime import date, timedelta
from pathlib import Path
from typing import Any

CHANNELS = ["google", "meta", "email"]
NUM_DAYS = 365
START_DATE = date(2023, 1, 1)

# Spend ranges per channel ($)
CHANNEL_SPEND = {
    "google": (50.0, 500.0),
    "meta": (30.0, 300.0),
    "email": (10.0, 80.0),
}
WEEKEND_FACTOR = 0.6  # weekends have 60% of weekday spend


def generate_ad_spend(client_ids: list[int]) -> list[dict[str, Any]]:
    """Return 365 daily rows per client per channel."""
    rng = random.Random(42)

    rows: list[dict[str, Any]] = []
    for client_id in client_ids:
        for channel in CHANNELS:
            lo, hi = CHANNEL_SPEND[channel]
            for day in range(NUM_DAYS):
                dt = START_DATE + timedelta(days=day)
                is_weekend = dt.weekday() >= 5
                factor = WEEKEND_FACTOR if is_weekend else 1.0
                spend = round(rng.uniform(lo, hi) * factor, 2)
                # Impressions ~1000–50000, clicks ~1–5% of impressions
                impressions = rng.randint(1_000, 50_000)
                clicks = rng.randint(int(impressions * 0.01), int(impressions * 0.05))
                rows.append(
                    {
                        "client_id": client_id,
                        "channel": channel,
                        "date": dt.isoformat(),
                        "spend": spend,
                        "impressions": impressions,
                        "clicks": clicks,
                    }
                )
    return rows


def main(client_ids: list[int] | None = None) -> None:
    """Generate ad spend CSV files."""
    from dotenv import load_dotenv

    load_dotenv()

    if client_ids is None:
        client_ids = list(range(1, 501))

    print(f"Generating ad spend for {len(client_ids)} clients × {len(CHANNELS)} channels...")
    rows = generate_ad_spend(client_ids)
    print(f"  {len(rows)} rows")

    out_dir = Path(os.getenv("DLT_DATA_DIR", "./data/dlt")) / "ad_spend"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "ad_spend.csv"
    with open(out_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Written to {out_file}")


if __name__ == "__main__":
    main()
