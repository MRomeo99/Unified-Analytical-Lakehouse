"""
Generates synthetic clients, leads, and appointments and loads them into Postgres.

Uses faker + random.seed(42) so make seed always produces identical data.
"""

import os
import random
from datetime import datetime, timedelta
from typing import Any

import psycopg2
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

INDUSTRIES = ["legal", "dental", "home_services", "med_spa", "auto"]
PLAN_TIERS = ["starter", "pro", "enterprise"]
LEAD_SOURCES = ["organic", "paid", "referral", "social"]
LEAD_STATUSES = ["new", "contacted", "qualified", "converted", "lost"]
NUM_CLIENTS = 500
LEADS_PER_CLIENT = 50

_START_DATE = datetime(2023, 1, 1)
_END_DATE = datetime(2023, 12, 31)


def generate_clients() -> list[dict[str, Any]]:
    """Return 500 deterministic synthetic client records."""
    rng = random.Random(42)
    fake_local = Faker()
    Faker.seed(42)

    clients = []
    for i in range(1, NUM_CLIENTS + 1):
        onboard = _START_DATE + timedelta(days=rng.randint(0, 180))
        clients.append(
            {
                "id": i,
                "name": fake_local.company(),
                "industry": INDUSTRIES[rng.randint(0, len(INDUSTRIES) - 1)],
                "plan_tier": PLAN_TIERS[rng.randint(0, len(PLAN_TIERS) - 1)],
                "onboard_date": onboard.date().isoformat(),
                "city": fake_local.city(),
                "state": fake_local.state_abbr(),
            }
        )
    return clients


def generate_leads(clients: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return ~50 leads per client across 12 months."""
    rng = random.Random(42)

    leads = []
    lead_id = 1
    for client in clients:
        count = rng.randint(35, 65)
        for _ in range(count):
            created = _START_DATE + timedelta(days=rng.randint(0, 364))
            value = round(rng.uniform(50.0, 5000.0), 2)
            leads.append(
                {
                    "id": lead_id,
                    "client_id": client["id"],
                    "source": LEAD_SOURCES[rng.randint(0, len(LEAD_SOURCES) - 1)],
                    "status": LEAD_STATUSES[rng.randint(0, len(LEAD_STATUSES) - 1)],
                    "value": value,
                    "created_at": created.isoformat(),
                }
            )
            lead_id += 1
    return leads


def generate_appointments(leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return one appointment per converted lead."""
    rng = random.Random(42)
    fake_local = Faker()
    Faker.seed(42)

    appointments = []
    appt_id = 1
    for lead in leads:
        if lead["status"] != "converted":
            continue
        created = datetime.fromisoformat(lead["created_at"])
        scheduled = created + timedelta(days=rng.randint(1, 14))
        appointments.append(
            {
                "id": appt_id,
                "lead_id": lead["id"],
                "client_id": lead["client_id"],
                "scheduled_at": scheduled.isoformat(),
                "status": rng.choice(["scheduled", "completed", "cancelled"]),
                "notes": fake_local.sentence(nb_words=6),
            }
        )
        appt_id += 1
    return appointments


def _get_connection() -> "psycopg2.connection":
    """Open a Postgres connection using env vars."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "beacon"),
        user=os.getenv("POSTGRES_USER", "beacon"),
        password=os.getenv("POSTGRES_PASSWORD", "beacon_secret"),
    )


def _create_schema(cur: Any) -> None:
    """Create operational tables if they don't exist."""
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS clients (
            id          INTEGER PRIMARY KEY,
            name        TEXT NOT NULL,
            industry    TEXT NOT NULL,
            plan_tier   TEXT NOT NULL,
            onboard_date DATE NOT NULL,
            city        TEXT,
            state       CHAR(2),
            _loaded_at  TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS leads (
            id          INTEGER PRIMARY KEY,
            client_id   INTEGER NOT NULL REFERENCES clients(id),
            source      TEXT NOT NULL,
            status      TEXT NOT NULL,
            value       NUMERIC(10,2),
            created_at  TIMESTAMP NOT NULL,
            _loaded_at  TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS appointments (
            id           INTEGER PRIMARY KEY,
            lead_id      INTEGER NOT NULL REFERENCES leads(id),
            client_id    INTEGER NOT NULL REFERENCES clients(id),
            scheduled_at TIMESTAMP NOT NULL,
            status       TEXT NOT NULL,
            notes        TEXT,
            _loaded_at   TIMESTAMP DEFAULT NOW()
        );
        """
    )


def _bulk_insert(cur: Any, table: str, rows: list[dict[str, Any]]) -> None:
    """Insert rows into table, truncating first for idempotency."""
    if not rows:
        return
    cur.execute(f"TRUNCATE TABLE {table} CASCADE;")
    cols = list(rows[0].keys())
    placeholders = ", ".join(["%s"] * len(cols))
    col_str = ", ".join(cols)
    values = [tuple(r[c] for c in cols) for r in rows]
    cur.executemany(f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})", values)


def main() -> None:
    """Generate all Postgres seed data and load it."""
    from dotenv import load_dotenv

    load_dotenv()

    print("Generating clients...")
    clients = generate_clients()
    print(f"  {len(clients)} clients")

    print("Generating leads...")
    leads = generate_leads(clients)
    print(f"  {len(leads)} leads")

    print("Generating appointments...")
    appointments = generate_appointments(leads)
    print(f"  {len(appointments)} appointments")

    print("Connecting to Postgres...")
    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                _create_schema(cur)
                _bulk_insert(cur, "clients", clients)
                _bulk_insert(cur, "leads", leads)
                _bulk_insert(cur, "appointments", appointments)
        print("Postgres seed complete.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
