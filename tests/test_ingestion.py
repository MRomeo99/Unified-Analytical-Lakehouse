"""
Integration tests for seed generators and dlt ingestion pipelines.
TDD: these tests were written before the implementations and define the contract.
"""

import sys
from pathlib import Path

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).parent.parent))


# ---------------------------------------------------------------------------
# Seed data tests
# ---------------------------------------------------------------------------


class TestGeneratePostgres:
    """Tests for seeds/generate_postgres.py."""

    def test_generate_clients_returns_500_rows(self, tmp_path, monkeypatch):
        """Seed must produce exactly 500 client records."""
        from seeds.generate_postgres import generate_clients

        clients = generate_clients()
        assert len(clients) == 500

    def test_client_has_required_fields(self, tmp_path, monkeypatch):
        """Every client record must include all domain fields."""
        from seeds.generate_postgres import generate_clients

        client = generate_clients()[0]
        required = {"id", "name", "industry", "plan_tier", "onboard_date", "city", "state"}
        assert required.issubset(set(client.keys()))

    def test_client_industries_are_valid(self):
        """Clients span exactly the 5 specified industries."""
        from seeds.generate_postgres import generate_clients

        clients = generate_clients()
        industries = {c["industry"] for c in clients}
        expected = {"legal", "dental", "home_services", "med_spa", "auto"}
        assert industries == expected

    def test_client_plan_tiers_are_valid(self):
        """Plan tiers are restricted to starter / pro / enterprise."""
        from seeds.generate_postgres import generate_clients

        clients = generate_clients()
        tiers = {c["plan_tier"] for c in clients}
        assert tiers.issubset({"starter", "pro", "enterprise"})

    def test_generate_leads_returns_approx_50_per_client(self):
        """Each client should have ~50 leads (±25 allowed variance)."""
        from seeds.generate_postgres import generate_clients, generate_leads

        clients = generate_clients()
        leads = generate_leads(clients)
        per_client = len(leads) / len(clients)
        assert 25 <= per_client <= 75

    def test_lead_has_required_fields(self):
        """Every lead record must include all domain fields."""
        from seeds.generate_postgres import generate_clients, generate_leads

        clients = generate_clients()
        leads = generate_leads(clients)
        required = {"id", "client_id", "source", "status", "value", "created_at"}
        assert required.issubset(set(leads[0].keys()))

    def test_lead_sources_are_valid(self):
        """Lead sources must be one of the four specified channels."""
        from seeds.generate_postgres import generate_clients, generate_leads

        clients = generate_clients()
        leads = generate_leads(clients)
        sources = {lead["source"] for lead in leads}
        assert sources.issubset({"organic", "paid", "referral", "social"})

    def test_lead_statuses_are_valid(self):
        """Lead statuses must match the specified state machine."""
        from seeds.generate_postgres import generate_clients, generate_leads

        clients = generate_clients()
        leads = generate_leads(clients)
        statuses = {lead["status"] for lead in leads}
        assert statuses.issubset({"new", "contacted", "qualified", "converted", "lost"})

    def test_generate_appointments_returns_converted_leads_only(self):
        """Appointments must only exist for leads with status 'converted'."""
        from seeds.generate_postgres import generate_appointments, generate_clients, generate_leads

        clients = generate_clients()
        leads = generate_leads(clients)
        appointments = generate_appointments(leads)
        converted_ids = {lead["id"] for lead in leads if lead["status"] == "converted"}
        for appt in appointments:
            assert appt["lead_id"] in converted_ids

    def test_seed_is_reproducible(self):
        """Same seed (42) always produces identical data."""
        from seeds.generate_postgres import generate_clients

        run_a = [c["id"] for c in generate_clients()]
        run_b = [c["id"] for c in generate_clients()]
        assert run_a == run_b


class TestGenerateSeoApi:
    """Tests for seeds/generate_seo_api.py."""

    def test_generates_52_weekly_snapshots_per_client(self):
        """Each client must have 52 weekly SEO rank snapshots."""
        from seeds.generate_seo_api import generate_seo_rankings

        rankings = generate_seo_rankings(client_ids=list(range(1, 6)))
        per_client = len(rankings) // 5
        assert per_client == 52 * 10  # 52 weeks × 10 keywords

    def test_seo_ranking_has_required_fields(self):
        """SEO snapshot must include position, keyword, client_id, and snapshot_date."""
        from seeds.generate_seo_api import generate_seo_rankings

        rankings = generate_seo_rankings(client_ids=[1])
        required = {"client_id", "keyword", "position", "snapshot_date"}
        assert required.issubset(set(rankings[0].keys()))

    def test_position_is_in_valid_range(self):
        """Keyword positions must be 1–100."""
        from seeds.generate_seo_api import generate_seo_rankings

        rankings = generate_seo_rankings(client_ids=[1])
        for row in rankings:
            assert 1 <= row["position"] <= 100


class TestGenerateAdSpend:
    """Tests for seeds/generate_ad_spend.py."""

    def test_generates_365_days_per_client_per_channel(self):
        """Each client×channel combination must have 365 daily rows."""
        from seeds.generate_ad_spend import generate_ad_spend

        rows = generate_ad_spend(client_ids=list(range(1, 3)))
        # 2 clients × 3 channels × 365 days
        assert len(rows) == 2 * 3 * 365

    def test_ad_spend_has_required_fields(self):
        """Ad spend rows must include spend, impressions, and clicks."""
        from seeds.generate_ad_spend import generate_ad_spend

        rows = generate_ad_spend(client_ids=[1])
        required = {"client_id", "channel", "date", "spend", "impressions", "clicks"}
        assert required.issubset(set(rows[0].keys()))

    def test_channels_are_valid(self):
        """Channels must be exactly Google, Meta, and email."""
        from seeds.generate_ad_spend import generate_ad_spend

        rows = generate_ad_spend(client_ids=[1])
        channels = {r["channel"] for r in rows}
        assert channels == {"google", "meta", "email"}

    def test_spend_is_within_expected_range(self):
        """Daily spend is within channel bounds, weekend-factor adjusted."""
        from seeds.generate_ad_spend import CHANNEL_SPEND, WEEKEND_FACTOR, generate_ad_spend

        rows = generate_ad_spend(client_ids=[1])
        # Weekend factor (0.6) can push weekend spend below the weekday floor
        min_spend = min(lo for lo, hi in CHANNEL_SPEND.values()) * WEEKEND_FACTOR
        max_spend = max(hi for lo, hi in CHANNEL_SPEND.values())
        for row in rows:
            assert min_spend <= row["spend"] <= max_spend


# ---------------------------------------------------------------------------
# dlt pipeline schema tests
# ---------------------------------------------------------------------------


class TestPipelineSchemas:
    """Smoke tests that pipeline modules are importable and expose correct types."""

    def test_postgres_source_is_importable(self):
        """postgres_source module must export a dlt source."""
        from ingestion.sources.postgres_source import postgres_source

        assert callable(postgres_source)

    def test_seo_api_source_is_importable(self):
        """seo_api_source module must export a dlt source."""
        from ingestion.sources.seo_api_source import seo_api_source

        assert callable(seo_api_source)

    def test_ad_spend_source_is_importable(self):
        """ad_spend_source module must export a dlt source."""
        from ingestion.sources.ad_spend_source import ad_spend_source

        assert callable(ad_spend_source)

    def test_pipelines_module_has_run_all_function(self):
        """pipelines.py must expose a run_all() entry point."""
        from ingestion.pipelines import run_all

        assert callable(run_all)
