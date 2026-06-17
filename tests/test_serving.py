"""
Integration tests for the FastAPI serving layer.

TDD: tests were written before implementation and define the API contract.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture
def mock_duckdb_context():
    """Patch duckdb.connect so tests don't need a real DuckDB file."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.__enter__ = lambda self: self
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.execute.return_value = mock_cursor
    return mock_conn


@pytest.fixture
def sample_client_context() -> dict:
    """Sample payload matching the /context/{client_id} response schema."""
    return {
        "client_id": 1,
        "client_name": "Apex Legal Group",
        "industry": "legal",
        "plan_tier": "pro",
        "days_active": 165,
        "total_leads": 52,
        "converted_leads": 12,
        "lead_conversion_rate": 0.2308,
        "total_appointments": 10,
        "top_keywords": [
            {"keyword": "legal near me", "position": 8},
            {"keyword": "best legal in Chicago", "position": 12},
        ],
        "monthly_spend_usd": 4234.5,
        "avg_cpl": 18.75,
        "last_updated": "2024-01-15T10:30:00",
    }


class TestClientContextEndpoint:
    """Tests for GET /context/{client_id}."""

    def _get_client_with_mock(self, mock_data: dict) -> TestClient:
        """Return a test client with DuckDB mocked to return sample data."""
        with patch("serving.routers.client_context.fetch_client_context") as mock_fetch:
            mock_fetch.return_value = mock_data
            from serving.main import app

            return TestClient(app)

    def test_returns_200_for_valid_client(self, sample_client_context):
        """Valid client_id returns HTTP 200."""
        with patch("serving.routers.client_context.fetch_client_context") as mock_fetch:
            mock_fetch.return_value = sample_client_context
            from serving.main import app

            client = TestClient(app)
            response = client.get("/context/1")
        assert response.status_code == 200

    def test_returns_404_for_unknown_client(self):
        """Unknown client_id returns HTTP 404."""
        with patch("serving.routers.client_context.fetch_client_context") as mock_fetch:
            mock_fetch.return_value = None
            from serving.main import app

            client = TestClient(app)
            response = client.get("/context/99999")
        assert response.status_code == 404

    def test_response_has_client_id(self, sample_client_context):
        """Response JSON includes client_id field."""
        with patch("serving.routers.client_context.fetch_client_context") as mock_fetch:
            mock_fetch.return_value = sample_client_context
            from serving.main import app

            client = TestClient(app)
            data = client.get("/context/1").json()
        assert "client_id" in data

    def test_response_has_industry(self, sample_client_context):
        """Response JSON includes industry field."""
        with patch("serving.routers.client_context.fetch_client_context") as mock_fetch:
            mock_fetch.return_value = sample_client_context
            from serving.main import app

            client = TestClient(app)
            data = client.get("/context/1").json()
        assert "industry" in data

    def test_response_has_lead_conversion_rate(self, sample_client_context):
        """Response JSON includes lead_conversion_rate."""
        with patch("serving.routers.client_context.fetch_client_context") as mock_fetch:
            mock_fetch.return_value = sample_client_context
            from serving.main import app

            client = TestClient(app)
            data = client.get("/context/1").json()
        assert "lead_conversion_rate" in data

    def test_response_has_top_keywords(self, sample_client_context):
        """Response JSON includes top_keywords list."""
        with patch("serving.routers.client_context.fetch_client_context") as mock_fetch:
            mock_fetch.return_value = sample_client_context
            from serving.main import app

            client = TestClient(app)
            data = client.get("/context/1").json()
        assert "top_keywords" in data
        assert isinstance(data["top_keywords"], list)

    def test_response_has_monthly_spend(self, sample_client_context):
        """Response includes monthly_spend_usd."""
        with patch("serving.routers.client_context.fetch_client_context") as mock_fetch:
            mock_fetch.return_value = sample_client_context
            from serving.main import app

            client = TestClient(app)
            data = client.get("/context/1").json()
        assert "monthly_spend_usd" in data

    def test_client_id_must_be_positive_integer(self):
        """Non-integer client_id returns 422 validation error."""
        from serving.main import app

        client = TestClient(app)
        response = client.get("/context/abc")
        assert response.status_code == 422

    def test_health_endpoint_returns_ok(self):
        """GET /health returns {status: ok}."""
        from serving.main import app

        client = TestClient(app)
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"

    def test_openapi_docs_available(self):
        """FastAPI auto-generated /docs endpoint returns 200."""
        from serving.main import app

        client = TestClient(app)
        response = client.get("/docs")
        assert response.status_code == 200
