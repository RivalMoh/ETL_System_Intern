import pandas as pd
import pytest
import requests


# ============================================================================
# Test Doubles & Helpers
# ============================================================================


class FakeResponse:
    """Mock HTTP response for testing API interactions."""

    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"HTTP {self.status_code}")

    def json(self):
        return self.payload


class DummyExtractor:
    """Mock extractor for testing catalog assessment without real API calls."""

    def __init__(self, mapping=None, errors=None):
        self.mapping = mapping or {}
        self.errors = errors or {}

    def get_dataset_details(self, dataset_id: str) -> pd.DataFrame:
        if dataset_id in self.errors:
            raise self.errors[dataset_id]
        return self.mapping.get(dataset_id, pd.DataFrame())


# ============================================================================
# Shared Fixtures
# ============================================================================


@pytest.fixture
def sample_catalog():
    """Standard test catalog with mixed title similarities."""
    return pd.DataFrame(
        {
            "id": ["A", "B", "C", "D"],
            "judul": [
                "Produksi Padi Jawa Tengah",
                "Jawa Tengah Produksi Padi",
                "Data Kemiskinan",
                "Kemiskinan Data",
            ],
        }
    )


@pytest.fixture
def messy_harvest_data():
    """Sample harvest dataset with duplicates and missing values."""
    return pd.DataFrame(
        {
            "record_id": ["A1", "A2", "A3", "A4"],
            "province": ["Jawa Barat", "Jawa Barat", "Jawa Tengah", None],
            "year": [2023, 2023, 2024, 2025],
            "harvest_tonnage": [100.5, 100.5, 150.0, 200.0],
        }
    )
